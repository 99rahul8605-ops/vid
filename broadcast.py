"""Admin broadcasts for eligible existing Telegram bot users.

One broadcast runs at a time, sequential delivery with Telegram RetryAfter support.
Progress is persisted in MongoDB; resume manually after restarting the bot.
"""
import asyncio
import html
import logging
import os
from datetime import datetime, timezone
from uuid import uuid4

from pymongo import ASCENDING, DESCENDING
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError
from telegram.ext import ApplicationHandlerStop

LOG = logging.getLogger(__name__)


def utcnow():
    return datetime.now(timezone.utc)


class BroadcastManager:
    def __init__(self, db, users, admin_id):
        self.users = users
        self.admin_id = admin_id
        self.jobs = db.broadcast_jobs
        self.jobs.create_index([("created_at", DESCENDING)])
        self.drafts = {}
        self.task = None
        self.interval = max(1.5, float(os.getenv("BROADCAST_DELAY_SECONDS", "2")))

    @staticmethod
    def _menu():
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📣 New Broadcast", callback_data="bc_new")],
            [InlineKeyboardButton("📊 Broadcast Status", callback_data="bc_status")],
            [InlineKeyboardButton("⏸ Pause", callback_data="bc_pause"),
             InlineKeyboardButton("▶️ Resume", callback_data="bc_resume")],
            [InlineKeyboardButton("❌ Cancel Draft", callback_data="bc_cancel")],
        ])

    @staticmethod
    def _confirm():
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Send Broadcast", callback_data="bc_send"),
            InlineKeyboardButton("❌ Cancel", callback_data="bc_cancel"),
        ]])

    @staticmethod
    def _audience_query(after_id=None):
        """Target every user document stored in the bot users collection."""
        query = {}
        if after_id is not None:
            query["user_id"] = {"$gt": after_id}
        return query

    def _current(self):
        return self.jobs.find_one(sort=[("created_at", DESCENDING)])

    def _busy(self):
        return self.jobs.find_one({"status": {"$in": ["sending", "paused"]}})

    async def menu(self, update, context):
        if update.effective_user.id != self.admin_id:
            return
        target = update.effective_chat.id
        count = self.users.count_documents(self._audience_query())
        await context.bot.send_message(
            target,
            f"📣 <b>BROADCAST PANEL</b>\n\nEligible bot users: <b>{count}</b>\n"
            f"Delay: <b>{self.interval:g}s</b> per recipient.\n"
            "All stored bot users will be attempted.",
            parse_mode=ParseMode.HTML, reply_markup=self._menu(),
        )

    async def command(self, update, context):
        return await self.menu(update, context)

    async def collect(self, update, context):
        uid = update.effective_user.id
        if uid != self.admin_id or self.drafts.get(uid) != "waiting":
            return
        m = update.effective_message
        if not m or not (m.text or m.photo or m.video or m.document or m.animation or m.audio or m.voice):
            await m.reply_text("Send text, photo, video, document, animation, audio or voice.")
            raise ApplicationHandlerStop
        self.drafts[uid] = {"chat_id": m.chat_id, "message_id": m.message_id}
        if m.text is not None:
            # Retain Telegram bold/italic/links while disabling web previews.
            self.drafts[uid]["text_html"] = m.text_html
        await m.reply_text("📋 Broadcast preview:")
        if m.text is not None:
            await context.bot.send_message(
                self.admin_id, self.drafts[uid]["text_html"],
                parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                reply_markup=self._confirm(),
            )
        else:
            await context.bot.copy_message(self.admin_id, m.chat_id, m.message_id,
                                           reply_markup=self._confirm())
        raise ApplicationHandlerStop

    async def callback(self, update, context):
        q = update.callback_query
        if q.from_user.id != self.admin_id:
            return await q.answer("Admin only.", show_alert=True)
        op = q.data
        await q.answer()
        if op == "bc_new":
            if self._busy():
                return await q.message.reply_text("Pause and finish the existing broadcast first.")
            self.drafts[self.admin_id] = "waiting"
            return await q.message.reply_text(
                "📣 Send the message to broadcast (text, photo, video, document, etc.).\n"
                "You will see a preview before sending.")
        if op == "bc_cancel":
            self.drafts.pop(self.admin_id, None)
            return await q.message.reply_text("✅ Broadcast draft cancelled.")
        if op == "bc_status":
            return await q.message.reply_text(self.status_text(), reply_markup=self._menu(),
                                              parse_mode=ParseMode.HTML)
        if op == "bc_send":
            draft = self.drafts.get(self.admin_id)
            if not isinstance(draft, dict):
                return await q.message.reply_text("Draft expired. Choose New Broadcast.")
            if self._busy():
                return await q.message.reply_text("An existing broadcast must finish first.")
            self.drafts.pop(self.admin_id, None)
            job_id = uuid4().hex
            audience = self.users.count_documents(self._audience_query())
            if not audience:
                return await q.message.reply_text("No eligible bot users found.")
            self.jobs.insert_one({
                "job_id": job_id, "status": "sending", "source_chat_id": draft["chat_id"],
                "source_message_id": draft["message_id"],
                "text_html": draft.get("text_html"), "created_at": utcnow(),
                "updated_at": utcnow(), "total_at_start": audience, "sent": 0, "failed": 0,
                "skipped": 0, "last_user_id": 0, "failures": [],
            })
            status = await q.message.reply_text(
                f"🚀 Broadcast started for {audience} eligible users.\n"
                "Use /broadcast to see progress and pause or resume.")
            self.jobs.update_one({"job_id": job_id}, {"$set": {
                "progress_chat_id": status.chat_id, "progress_message_id": status.message_id}})
            self._start(context.application, job_id)
            return
        if op == "bc_pause":
            active = self.jobs.find_one({"status": "sending"})
            if not active:
                return await q.message.reply_text("No active broadcast.")
            self.jobs.update_one({"_id": active["_id"]},
                                 {"$set": {"status": "paused", "updated_at": utcnow()}})
            return await q.message.reply_text("⏸ Broadcast will pause after the current recipient.")
        if op == "bc_resume":
            job = self.jobs.find_one({"status": "paused"}, sort=[("created_at", DESCENDING)])
            if not job:
                return await q.message.reply_text("No paused broadcast found.")
            if self.task and not self.task.done():
                return await q.message.reply_text("Previous sender is still stopping; try again.")
            self.jobs.update_one({"_id": job["_id"]},
                                 {"$set": {"status": "sending", "updated_at": utcnow()}})
            self._start(context.application, job["job_id"])
            return await q.message.reply_text("▶️ Broadcast resumed.")

    def status_text(self):
        job = self._current()
        if not job:
            return "ℹ️ No previous broadcast."
        return ("📊 <b>BROADCAST STATUS</b>\n\n"
                f"State: <b>{html.escape(job['status'])}</b>\n"
                f"Eligible users at start: <b>{job.get('total_at_start', 0)}</b>\n"
                f"✅ Sent: <b>{job.get('sent', 0)}</b>\n"
                f"❌ Failed: <b>{job.get('failed', 0)}</b>\n"
                f"⏭ Skipped: <b>{job.get('skipped', 0)}</b>\n"
                f"Last recipient ID: <code>{job.get('last_user_id', 0)}</code>")

    def _start(self, app, job_id):
        self.task = asyncio.create_task(self._send_loop(app, job_id), name=f"broadcast-{job_id}")

    async def _send_loop(self, app, job_id):
        try:
            while True:
                job = await asyncio.to_thread(self.jobs.find_one, {"job_id": job_id})
                if not job or job["status"] != "sending":
                    return
                # Small batches: no unbounded user-ID list held in RAM.
                last_id = job.get("last_user_id", 0)
                docs = await asyncio.to_thread(
                    lambda: list(self.users.find(self._audience_query(last_id),
                                                 {"user_id": 1}).sort("user_id", ASCENDING).limit(100))
                )
                if not docs:
                    await asyncio.to_thread(self.jobs.update_one, {"job_id": job_id, "status": "sending"},
                                         {"$set": {"status": "completed", "updated_at": utcnow()}})
                    await self._edit_progress(app.bot, self.jobs.find_one({"job_id": job_id}))
                    await app.bot.send_message(self.admin_id, "✅ Broadcast completed.\n" +
                                               self.status_text_plain(job_id))
                    return
                for doc in docs:
                    job = await asyncio.to_thread(self.jobs.find_one, {"job_id": job_id})
                    if not job or job["status"] != "sending":
                        return
                    uid = doc["user_id"]
                    result = "sent"
                    while True:
                        if self.jobs.find_one({"job_id": job_id})["status"] != "sending":
                            return
                        try:
                            if job.get("text_html") is not None:
                                await app.bot.send_message(
                                    chat_id=uid, text=job["text_html"],
                                    parse_mode=ParseMode.HTML,
                                    disable_web_page_preview=True,
                                )
                            else:
                                await app.bot.copy_message(chat_id=uid,
                                    from_chat_id=job["source_chat_id"],
                                    message_id=job["source_message_id"], protect_content=False)
                            break
                        except RetryAfter as e:
                            await asyncio.sleep(float(e.retry_after) + 1)
                        except (Forbidden, BadRequest, TelegramError) as e:
                            LOG.info("Broadcast delivery to %s failed: %s", uid, type(e).__name__)
                            result = "failed"
                            break
                    await asyncio.to_thread(self.jobs.update_one, {"job_id": job_id}, {
                        "$inc": {result: 1},
                        "$set": {"last_user_id": uid, "updated_at": utcnow()},
                    })
                    updated = await asyncio.to_thread(self.jobs.find_one, {"job_id": job_id})
                    if ((updated.get("sent", 0) + updated.get("failed", 0)) % 10 == 0):
                        await self._edit_progress(app.bot, updated)
                    await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOG.exception("Broadcast interrupted")
            await asyncio.to_thread(self.jobs.update_one, {"job_id": job_id, "status": "sending"},
                                 {"$set": {"status": "paused", "updated_at": utcnow()}})
            try:
                await app.bot.send_message(self.admin_id,
                    "⚠️ Broadcast paused after an error. Open /broadcast to resume.")
            except TelegramError:
                pass

    async def _edit_progress(self, bot, job):
        if not job or not job.get("progress_message_id"):
            return
        text = ("📣 <b>BROADCAST PROGRESS</b>\n\n"
                f"Status: <b>{html.escape(job['status'])}</b>\n"
                f"✅ Sent: <b>{job.get('sent', 0)}</b>\n"
                f"❌ Failed: <b>{job.get('failed', 0)}</b>\n"
                f"⏭ Skipped: <b>{job.get('skipped', 0)}</b>\n"
                f"Eligible at start: <b>{job.get('total_at_start', 0)}</b>")
        try:
            await bot.edit_message_text(chat_id=job["progress_chat_id"],
                message_id=job["progress_message_id"], text=text, parse_mode=ParseMode.HTML)
        except TelegramError:
            pass

    def status_text_plain(self, job_id):
        job = self.jobs.find_one({"job_id": job_id}) or {}
        return (f"Sent: {job.get('sent', 0)}; Failed: {job.get('failed', 0)}; "
                f"Skipped: {job.get('skipped', 0)}")

    async def shutdown(self):
        if self.task and not self.task.done():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        self.jobs.update_many({"status": "sending"},
                              {"$set": {"status": "paused", "updated_at": utcnow()}})
