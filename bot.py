"""Admin-controlled Telegram media-link archiver.

A safe, standalone implementation of /link and /done. It only uses Bot API
membership and does not request users' OTP, login credentials or sessions.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from uuid import uuid4
from threading import Thread

from app import start_server

from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient
from telegram import Update
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TelegramError, TimedOut
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

load_dotenv()

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
ADMIN_ID = int(os.environ["ADMIN_ID"])
STORAGE_CHANNEL_ID = int(os.environ["STORAGE_CHANNEL_ID"])
MONGO_URI = os.environ["MONGO_URI"].strip()
MONGO_DB = os.getenv("MONGO_DB", "video_unlock_bot").strip()
COPY_GAP_SECONDS = max(0.5, float(os.getenv("UPLOAD_DELAY_SECONDS", "2")))
PROGRESS_INTERVAL = max(1.5, float(os.getenv("UPLOAD_PROGRESS_REFRESH_SECONDS", "2.5")))
MAX_RETRIES = max(1, min(8, int(os.getenv("UPLOAD_MAX_RETRIES", "5"))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
LOG = logging.getLogger("media-link")

mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
db = mongo[MONGO_DB]
links = db.video_links              # compatible with previously stored /link URLs
drafts = db.video_link_drafts
links.create_index([("token", ASCENDING)], unique=True)
links.create_index([("created_at", DESCENDING)])
drafts.create_index([("admin_id", ASCENDING)], unique=True)

# Only one admin /link upload is active at a time.
ACTIVE = {}


def utcnow():
    return datetime.now(timezone.utc)


def label_for(message):
    """Recognize all common Bot API message kinds; unsupported kinds are tried too."""
    kinds = (
        ("photo", "🖼 Photo"),
        ("video", "🎬 Video"),
        ("document", "📄 Document"),
        ("animation", "🎞 GIF/Animation"),
        ("audio", "🎵 Audio"),
        ("voice", "🎤 Voice"),
        ("video_note", "⭕ Video note"),
        ("sticker", "🏷 Sticker"),
        ("contact", "👤 Contact"),
        ("location", "📍 Location"),
        ("venue", "📍 Venue"),
        ("poll", "📊 Poll"),
        ("dice", "🎲 Dice"),
        ("text", "📝 Text"),
    )
    for attr, name in kinds:
        if getattr(message, attr, None):
            return name
    return "📎 Other message"


def short_error(error):
    message = str(error).replace("\n", " ").replace("\r", " ")
    return (message or type(error).__name__)[:150]


def render_progress(rt, doc, *, final_url=None):
    received = int(doc.get("submitted", 0))
    saved = len(doc.get("copied", []))
    failed = len(doc.get("failed", []))
    pending = max(0, received - saved - failed)
    completed = saved + failed
    percent = round(100 * completed / received) if received else 0
    blocks = min(10, round(percent / 10))
    bar = "▰" * blocks + "▱" * (10 - blocks)
    phase = rt.get("phase", "uploading")
    status = {
        "uploading": "📤 Receiving and saving messages",
        "finishing": "⏳ Finishing queued copies",
        "complete": "✅ Upload finished",
        "cancelled": "❌ Upload cancelled",
    }.get(phase, "📦 Storage update")
    lines = [
        "🔗 MEDIA LINK — LIVE PROGRESS", "", status, "",
        f"📥 Received: {received}",
        f"✅ Saved: {saved}",
        f"⏳ Pending: {pending}",
        f"❌ Failed: {failed}",
        f"{bar}  {percent}% processed",
    ]
    if rt.get("active_label") and pending and phase != "complete":
        lines.append(f"\n🔄 Copying: {rt['active_label']}")
    copied = doc.get("copied", [])
    if copied:
        last = copied[-1]
        lines.append(f"\n💾 Last saved: {last.get('kind', 'Message')} (#{last['source_message_id']})")
    if rt.get("cooldown_seconds", 0):
        lines.append(f"⏱ Telegram cooldown: {rt['cooldown_seconds']}s")
    if final_url:
        lines.extend(["", "✅ Shareable link:", final_url])
    elif phase == "uploading":
        lines.append("\nSend more files/messages, then /done")
    return "\n".join(lines)


async def progress_edit(bot, rt, *, force=False, final_url=None):
    if not rt.get("progress_mid"):
        return
    async with rt["edit_lock"]:
        now = asyncio.get_running_loop().time()
        if not force and now - rt.get("last_edit_at", 0) < PROGRESS_INTERVAL:
            return
        doc = await asyncio.to_thread(drafts.find_one, {"admin_id": ADMIN_ID}) or {}
        text = render_progress(rt, doc, final_url=final_url)
        if text == rt.get("last_text"):
            return
        try:
            await bot.edit_message_text(chat_id=rt["progress_chat_id"],
                                        message_id=rt["progress_mid"], text=text,
                                        read_timeout=30, write_timeout=30)
            rt["last_text"] = text
            rt["last_edit_at"] = now
        except RetryAfter as error:
            rt["cooldown_seconds"] = max(1, round(float(error.retry_after)))
            LOG.info("Progress edit rate limited: %ss", rt["cooldown_seconds"])
        except BadRequest as error:
            err = str(error).lower()
            if "not modified" in err:
                rt["last_text"] = text
            elif "message to edit not found" in err or "message can't be edited" in err:
                try:
                    new = await bot.send_message(rt["progress_chat_id"], text)
                    rt["progress_mid"] = new.message_id
                    rt["last_text"] = text
                except TelegramError:
                    LOG.warning("Progress message could not be recreated")
            else:
                LOG.warning("Progress edit: %s", short_error(error))
        except TelegramError as error:
            LOG.warning("Progress edit: %s", short_error(error))


async def progress_worker(bot, rt):
    try:
        while True:
            try:
                await asyncio.wait_for(rt["event"].wait(), timeout=PROGRESS_INTERVAL)
            except asyncio.TimeoutError:
                pass
            rt["event"].clear()
            await progress_edit(bot, rt)
    except asyncio.CancelledError:
        raise
    except Exception:
        LOG.exception("Progress worker error")


async def copy_job(bot, rt, chat_id, mid, kind):
    """Copy admin's message without downloading it; record completed or failed."""
    async with rt["copy_lock"]:
        rt["active_label"] = f"{kind} (#{mid})"
        rt["event"].set()
        loop = asyncio.get_running_loop()
        last_error = "Copy failed"
        for attempt in range(MAX_RETRIES):
            delay = max(rt["next_copy_at"], rt["cooldown_until"]) - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                saved = await bot.copy_message(
                    chat_id=STORAGE_CHANNEL_ID,
                    from_chat_id=chat_id,
                    message_id=mid,
                    protect_content=False,
                    connect_timeout=20, read_timeout=60, write_timeout=60, pool_timeout=30,
                )
            except RetryAfter as error:
                wait = float(error.retry_after) + 1.0
                rt["cooldown_seconds"] = round(wait)
                rt["cooldown_until"] = loop.time() + wait
                last_error = f"Telegram rate limit: {wait:.0f}s"
                continue
            except (BadRequest, Forbidden) as error:
                last_error = short_error(error)
                break
            except (NetworkError, TimedOut, TelegramError) as error:
                last_error = short_error(error)
                if attempt + 1 < MAX_RETRIES:
                    await asyncio.sleep(min(2 ** attempt, 16))
                continue
            except asyncio.CancelledError:
                raise
            except Exception as error:
                LOG.exception("Unexpected storage error for message %s", mid)
                last_error = short_error(error)
                break
            finally:
                rt["next_copy_at"] = max(rt["next_copy_at"], loop.time() + COPY_GAP_SECONDS)

            try:
                await asyncio.to_thread(
                    drafts.update_one, {"admin_id": ADMIN_ID},
                    {"$push": {"copied": {
                        "source_message_id": mid,
                        "storage_message_id": saved.message_id,
                        "kind": kind,
                    }}},
                )
                rt["cooldown_seconds"] = 0
                rt["active_label"] = None
                rt["event"].set()
                return
            except Exception as error:
                last_error = "Saved in channel, but database record failed: " + short_error(error)
                LOG.exception("MongoDB record failed for source %s", mid)
                break

        try:
            await asyncio.to_thread(drafts.update_one, {"admin_id": ADMIN_ID},
                                    {"$push": {"failed": {
                                        "source_message_id": mid,
                                        "kind": kind,
                                        "reason": last_error,
                                    }}})
        except Exception:
            LOG.exception("MongoDB failed to record copy error for source %s", mid)
        rt["active_label"] = None
        rt["cooldown_seconds"] = 0
        rt["event"].set()


def enqueue(bot, rt, chat_id, mid, kind):
    task = asyncio.create_task(copy_job(bot, rt, chat_id, mid, kind), name=f"media-copy-{mid}")
    rt["tasks"].add(task)
    task.add_done_callback(rt["tasks"].discard)


async def link_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if ADMIN_ID in ACTIVE:
        return await update.message.reply_text("An upload is already active. Send messages, /done or /cancel.")
    old = drafts.find_one({"admin_id": ADMIN_ID})
    await asyncio.to_thread(drafts.replace_one, {"admin_id": ADMIN_ID}, {
        "admin_id": ADMIN_ID, "submitted": 0, "copied": [], "failed": [],
        "created_at": utcnow(),
    }, True)
    rt = {
        "phase": "uploading", "closing": False,
        "copy_lock": asyncio.Lock(), "edit_lock": asyncio.Lock(),
        "event": asyncio.Event(), "tasks": set(), "seen": set(),
        "next_copy_at": 0.0, "cooldown_until": 0.0,
        "cooldown_seconds": 0, "active_label": None,
        "progress_chat_id": update.effective_chat.id, "progress_mid": None,
        "last_text": None, "last_edit_at": 0.0,
    }
    ACTIVE[ADMIN_ID] = rt
    await update.message.reply_text(
        "📦 MEDIA LINK UPLOAD\n\n"
        "Send or forward photos, videos, PDFs, documents, GIFs, audio, voice, stickers, "
        "text or other normal Telegram messages.\n\n"
        "All are copied to your private storage channel in order. "
        "Use /done when finished, /linkstatus for a snapshot, or /cancel to discard the draft.\n"
        "Progress updates in one message approximately every 2.5 seconds."
        + ("\n\n⚠️ An unfinished draft from a previous run was replaced." if old else "")
    )
    msg = await update.message.reply_text(render_progress(rt, {"submitted": 0}))
    rt["progress_mid"] = msg.message_id
    rt["last_text"] = msg.text
    rt["last_edit_at"] = asyncio.get_running_loop().time()
    rt["progress_task"] = asyncio.create_task(progress_worker(context.bot, rt))


async def incoming_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user or update.effective_user.id != ADMIN_ID:
        return
    rt = ACTIVE.get(ADMIN_ID)
    if not rt:
        return
    if rt["closing"]:
        return await update.message.reply_text("⏳ /done is finishing. This message was not queued.")
    mid = update.message.message_id
    if mid in rt["seen"]:
        return
    rt["seen"].add(mid)
    kind = label_for(update.message)
    try:
        await asyncio.to_thread(drafts.update_one, {"admin_id": ADMIN_ID}, {"$inc": {"submitted": 1}})
    except Exception:
        rt["seen"].discard(mid)
        LOG.exception("Failed to queue message %s", mid)
        return await update.message.reply_text("❌ Database unavailable; this message was not queued. Try sending it again.")
    enqueue(context.bot, rt, update.effective_chat.id, mid, kind)
    rt["event"].set()


async def stop_worker(rt):
    worker = rt.pop("progress_task", None)
    if worker:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def done_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    rt = ACTIVE.get(ADMIN_ID)
    if not rt:
        return await update.message.reply_text("No active upload. Send /link first.")
    if rt["closing"]:
        return await update.message.reply_text("Upload already finishing.")
    rt["closing"] = True
    rt["phase"] = "finishing"
    rt["event"].set()
    # Wait for *all* copies; Telegram media albums consist of several messages.
    if rt["tasks"]:
        await asyncio.gather(*list(rt["tasks"]), return_exceptions=True)
    rt["phase"] = "complete"
    await stop_worker(rt)
    doc = await asyncio.to_thread(drafts.find_one, {"admin_id": ADMIN_ID}) or {}
    copied = sorted(doc.get("copied", []), key=lambda item: item["source_message_id"])
    failed = doc.get("failed", [])
    mids = [int(item["storage_message_id"]) for item in copied]
    url = None
    if mids:
        token = uuid4().hex[:16]
        await asyncio.to_thread(links.insert_one, {
            "token": token, "message_ids": mids, "enabled": True,
            "created_by": ADMIN_ID, "created_at": utcnow(),
            "source": "safe_media_link_upload", "submitted": doc.get("submitted", 0),
            "failed_count": len(failed),
        })
        me = context.application.bot_data.get("username") or (await context.bot.get_me()).username
        url = f"https://t.me/{me}?start=vl_{token}"
    summary = render_progress(rt, doc, final_url=url)
    if not mids:
        summary += "\n\n❌ Nothing was saved; no link was created."
    if failed:
        failed_ids = ", ".join(str(x["source_message_id"]) for x in failed[:8])
        summary += f"\n\n⚠️ Failed message IDs: {failed_ids}"
        if len(failed) > 8:
            summary += f" (+{len(failed)-8} more)"
    try:
        await context.bot.edit_message_text(
            chat_id=rt["progress_chat_id"], message_id=rt["progress_mid"], text=summary
        )
    except TelegramError:
        await update.message.reply_text(summary)
    await asyncio.to_thread(drafts.delete_one, {"admin_id": ADMIN_ID})
    ACTIVE.pop(ADMIN_ID, None)


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    rt = ACTIVE.get(ADMIN_ID)
    if not rt:
        return await update.message.reply_text("No active /link upload.")
    doc = await asyncio.to_thread(drafts.find_one, {"admin_id": ADMIN_ID}) or {}
    await update.message.reply_text(render_progress(rt, doc))


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    rt = ACTIVE.pop(ADMIN_ID, None)
    if not rt:
        return await update.message.reply_text("No active upload to cancel.")
    rt["closing"] = True
    rt["phase"] = "cancelled"
    tasks = list(rt["tasks"])
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    await stop_worker(rt)
    doc = await asyncio.to_thread(drafts.find_one, {"admin_id": ADMIN_ID}) or {}
    try:
        await context.bot.edit_message_text(
            chat_id=rt["progress_chat_id"], message_id=rt["progress_mid"],
            text=render_progress(rt, doc) + "\n\nDraft discarded; previously saved channel messages were not removed.",
        )
    except TelegramError:
        await update.message.reply_text("Upload cancelled; draft discarded.")
    await asyncio.to_thread(drafts.delete_one, {"admin_id": ADMIN_ID})


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if not args or not args[0].startswith("vl_"):
        return await update.message.reply_text(
            "📁 Media Link Bot\n\nOpen a shareable media link to receive its saved files."
        )
    token = args[0][3:]
    doc = await asyncio.to_thread(links.find_one, {"token": token, "enabled": True})
    if not doc:
        return await update.message.reply_text("⚠️ This link is invalid or has been disabled.")
    ids = list(doc.get("message_ids", []))
    if not ids:
        return await update.message.reply_text("No saved messages in this link.")
    status = await update.message.reply_text(f"📨 Sending {len(ids)} saved message(s)…")
    delivered = 0
    for mid in ids:
        for attempt in range(3):
            try:
                await context.bot.copy_message(
                    chat_id=update.effective_chat.id,
                    from_chat_id=STORAGE_CHANNEL_ID,
                    message_id=int(mid),
                    protect_content=True,
                )
                delivered += 1
                break
            except RetryAfter as error:
                await asyncio.sleep(float(error.retry_after) + 1)
            except TelegramError as error:
                LOG.warning("Delivery failed message %s: %s", mid, short_error(error))
                break
        await asyncio.sleep(0.1)
    await status.edit_text(f"✅ Delivery complete: {delivered}/{len(ids)} saved message(s) sent.")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == ADMIN_ID:
        await update.message.reply_text(
            "Admin: /link → send any Telegram media/text → /done\n"
            "/linkstatus → current counts\n/cancel → discard current draft\n\n"
            "The progress message edits automatically as files finish saving."
        )
    else:
        await update.message.reply_text("Open a media link shared with you to receive its contents.")


async def post_init(app):
    username = (await app.bot.get_me()).username
    app.bot_data["username"] = username
    LOG.info("Running @%s (storage chat %s)", username, STORAGE_CHANNEL_ID)


async def post_shutdown(app):
    for rt in list(ACTIVE.values()):
        rt["closing"] = True
        for task in list(rt["tasks"]):
            task.cancel()
        await stop_worker(rt)
    ACTIVE.clear()
    mongo.close()


def main():
    mongo.admin.command("ping")
    if os.getenv("PORT"):
        Thread(target=start_server, daemon=True).start()
    app = (Application.builder().token(BOT_TOKEN)
           .post_init(post_init).post_shutdown(post_shutdown).build())
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("link", link_command))
    app.add_handler(CommandHandler("done", done_command))
    app.add_handler(CommandHandler("linkstatus", status_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("help", help_command))
    # Unlike filters.VIDEO, this catches photos, PDFs, animations, audio, text,
    # voice, stickers and other normal private messages as well.
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND,
                                   incoming_message))
    app.run_polling(drop_pending_updates=False, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
