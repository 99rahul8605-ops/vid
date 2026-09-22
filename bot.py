
import asyncio
import html
import logging
import os
import re
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING, DESCENDING
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.constants import ChatMemberStatus, ParseMode
from telegram.error import BadRequest, Forbidden, TelegramError, RetryAfter, TimedOut, NetworkError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
MONGO_URI = os.environ["MONGO_URI"].strip()
MONGO_DB = os.getenv("MONGO_DB", "video_unlock_bot").strip()
ADMIN_ID = int(os.environ["ADMIN_ID"])
STORAGE_CHANNEL_ID = int(os.environ["STORAGE_CHANNEL_ID"])
DELETE_AFTER_HOURS = int(os.getenv("DELETE_AFTER_HOURS", "12"))
REFERRALS_PER_BATCH = max(1, int(os.getenv("REFERRALS_PER_BATCH", "1")))
PENDING_REQUEST_TTL_HOURS = max(1, int(os.getenv("PENDING_REQUEST_TTL_HOURS", "72")))
# Storage-channel copying is intentionally serialized even if an older .env
# still contains UPLOAD_CONCURRENCY=3. One worker avoids overlapping 429s.
UPLOAD_CONCURRENCY = 1
# Minimum time BETWEEN completed copy attempts (success or failure).
UPLOAD_COPY_GAP_SECONDS = max(2.0, float(os.getenv("UPLOAD_DELAY_SECONDS", os.getenv("UPLOAD_COPY_GAP_SECONDS", "2"))))
UPLOAD_MAX_RETRIES = max(1, min(8, int(os.getenv("UPLOAD_MAX_RETRIES", "5"))))

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

users.create_index([("user_id", ASCENDING)], unique=True)
required_chats.create_index([("chat_id", ASCENDING)], unique=True)
batches.create_index([("batch_no", ASCENDING)], unique=True)
pending_deletions.create_index([("delete_at", ASCENDING)])
drafts.create_index([("admin_id", ASCENDING)], unique=True)
pending_requests.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)], unique=True)
pending_requests.create_index([("requested_at", ASCENDING)])
membership_events.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)], unique=True)

ADMIN_STATE = {}
UPLOAD_RUNTIME = {}

USER_MENU = ReplyKeyboardMarkup(
    [
        ["🎬 My Videos", "🔓 Unlock Next 10"],
        ["👥 My Referral", "📊 My Progress"],
    ],
    resize_keyboard=True,
)

ADMIN_MENU = ReplyKeyboardMarkup(
    [
        ["📤 Upload Videos", "📚 Video Batches"],
        ["➕ Add Required Group", "👥 Required Groups"],
        ["🗑 Remove Required Channel", "📊 Bot Stats"],
        ["❌ Cancel"],
    ],
    resize_keyboard=True,
)

UPLOAD_MENU = ReplyKeyboardMarkup(
    [["DONE", "🔁 Retry Failed"], ["✅ Publish Successful", "❌ Cancel"]],
    resize_keyboard=True,
)

def esc(v):
    return html.escape(str(v))

def utcnow():
    return datetime.now(timezone.utc)

def user_doc(uid):
    return users.find_one({"user_id": uid})

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
    # post_init runs before Application.start(), so Application.create_task()
    # would not be tracked and can survive shutdown as a pending task.
    # Own the task explicitly and cancel/await it in post_stop below.
    app.bot_data["video_cleanup_task"] = asyncio.create_task(
        deletion_worker(app), name="video-cleanup"
    )
    log.info("Bot started as @%s", me.username)


async def post_stop(app: Application):
    # Do not leave upload-copy tasks dangling at shutdown. Completed transfers
    # have already been saved to MongoDB; unfinished work remains in the draft.
    runtime = UPLOAD_RUNTIME.pop(ADMIN_ID, None)
    if runtime:
        runtime["closing"] = True
        running = list(runtime["tasks"])
        for t in running:
            t.cancel()
        if running:
            await asyncio.gather(*running, return_exceptions=True)
    task = app.bot_data.pop("video_cleanup_task", None)
    if task is not None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    log.info("Video cleanup worker stopped")

async def ensure_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    }
    users.insert_one(doc)
    return doc

async def on_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Save approval/private join requests temporarily for 24 hours.
    Public/custom-link chats do not use this fallback.
    """
    req = update.chat_join_request
    if not req:
        return

    cfg = required_chats.find_one({
        "chat_id": req.chat.id,
        "enabled": True,
    })
    if not cfg:
        return

    if cfg.get("link_mode") != "approval":
        return

    now = utcnow()
    expires_at = now + timedelta(hours=PENDING_REQUEST_TTL_HOURS)

    pending_requests.update_one(
        {"chat_id": req.chat.id, "user_id": req.from_user.id},
        {
            "$set": {
                "requested_at": now,
                "expires_at": expires_at,
                "invite_link": req.invite_link.invite_link if req.invite_link else None,
            }
        },
        upsert=True,
    )

    log.info(
        "Join request captured chat=%s user=%s expires=%s",
        req.chat.id,
        req.from_user.id,
        expires_at.isoformat(),
    )


def has_active_pending_request(chat_id: int, user_id: int) -> bool:
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
    old_status = cmu.old_chat_member.status

    active_statuses = {
        ChatMemberStatus.MEMBER,
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.OWNER,
    }

    def status_is_member(member_obj):
        status = member_obj.status
        if status in active_statuses:
            return True
        if status == ChatMemberStatus.RESTRICTED:
            return bool(getattr(member_obj, "is_member", False))
        return False

    old_is_member = status_is_member(cmu.old_chat_member)
    new_is_member = status_is_member(cmu.new_chat_member)
    now = utcnow()

    if new_is_member:
        pending_requests.delete_many({"chat_id": chat_id, "user_id": uid})
        membership_events.update_one(
            {"chat_id": chat_id, "user_id": uid},
            {
                "$set": {
                    "is_member": True,
                    "last_join_at": now,
                    "last_status": str(new_status),
                }
            },
            upsert=True,
        )
        return

    membership_events.update_one(
        {"chat_id": chat_id, "user_id": uid},
        {
            "$set": {
                "is_member": False,
                "last_status": str(new_status),
                "last_seen_nonmember_at": now,
            }
        },
        upsert=True,
    )

    # If they transitioned from member -> non-member, this is a real leave/remove.
    if old_is_member and not new_is_member:
        pending_requests.delete_many({"chat_id": chat_id, "user_id": uid})
        membership_events.update_one(
            {"chat_id": chat_id, "user_id": uid},
            {"$set": {"last_leave_at": now}},
            upsert=True,
        )

        # Any request from before this leave is invalid.
        pending_requests.delete_many(
            {
                "chat_id": chat_id,
                "user_id": uid,
                "requested_at": {"$lte": now},
            }
        )

        users.update_one(
            {"user_id": uid},
            {
                "$set": {
                    "verified": False,
                    "verification_revoked_at": now,
                }
            },
        )

        try:
            await context.bot.send_message(
                uid,
                "🔒 <b>Access Locked Again</b>\n\n"
                "Required group/channel membership chhodne ke baad access revoke ho gaya. "
                "Dobara join/request karke verify karo.",
                parse_mode=ParseMode.HTML,
            )
        except TelegramError:
            pass


async def check_one_chat(bot, ch, uid):
    """
    Public/custom-link chat:
      current member check only.

    Approval/private chat:
      current member check first;
      if not a member, recent captured join request (<= TTL) counts as pending.
    """
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
            pending_requests.delete_many({"chat_id": chat_id, "user_id": uid})
            membership_events.update_one(
                {"chat_id": chat_id, "user_id": uid},
                {"$set": {
                    "is_member": True,
                    "last_seen_member_at": utcnow(),
                    "last_status": str(status),
                }},
                upsert=True,
            )
            return ch, "member"

        membership_events.update_one(
            {"chat_id": chat_id, "user_id": uid},
            {"$set": {
                "is_member": False,
                "last_seen_nonmember_at": utcnow(),
                "last_status": str(status),
            }},
            upsert=True,
        )

    except TelegramError as e:
        log.warning(
            "getChatMember failed chat=%s user=%s error=%s",
            chat_id, uid, type(e).__name__
        )

    # Public/custom-link chats never accept pending fallback.
    if ch.get("link_mode") != "approval":
        return ch, "missing"

    pending = await asyncio.to_thread(
        has_active_pending_request, chat_id, uid
    )
    return ch, ("pending" if pending else "missing")


async def membership_result(bot, uid):
    """
    Verifies all required chats concurrently instead of one-by-one.
    A recent join request counts as pending/accepted for the gate.
    """
    chats = active_required_chats()
    if not chats:
        return chats, [], []

    results = await asyncio.gather(
        *(check_one_chat(bot, ch, uid) for ch in chats),
        return_exceptions=True,
    )

    missing = []
    pending = []
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
        rows.append(
            [InlineKeyboardButton(f"➕ Join {ch['name']}", url=ch["join_url"])]
        )
    rows.append(
        [InlineKeyboardButton("✅ I've Joined — Verify", callback_data="verify_join")]
    )
    return InlineKeyboardMarkup(rows)

async def show_join_gate(update, context, chats=None):
    if chats is None:
        chats = active_required_chats()

    if not chats:
        return await home(update, context)

    text = (
        "🔐 <b>VIDEO ACCESS LOCKED</b>\n\n"
        "Sirf neeche diye gaye remaining groups/channels complete karo.\n\n"
        f"📌 Remaining: <b>{len(chats)}</b>\n"
        "✅ Join/request complete karne ke baad <b>Verify</b> dabao.\n\n"
        "ℹ️ Already completed groups dobara show nahi honge."
    )

    await update.effective_chat.send_message(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=join_keyboard(chats),
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update, context)

    uid = update.effective_user.id
    chats, missing, pending = await membership_result(context.bot, uid)

    if not missing:
        current = users.find_one({"user_id": uid}) or {}
        set_data = {
            "verified": True,
            "verified_at": utcnow(),
        }
        if int(current.get("unlocked_batch", 0)) == 0 and published_batch_count() > 0:
            set_data["unlocked_batch"] = 1

        users.update_one(
            {"user_id": uid},
            {"$set": set_data},
        )
        return await home(update, context)

    users.update_one(
        {"user_id": uid},
        {"$set": {"verified": False}},
    )

    # Only missing chats are shown. Member/pending chats stay hidden.
    await show_join_gate(update, context, chats=missing)


async def home(update, context):
    d = user_doc(update.effective_user.id) or {}
    unlocked = int(d.get("unlocked_batch", 0))
    total = published_batch_count()
    refs = int(d.get("verified_referrals", 0))
    if unlocked == 0 and d.get("verified") and total:
        users.update_one(
            {"user_id": update.effective_user.id},
            {"$set": {"unlocked_batch": 1}},
        )
        unlocked = 1

    await update.effective_chat.send_message(
        "🎬 <b>VIDEO VAULT</b>\n\n"
        f"🔓 Unlocked batch: <b>{unlocked}/{total}</b>\n"
        f"👥 Successful referrals: <b>{refs}</b>\n"
        "🎞 Videos per full batch: <b>10</b>\n"
        f"⏳ Videos auto-delete in <b>{DELETE_AFTER_HOURS} hours</b>.\n\n"
        "Menu se option choose karo.",
        parse_mode=ParseMode.HTML,
        reply_markup=USER_MENU,
    )

async def verify_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("Verifying all chats…")
    uid = q.from_user.id

    # Cosmetic state while verification runs.
    try:
        await q.edit_message_reply_markup(
            InlineKeyboardMarkup(
                [[InlineKeyboardButton("⏳ Verifying…", callback_data="noop")]]
            )
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
            "Ye joins/request abhi missing hain:\n"
            f"{names}"
            f"{pending_text}\n\n"
            "Sirf upar dikh rahe remaining chats complete karke dobara verify karo."
        )
        try:
            await q.edit_message_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=join_keyboard(missing),
            )
        except BadRequest:
            await q.message.reply_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=join_keyboard(missing),
            )
        return

    before = users.find_one({"user_id": uid}) or {}
    first_verify = not bool(before.get("verified"))

    set_data = {"verified": True, "verified_at": utcnow()}
    if int(before.get("unlocked_batch", 0)) == 0 and published_batch_count() > 0:
        set_data["unlocked_batch"] = 1

    users.update_one({"user_id": uid}, {"$set": set_data})

    fresh = users.find_one({"user_id": uid}) or {}
    referrer = fresh.get("referred_by")
    if referrer and not fresh.get("referral_rewarded"):
        claimed = users.find_one_and_update(
            {"user_id": uid, "referral_rewarded": False},
            {
                "$set": {
                    "referral_rewarded": True,
                    "referral_rewarded_at": utcnow(),
                }
            },
        )
        if claimed:
            users.update_one(
                {"user_id": referrer},
                {"$inc": {"verified_referrals": 1}},
            )
            rd = users.find_one({"user_id": referrer}) or {}
            current = int(rd.get("verified_referrals", 0))
            need = required_referrals_for_next(
                max(1, int(rd.get("unlocked_batch", 1)))
            )
            try:
                await context.bot.send_message(
                    referrer,
                    "🎉 <b>REFERRAL SUCCESSFUL!</b>\n\n"
                    "Referred user ne force-join verification complete kar liya.\n\n"
                    f"👥 Verified referrals: <b>{current}</b>\n"
                    f"🎯 Next unlock target: <b>{need}</b>\n\n"
                    + (
                        "✅ Next 10 ready hain — <b>Unlock Next 10</b> dabao."
                        if current >= need
                        else "Invite progress continue karo."
                    ),
                    parse_mode=ParseMode.HTML,
                    reply_markup=USER_MENU,
                )
            except TelegramError:
                pass

    pending_note = (
        f"\n⏳ Pending approval accepted: <b>{len(pending)}</b>"
        if pending
        else ""
    )

    # Success edit intentionally has NO inline reply_markup.
    # This removes all old Join/Verify buttons immediately.
    success_text = (
        "✅ <b>VERIFICATION SUCCESSFUL</b>\n\n"
        "Access unlocked. 🎉"
        f"{pending_note}\n\n"
        f"Videos protected rahenge aur <b>{DELETE_AFTER_HOURS} hours</b> baad delete ho jayenge."
    )
    try:
        await q.edit_message_text(
            success_text,
            parse_mode=ParseMode.HTML,
            reply_markup=None,
        )
    except BadRequest:
        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except BadRequest:
            pass

    if first_verify:
        await q.message.reply_text(
            "👇 <b>Your video menu is ready</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=USER_MENU,
        )

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
        await update.effective_chat.send_message(
            "🔒 <b>Access re-locked.</b>\nRequired membership/request missing hai.",
            parse_mode=ParseMode.HTML,
        )
        await show_join_gate(update, context)
        return False
    return True

async def send_batch(chat_id, batch_no, context):
    batch = batches.find_one({"batch_no": batch_no, "published": True})
    if not batch:
        return await context.bot.send_message(
            chat_id, "⚠️ Ye batch available nahi hai."
        )

    mids = batch.get("message_ids", [])
    await context.bot.send_message(
        chat_id,
        "🎬 <b>YOUR VIDEOS ARE HERE</b>\n\n"
        f"📦 Batch: <b>{batch_no}</b>\n"
        f"🎞 Videos: <b>{len(mids)}</b>\n"
        "🔒 Forward/save protection: <b>ON</b>\n"
        f"⏳ Auto-delete: <b>{DELETE_AFTER_HOURS} hours</b>",
        parse_mode=ParseMode.HTML,
    )

    sent = []
    for mid in mids:
        try:
            r = await context.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=STORAGE_CHANNEL_ID,
                message_id=mid,
                protect_content=True,
            )
            sent.append(r.message_id)
            await asyncio.sleep(0.10)
        except TelegramError as e:
            log.warning(
                "Video copy failed batch=%s source=%s error=%s",
                batch_no,
                mid,
                type(e).__name__,
            )

    delete_at = utcnow() + timedelta(hours=DELETE_AFTER_HOURS)
    if sent:
        pending_deletions.insert_many(
            [
                {
                    "chat_id": chat_id,
                    "message_id": mid,
                    "delete_at": delete_at,
                    "kind": "video",
                }
                for mid in sent
            ]
        )

    await context.bot.send_message(
        chat_id,
        "✅ <b>DELIVERY COMPLETE</b>\n\n"
        f"Sent: <b>{len(sent)}/{len(mids)}</b>\n"
        "Next set ke liye <b>🔓 Unlock Next 10</b> use karo.",
        parse_mode=ParseMode.HTML,
        reply_markup=USER_MENU,
    )

async def my_videos(update, context):
    if not await ensure_verified(update, context):
        return
    d = user_doc(update.effective_user.id) or {}
    current = int(d.get("unlocked_batch", 0))
    if current <= 0:
        return await update.message.reply_text("⚠️ Abhi koi batch unlocked nahi hai.")
    await update.message.reply_text(
        f"🔁 Batch <b>{current}</b> resend kar raha hoon…",
        parse_mode=ParseMode.HTML,
    )
    await send_batch(update.effective_chat.id, current, context)

async def unlock_next(update, context):
    if not await ensure_verified(update, context):
        return

    uid = update.effective_user.id
    d = user_doc(uid) or {}
    current = max(1, int(d.get("unlocked_batch", 1)))
    total = published_batch_count()

    if current >= total:
        return await update.message.reply_text(
            "🏁 <b>ALL AVAILABLE VIDEOS UNLOCKED</b>",
            parse_mode=ParseMode.HTML,
        )

    need = required_referrals_for_next(current)
    have = int(d.get("verified_referrals", 0))
    if have < need:
        username = context.application.bot_data["username"]
        link = make_ref_link(username, uid)
        return await update.message.reply_text(
            "🔐 <b>NEXT 10 VIDEOS LOCKED</b>\n\n"
            f"Required verified referrals: <b>{need}</b>\n"
            f"Successful referrals: <b>{have}</b>\n"
            f"Remaining: <b>{need-have}</b>\n\n"
            "Referral tabhi count hoga jab friend required joins/approval requests complete karke verify kare.\n\n"
            f"🔗 <b>Your referral link:</b>\n<code>{esc(link)}</code>",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    nxt = current + 1
    users.update_one({"user_id": uid}, {"$set": {"unlocked_batch": nxt}})
    batch = batches.find_one({"batch_no": nxt, "published": True}) or {}
    count = len(batch.get("message_ids", []))
    await update.message.reply_text(
        f"🎉 <b>BATCH {nxt} UNLOCKED!</b>\n"
        f"🎞 Videos in this batch: <b>{count}</b>",
        parse_mode=ParseMode.HTML,
    )
    await send_batch(update.effective_chat.id, nxt, context)

async def referral(update, context):
    if not await ensure_verified(update, context):
        return
    uid = update.effective_user.id
    d = user_doc(uid) or {}
    current = max(1, int(d.get("unlocked_batch", 1)))
    refs = int(d.get("verified_referrals", 0))
    need = required_referrals_for_next(current)
    link = make_ref_link(context.application.bot_data["username"], uid)

    await update.message.reply_text(
        "👥 <b>MY REFERRAL</b>\n\n"
        f"✅ Successful referrals: <b>{refs}</b>\n"
        f"🎯 Next batch target: <b>{need}</b>\n"
        f"📈 Progress: <b>{min(refs, need)}/{need}</b>\n\n"
        f"🔗 <b>Your link:</b>\n<code>{esc(link)}</code>\n\n"
        "⚠️ Referral only after force-join verification counts.",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )

async def progress(update, context):
    if not await ensure_verified(update, context):
        return
    d = user_doc(update.effective_user.id) or {}
    current = int(d.get("unlocked_batch", 0))
    refs = int(d.get("verified_referrals", 0))

    unlocked_video_count = 0
    for b in batches.find(
        {"published": True, "batch_no": {"$lte": current}},
        {"message_ids": 1},
    ):
        unlocked_video_count += len(b.get("message_ids", []))

    await update.message.reply_text(
        "📊 <b>YOUR PROGRESS</b>\n\n"
        f"🎬 Unlocked batches: <b>{current}/{published_batch_count()}</b>\n"
        f"🎞 Unlocked videos: <b>{unlocked_video_count}</b>\n"
        f"👥 Verified referrals: <b>{refs}</b>\n"
        f"🎯 Next target: <b>{required_referrals_for_next(max(1,current))}</b>\n"
        f"⏳ Video expiry: <b>{DELETE_AFTER_HOURS} hours</b>",
        parse_mode=ParseMode.HTML,
    )

# ---------------- ADMIN ----------------

async def admin(update, context):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(
        "🛠 <b>ADMIN CONTROL PANEL</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=ADMIN_MENU,
    )

async def admin_upload_start(update, context):
    if update.effective_user.id != ADMIN_ID:
        return
    runtime = UPLOAD_RUNTIME.get(ADMIN_ID)
    if runtime and not runtime["closing"]:
        return await update.message.reply_text("Upload already running. Videos bhejo ya DONE dabao.", reply_markup=UPLOAD_MENU)

    old = drafts.find_one({"admin_id": ADMIN_ID})
    # Do not overwrite an old unfinished draft when updating the code.
    if old and (old.get("copied") or old.get("message_ids") or old.get("failed")):
        if old.get("message_ids") and not old.get("copied"):
            return await update.message.reply_text(
                "⚠️ Purane version ka unfinished draft mila hai. Existing uploads safe hain. "
                "Pehle old version me DONE complete karo, phir naya upload start karo."
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

    UPLOAD_RUNTIME[ADMIN_ID] = {
        "semaphore": asyncio.Semaphore(UPLOAD_CONCURRENCY),
        "tasks": set(), "closing": False, "seen": seen,
        "submitted": submitted, "cooldown_until": 0.0,
        "next_copy_at": 0.0, "storage_send_lock": asyncio.Lock(),
    }
    ADMIN_STATE[ADMIN_ID] = "upload_videos"
    await update.message.reply_text(
        "🚀 <b>STORAGE UPLOAD</b>\n\n"
        "Storage copy workers: <b>1 (sequential)</b>\n"
        f"Gap between copies: <b>{UPLOAD_COPY_GAP_SECONDS:g}s</b>\n"
        "Telegram RetryAfter aaya to poori queue required time tak pause hogi. "
        "Every successful storage copy MongoDB me save hoga. "
        "DONE par errors report honge; failures ko Retry Failed se dobara try kar sakte ho.",
        parse_mode=ParseMode.HTML,
        reply_markup=UPLOAD_MENU,
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
    # Never echo HTTP URLs, bot tokens or credentials into admin messages/logs.
    if isinstance(exc, RetryAfter):
        return "RetryAfter"
    if isinstance(exc, BadRequest):
        detail = str(exc).replace(BOT_TOKEN, "[TOKEN]")
        return ("BadRequest: " + detail)[:180]
    if isinstance(exc, Forbidden):
        return "Forbidden: bot lacks destination access or permissions"
    return type(exc).__name__


async def _storage_copy_job(bot, runtime, src_chat_id, source_id):
    # Exactly one storage-copy job runs at once. The extra lock explicitly
    # serializes Telegram sends so future changes to task concurrency cannot
    # accidentally bypass the SHARED cooldown and inter-copy gap.
    async with runtime["semaphore"]:
        reason = "Unknown"
        for attempt in range(UPLOAD_MAX_RETRIES):
            try:
                async with runtime["storage_send_lock"]:
                    loop = asyncio.get_running_loop()
                    # Both the normal 2-second gap and ANY worker's flood-wait
                    # apply to the next request. No copy can race past this gate.
                    delay = max(
                        runtime["cooldown_until"],
                        runtime["next_copy_at"],
                    ) - loop.time()
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
                        # Extend the BOT-WIDE storage cooldown; even queued
                        # work must honor Telegram's full wait before sending.
                        wait = float(exc.retry_after) + 1.0
                        runtime["cooldown_until"] = max(
                            runtime["cooldown_until"],
                            loop.time() + wait,
                        )
                        reason = f"RetryAfter ({round(wait)}s)"
                        log.warning(
                            "Storage rate-limited. Pausing storage queue for %ss",
                            round(wait),
                        )
                        # Never retry before the normal gap either.
                        runtime["next_copy_at"] = loop.time() + UPLOAD_COPY_GAP_SECONDS
                        continue
                    finally:
                        # Starts the 2-second gap AFTER the preceding attempt
                        # finishes (not merely between attempt start times).
                        runtime["next_copy_at"] = max(
                            runtime["next_copy_at"],
                            loop.time() + UPLOAD_COPY_GAP_SECONDS,
                        )

                # Retain the old crash-resilient MongoDB source→storage mapping.
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
                return
            except (TimedOut, NetworkError) as exc:
                reason = type(exc).__name__
                # The server may have copied a video before the timeout.
                # A retry might create an extra storage message.
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
                raise

        drafts.update_one(
            {"admin_id": ADMIN_ID},
            {"$pull": {"failed": {"source_message_id": source_id}}},
        )
        drafts.update_one(
            {"admin_id": ADMIN_ID},
            {"$addToSet": {"failed": {"source_message_id": source_id, "reason": reason}}},
        )


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
        return await update.message.reply_text("Upload closing hai. DONE process complete hone do.")
    mid = update.message.message_id
    if mid in runtime["seen"]:
        return
    runtime["seen"].add(mid)
    runtime["submitted"] += 1
    drafts.update_one({"admin_id": ADMIN_ID}, {"$inc": {"submitted": 1}})
    _enqueue_upload(context.bot, runtime, update.effective_chat.id, mid)
    n = runtime["submitted"]
    if n == 1 or n % 10 == 0:
        doc = drafts.find_one({"admin_id": ADMIN_ID}) or {}
        await update.message.reply_text(
            f"📤 Received: <b>{n}</b> | Copied: <b>{len(doc.get('copied', []))}</b> | "
            f"Failed: <b>{len(doc.get('failed', []))}</b>",
            parse_mode=ParseMode.HTML,
        )


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
        return await update.message.reply_text("Retry karne ke liye failed videos nahi hain.")
    runtime["closing"] = False
    ADMIN_STATE[ADMIN_ID] = "upload_videos"
    await update.message.reply_text(f"🔁 Retrying {len(failed)} failed videos…")
    for entry in failed:
        _enqueue_upload(context.bot, runtime, update.effective_chat.id, entry["source_message_id"])
    await _drain_upload(runtime)
    doc = drafts.find_one({"admin_id": ADMIN_ID}) or {}
    await update.message.reply_text(
        f"✅ Retry complete. Saved: {len(doc.get('copied', []))}; "
        f"Still failed: {len(doc.get('failed', []))}. DONE dabao.",
        reply_markup=UPLOAD_MENU,
    )


async def admin_upload_done(update, context, force_publish=False):
    runtime = UPLOAD_RUNTIME.get(ADMIN_ID)
    doc = drafts.find_one({"admin_id": ADMIN_ID})
    if not doc:
        return await update.message.reply_text("No active upload session.", reply_markup=ADMIN_MENU)
    if runtime:
        runtime["closing"] = True
    status = await update.message.reply_text("⏳ Copy jobs finish ho rahi hain…")
    await _drain_upload(runtime)
    doc = drafts.find_one({"admin_id": ADMIN_ID}) or {}
    failed = doc.get("failed", [])
    copied = sorted(doc.get("copied", []), key=lambda x: x["source_message_id"])
    if failed and not force_publish:
        details = "\n".join(
            f"• Video message {x['source_message_id']}: {esc(x.get('reason', 'Unknown'))}"
            for x in failed[:10]
        )
        return await status.edit_text(
            f"⚠️ <b>{len(failed)} VIDEOS FAILED</b>\n\n{details}\n\n"
            "🔁 Retry Failed dabao, phir DONE. Ya ✅ Publish Successful se failed skip karo.",
            parse_mode=ParseMode.HTML,
        )
    # Publish only once; the draft is retained until all DB inserts succeed.
    ids = [x["storage_message_id"] for x in copied]
    published = []
    for i in range(0, len(ids), 10):
        published.append(await publish_message_ids(ids[i:i+10]))
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
    )
    await update.message.reply_text("Admin menu", reply_markup=ADMIN_MENU)

async def admin_add_group(update, context):
    if update.effective_user.id != ADMIN_ID:
        return
    ADMIN_STATE[ADMIN_ID] = "group_chat_id"
    await update.message.reply_text(
        "➕ <b>ADD REQUIRED CHAT</b>\n\n"
        "Numeric group/channel ID bhejo.\n"
        "Example: <code>-1001234567890</code>\n\n"
        "Bot default se is chat ke liye <b>approval/join-request link</b> generate karega.\n"
        "Bot ko chat me admin + invite rights chahiye.",
        parse_mode=ParseMode.HTML,
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True),
    )

async def admin_group_flow(update, context):
    if update.effective_user.id != ADMIN_ID:
        return False

    st = ADMIN_STATE.get(ADMIN_ID)
    text = (update.message.text or "").strip()

    # Second-layer safety: never try to parse cancel as a chat ID/URL.
    if text.casefold() in {"cancel", "/cancel", "❌ cancel"}:
        await cancel(update, context)
        return True

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
                "❌ Approval link generate nahi hua.\n"
                "Bot ko target chat me admin + invite-user permission do.\n\n"
                f"Error: <code>{esc(type(e).__name__)}</code>",
                parse_mode=ParseMode.HTML,
            )
            return True

        required_chats.update_one(
            {"chat_id": cid},
            {
                "$set": {
                    "chat_id": cid,
                    "name": name,
                    "join_url": invite.invite_link,
                    "link_mode": "approval",
                    "enabled": True,
                    "position": required_chats.count_documents({}),
                    "updated_at": utcnow(),
                }
            },
            upsert=True,
        )
        ADMIN_STATE.pop(ADMIN_ID, None)

        await update.message.reply_text(
            "✅ <b>REQUIRED CHAT ADDED</b>\n\n"
            f"Name: <b>{esc(name)}</b>\n"
            f"ID: <code>{cid}</code>\n"
            "Link mode: <b>Approval / Join Request</b>\n\n"
            "Pending requests from this link will be recognized automatically.",
            parse_mode=ParseMode.HTML,
            reply_markup=ADMIN_MENU,
        )
        return True

    if st == "custom_link_chat_id":
        try:
            cid = int(text)
        except ValueError:
            await update.message.reply_text("Invalid numeric chat ID.")
            return True
        if not required_chats.find_one({"chat_id": cid}):
            await update.message.reply_text("Ye required chat configured nahi hai.")
            return True
        context.user_data["custom_link_chat_id"] = cid
        ADMIN_STATE[ADMIN_ID] = "custom_link_url"
        await update.message.reply_text("Ab custom t.me join/invite URL bhejo.")
        return True

    if st == "custom_link_url":
        if not (text.startswith("https://t.me/") or text.startswith("http://t.me/")):
            await update.message.reply_text("Valid Telegram t.me URL bhejo.")
            return True
        cid = context.user_data.pop("custom_link_chat_id")
        required_chats.update_one(
            {"chat_id": cid},
            {
                "$set": {
                    "join_url": text,
                    "link_mode": "custom",
                    "updated_at": utcnow(),
                }
            },
        )
        ADMIN_STATE.pop(ADMIN_ID, None)
        await update.message.reply_text(
            "✅ Custom link set.",
            reply_markup=ADMIN_MENU,
        )
        return True

    return False

async def set_custom_link(update, context):
    if update.effective_user.id != ADMIN_ID:
        return
    ADMIN_STATE[ADMIN_ID] = "custom_link_chat_id"
    await update.message.reply_text(
        "🔗 Custom link set karne ke liye required chat ka numeric ID bhejo.",
        reply_markup=ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True),
    )

async def admin_batches(update, context):
    rows = list(batches.find().sort("batch_no", ASCENDING))
    text = "📚 <b>VIDEO BATCHES</b>\n\n"
    if rows:
        text += "\n".join(
            f"• Batch <b>{x['batch_no']}</b> — {len(x.get('message_ids', []))} videos"
            for x in rows
        )
    else:
        text += "No batches."
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def admin_groups(update, context):
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
        text += (
            "\n\nCustom link chahiye to command use karo:\n"
            "<code>/setcustomlink</code>"
        )
    else:
        text += "No required chats."

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

# Removing a required chat does NOT delete its Telegram channel or revoke its invite link.
# This only removes its force-join requirement and related temporary verification data.
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
        await update.message.reply_text("ℹ️ Koi required group/channel configured nahi hai.", reply_markup=ADMIN_MENU)
        return
    await update.message.reply_text(
        f"🗑 <b>REMOVE REQUIRED CHANNEL</b>\n\n"
        f"Konsa group/channel force-join se hatana hai? ({total} total)\n"
        "Channel khud delete nahi hoga; sirf bot ki required list se hatega.",
        parse_mode=ParseMode.HTML,
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
            "Isko force-join list se remove karna hai? Telegram group/channel delete nahi hoga.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Remove", callback_data=f"rm_yes:{chat_id}")],
                [InlineKeyboardButton("⬅️ Back", callback_data="rm_list:0"),
                 InlineKeyboardButton("❌ Cancel", callback_data="rm_cancel")],
            ]),
        )
        return
    if data.startswith("rm_yes:"):
        # Delete only the selected required chat; a repeat click is harmless.
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
    total = users.count_documents({})
    verified = users.count_documents({"verified": True})
    agg = list(
        users.aggregate(
            [{"$group": {"_id": None, "n": {"$sum": "$verified_referrals"}}}]
        )
    )
    refs = agg[0]["n"] if agg else 0
    cutoff = utcnow() - timedelta(hours=PENDING_REQUEST_TTL_HOURS)
    active_pending = pending_requests.count_documents(
        {"requested_at": {"$gte": cutoff}}
    )

    await update.message.reply_text(
        "📊 <b>BOT STATS</b>\n\n"
        f"👤 Total users: <b>{total}</b>\n"
        f"✅ Verified: <b>{verified}</b>\n"
        f"🤝 Successful referrals: <b>{refs}</b>\n"
        f"⏳ Active pending join requests: <b>{active_pending}</b>\n"
        f"📦 Batches: <b>{published_batch_count()}</b>\n"
        f"🗑 Pending deletions: <b>{pending_deletions.count_documents({})}</b>",
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
    ADMIN_STATE.pop(ADMIN_ID, None)
    drafts.delete_one({"admin_id": ADMIN_ID})
    context.user_data.pop("custom_link_chat_id", None)
    await update.message.reply_text("Cancelled.", reply_markup=ADMIN_MENU)

async def text_router(update, context):
    txt = (update.message.text or "").strip()
    normalized = txt.casefold()

    if update.effective_user.id == ADMIN_ID and normalized in {"cancel", "/cancel", "❌ cancel"}:
        return await cancel(update, context)

    if update.effective_user.id == ADMIN_ID:
        if await admin_group_flow(update, context):
            return

        if txt == "🔁 Retry Failed":
            return await admin_retry_failed(update, context)
        if txt == "✅ Publish Successful":
            return await admin_upload_done(update, context, force_publish=True)
        if ADMIN_STATE.get(ADMIN_ID) == "upload_videos" and txt.upper() == "DONE":
            return await admin_upload_done(update, context)

        if txt == "📤 Upload Videos":
            return await admin_upload_start(update, context)
        if txt == "📚 Video Batches":
            return await admin_batches(update, context)
        if txt == "➕ Add Required Group":
            return await admin_add_group(update, context)
        if txt == "👥 Required Groups":
            return await admin_groups(update, context)
        if txt == "🗑 Remove Required Channel":
            return await admin_remove_chat_menu(update, context)
        if txt == "📊 Bot Stats":
            return await admin_stats(update, context)
        if txt == "❌ Cancel":
            return await cancel(update, context)

    if txt == "🎬 My Videos":
        return await my_videos(update, context)
    if txt == "🔓 Unlock Next 10":
        return await unlock_next(update, context)
    if txt == "👥 My Referral":
        return await referral(update, context)
    if txt == "📊 My Progress":
        return await progress(update, context)

async def checkme(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    chats = active_required_chats()
    if not chats:
        return await update.message.reply_text("No required chats configured.")

    results = await asyncio.gather(
        *(check_one_chat(context.bot, ch, uid) for ch in chats),
        return_exceptions=True,
    )

    lines = ["🔎 <b>Membership Check</b>\n"]
    for ch, result in zip(chats, results):
        if isinstance(result, Exception):
            state = "error"
        else:
            _, state = result

        icon = {"member": "✅", "pending": "⏳", "missing": "❌"}.get(state, "⚠️")
        mode = ch.get("link_mode", "custom")
        pending_db = has_active_pending_request(ch["chat_id"], uid)

        lines.append(
            f"{icon} {esc(ch['name'])}: <b>{state}</b>\n"
            f"   mode=<code>{esc(mode)}</code> pending_record=<code>{str(pending_db).lower()}</code>"
        )

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


def main():
    mongo.admin.command("ping")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_stop(post_stop)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CommandHandler("checkme", checkme))
    app.add_handler(CommandHandler("setcustomlink", set_custom_link))

    app.add_handler(ChatJoinRequestHandler(on_join_request))
    app.add_handler(ChatMemberHandler(on_chat_member_update, ChatMemberHandler.CHAT_MEMBER))

    app.add_handler(CallbackQueryHandler(remove_chat_callback, pattern=r"^rm_(?:list|pick|yes|cancel)(?::.*)?$"))
    app.add_handler(
        CallbackQueryHandler(verify_join, pattern=r"^verify_join$")
    )
    app.add_handler(
        CallbackQueryHandler(noop_callback, pattern=r"^noop$")
    )

    # Video upload handler first; it only acts when admin upload mode is active.
    app.add_handler(
        MessageHandler(
            filters.VIDEO & filters.ChatType.PRIVATE,
            admin_collect_video,
        )
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE,
            text_router,
        )
    )

    app.run_polling(
        drop_pending_updates=False,
        allowed_updates=Update.ALL_TYPES,
    )

if __name__ == "__main__":
    main()
