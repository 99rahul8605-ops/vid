# Video Unlock & Referral Bot — English Edition

All built-in user-facing and administrator-facing bot messages are in English, including verification, referrals, video delivery, upload progress, errors, and required-chat removal. Telegram chat names and captions supplied by administrators are displayed as provided.

## Install

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
nano .env
python3 bot.py
```

Fill in `BOT_TOKEN`, `ADMIN_ID`, `MONGO_URI`, and `STORAGE_CHANNEL_ID` in `.env`. Keep your existing `.env` and MongoDB database when updating an installation. The ZIP contains only `.env.example`, never live credentials.

Add the bot as an administrator in the private video storage channel. For required groups/channels, grant it the administrator permissions needed to check membership, receive join requests, and create invite links.

## User experience

- `/start` checks membership in all required chats concurrently. Only missing chats are shown.
- An approval-required chat can recognize a recently captured pending join request; its recognition window is configurable with `PENDING_REQUEST_TTL_HOURS` (72 hours in this build).
- Successful verification unlocks the first video batch. Each additional batch requires successful, verified referrals, configured with `REFERRALS_PER_BATCH`.
- Delivered videos use Telegram's `protect_content=True` and are scheduled for deletion after `DELETE_AFTER_HOURS` (default: 12 hours). The deletion queue is stored in MongoDB to survive restarts.
- `/checkme` displays the user's membership, pending-request, or missing status for each required chat.

**Pending request limitation:** Telegram's Bot API does not reliably notify bots when another administrator dismisses a join request. A pending record may therefore remain valid until its configured expiry. A user session is not required by this build.

## Administrator menu

Send `/admin` in the bot's private chat. The menu includes **Upload Videos**, **Video Batches**, **Add Required Group**, **Required Groups**, **Remove Required Channel**, **Bot Stats**, and **Cancel**.

To add a required chat, select **Add Required Group** and send its numeric Telegram chat ID. The bot generates an approval-required invite link by default. To change an existing link, use `/setcustomlink` and follow the instructions. **Remove Required Channel** offers a selection list and confirmation; removing a chat does not delete the Telegram chat or any videos.

## Upload and live progress

1. Choose **Upload Videos** in the administrator menu.
2. Send or forward videos to the bot; they are copied to the private storage channel **without** storage-side forwarding restrictions.
3. A single live progress message updates about every three seconds. It shows received, stored, failed, pending, progress percentage, active source message, and any Telegram rate-limit cooldown.
4. Press **DONE** when you have finished sending videos. The bot waits for queued copies and reports any failures. Choose **Retry Failed** and then **DONE**, or choose **Publish Successful** to skip failed copies.
5. Successful videos are published in their original source-message order in batches of 10; a final partial batch is allowed.

Use **Upload Status** or `/uploadstatus` for an extra progress snapshot. The storage queue uses one worker with a minimum two-second gap between copy attempts and respects Telegram's `RetryAfter` wait times. Adjust the gap if needed:

```env
UPLOAD_CONCURRENCY=1
UPLOAD_DELAY_SECONDS=2
UPLOAD_PROGRESS_REFRESH_SECONDS=3
UPLOAD_MAX_RETRIES=5
```

A process restart does not resume transfers that were in flight. MongoDB retains the records of successful and failed copies for the unfinished draft. Finish any active upload on the old version before deploying an update.

**Content protection:** Stored video copies are unprotected, but delivered copies are protected. Protection restricts ordinary forwarding/saving in Telegram; it cannot prevent screen recording or external capture. If the storage channel itself has **Restrict Saving Content** enabled, disable that setting in Telegram if you want unrestricted storage copies.
