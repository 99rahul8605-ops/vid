Video Bot: shareable force-join protected video links + persistent deletion retry.

Replace bot.py and broadcast.py from this ZIP. Keep your existing external verification file and .env unchanged.

NEW: Admin -> 🔗 Video Links
- Create a link for one video or a custom bulk bundle from already published batches.
- Selection examples:
  1:3        = batch 1, video 3
  1:1-5      = batch 1, videos 1 through 5
  batch:2    = whole batch 2
  1:2,1:5,3:1-4 = mixed custom bundle
- Generated deep links use /start=vl_<token>.
- On every link open, the bot checks all currently required groups/channels before delivery.
- Missing groups only are shown. Verify & Get Videos re-checks membership before sending.
- Special links do not require referral/unlock progress.
- Linked videos use protected delivery and the normal configurable auto-delete timer.
- Admin can list recent links and disable any active link.

Deletion worker: temporary Telegram/network failures are retried persistently; deletion jobs are removed only after confirmed deletion/already-gone responses.

/set supports normal Telegram formatting and global link previews remain disabled.


Direct link flow: /link -> send videos -> /done -> share generated deep link.
