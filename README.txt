Video Bot — Admin Broadcast

Broadcast changes:
- User-facing Announcements controls are removed.
- /stopbroadcast, /subscribe and /unsubscribe are not registered.
- Broadcast targets every user ID stored in the MongoDB users collection.
- Delivery failures (for example, a user blocking the bot) are counted as failed.
- Existing human_verification.py is not included or modified; keep your own file unchanged.
