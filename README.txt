Video Bot: easy /set formatting + single-message verification + Broadcast.

Replace bot.py and broadcast.py from this ZIP. Keep your existing external verification file and .env unchanged.

/set: Enter regular Telegram text; use Telegram bold/italic/links and line breaks. The bot converts the actual rich-text formatting to safe Telegram HTML and previews it before saving. Buttons remain plain-text; timer remains numeric. No manual Markdown/HTML required.

Link previews are disabled globally for bot-generated text, edits and previews via Telegram application defaults. Text broadcasts use send_message (rather than copy_message) with preserved Telegram HTML entities and previews explicitly disabled. Media broadcasts still use copy_message. External verification module is not included or modified.
Requires python-telegram-bot >=20.8 for LinkPreviewOptions defaults.
