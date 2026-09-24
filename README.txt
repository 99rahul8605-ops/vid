Video Bot: easy /set formatting + single-message verification + Broadcast.

Replace bot.py and broadcast.py from this ZIP. Keep your existing external verification file and .env unchanged.

/set: Enter regular Telegram text; use Telegram bold/italic/links and line breaks. The bot converts the actual rich-text formatting to safe Telegram HTML and previews it before saving. Buttons remain plain-text; timer remains numeric. No manual Markdown/HTML required.
