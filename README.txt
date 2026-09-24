Video Bot: single-message force-join verification fix

After a user passes the required-group verification, the bot edits the existing verification message with the success text and menu. It no longer sends a second “Your video menu is ready” message.

Replace bot.py and broadcast.py from this ZIP. Keep your existing human_verification.py and .env unchanged.
