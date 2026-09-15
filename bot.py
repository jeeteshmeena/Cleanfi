# Cleanfi - Telegram audiobook metadata cleaner
# Production implementation will be added in the next hardening pass.

import os
from pyrogram import Client, filters

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

app = Client("cleanfi", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

@app.on_message(filters.command("start"))
async def start(_, message):
    await message.reply_text("Cleanfi is configured. Use /help for commands.")

@app.on_message(filters.command("help"))
async def help_cmd(_, message):
    await message.reply_text("Commands: /range /meta /startjob /status /cancel /retry /failed /test")

app.run()
