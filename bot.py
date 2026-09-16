from pyrogram import filters
from cleanfi_bot import app, main as run_app, allowed, main_kb

@app.on_message(filters.private & filters.command("menu"))
async def menu_cmd(_, m):
    if allowed(m):
        await m.reply_text("CLEANFI", reply_markup=main_kb())

if __name__ == "__main__":
    run_app()
