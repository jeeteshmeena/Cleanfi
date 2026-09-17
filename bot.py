import asyncio
import logging

import cleanfi_v2 as core
from pyrogram import filters, idle


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("cleanfi")


# High-priority fallback for the two entry commands. It intentionally uses
# plain private text matching instead of filters.command so a command-parser
# problem cannot make the bot appear completely dead. stop_propagation()
# prevents the original /start handler from replying a second time.
@core.app.on_message(filters.private & filters.text, group=-1)
async def _entry_fallback(_, message):
    text = (message.text or "").strip().split(maxsplit=1)[0].lower()
    if text not in ("/start", "/menu"):
        return
    user_id = message.from_user.id if message.from_user else None
    log.info("Incoming %s from user=%s", text, user_id)
    if user_id in core.ADMINS:
        await message.reply_text(
            "CLEANFI\n\nAudiobook metadata cleaner and repacker.",
            reply_markup=core.menu(),
        )
    else:
        log.warning("Unauthorized %s from user=%s", text, user_id)
    message.stop_propagation()


async def main_async():
    core.load()

    # Use a dedicated bot session name. The previous runtime used the generic
    # "cleanfi" session, which could leave an old/stale Pyrogram session in use.
    # Keep the old session untouched as a backup.
    try:
        core.app.name = "cleanfi_bot"
        if hasattr(core.app, "storage") and hasattr(core.app.storage, "name"):
            core.app.storage.name = "cleanfi_bot"
    except Exception:
        log.exception("Could not switch to the dedicated bot session name")

    await core.app.start()

    # Verify the authenticated identity immediately so a wrong/stale session
    # cannot look like a healthy but unresponsive bot.
    me = await core.app.get_me()
    log.info(
        "Cleanfi connected as @%s (id=%s, is_bot=%s)",
        me.username or "-",
        me.id,
        me.is_bot,
    )
    log.info("Admins configured: %d", len(core.ADMINS))
    log.info("Handler groups: %d", len(getattr(core.app.dispatcher, "groups", {})))

    if not me.is_bot:
        raise RuntimeError("The active Pyrogram session is not a bot account")

    if core.queue:
        core.queue_task = asyncio.create_task(core.worker(), name="cleanfi-queue-worker")
        log.info("Recovered %d queued job(s)", len(core.queue))

    try:
        await idle()
    finally:
        task = core.queue_task
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        try:
            await core.app.stop()
        except ConnectionError:
            pass
        log.info("Cleanfi stopped cleanly")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
