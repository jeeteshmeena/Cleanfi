import asyncio
import logging
import signal

import cleanfi_v2 as core
from pyrogram import idle


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("cleanfi")


async def main_async():
    core.load()

    # Use a dedicated bot session name. The previous runtime used the generic
    # "cleanfi" session, which could leave an old/stale Pyrogram session in use.
    # Keep the old session untouched as a backup; Pyrogram will create/load
    # cleanfi_bot.session for this bot identity.
    try:
        core.app.name = "cleanfi_bot"
        if hasattr(core.app, "storage") and hasattr(core.app.storage, "name"):
            core.app.storage.name = "cleanfi_bot"
    except Exception:
        log.exception("Could not switch to the dedicated bot session name")

    await core.app.start()

    # Verify the authenticated identity immediately. This turns a silent
    # wrong-session problem into an explicit startup error/log entry.
    me = await core.app.get_me()
    log.info("Cleanfi connected as @%s (id=%s, is_bot=%s)", me.username or "-", me.id, me.is_bot)
    log.info("Admins configured: %d", len(core.ADMINS))
    log.info("Registered handlers: %d groups", len(getattr(core.app.dispatcher, "groups", {})))

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
