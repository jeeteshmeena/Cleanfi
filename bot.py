import asyncio
import logging

import cleanfi_v2 as core
from pyrogram import filters, idle
from pyrogram.handlers import RawUpdateHandler


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("cleanfi")


async def _raw_update(_, update, users, chats):
    """Lowest-level update probe; bypasses all Pyrogram message filters."""
    log.info("RAW TELEGRAM UPDATE: %s", type(update).__name__)


# Register this explicitly instead of relying on the decorator API.  If Telegram
# delivers any update to this process, this handler must see it.
core.app.add_handler(RawUpdateHandler(_raw_update), group=-1000)


# Diagnostic fallback: this runs before normal message handlers and proves that
# private text messages are reaching the message dispatcher.
@core.app.on_message(filters.private & filters.text, group=-100)
async def _entry_fallback(_, message):
    text = (message.text or "").strip().split(maxsplit=1)[0].lower()
    user_id = message.from_user.id if message.from_user else None
    log.info("Incoming private message: user=%s text=%r", user_id, message.text)

    if text not in ("/start", "/menu"):
        return

    if user_id not in core.ADMINS:
        log.warning("Unauthorized %s from user=%s", text, user_id)
        message.stop_propagation()
        return

    try:
        await message.reply_text(
            "CLEANFI\n\nAudiobook metadata cleaner and repacker.",
            reply_markup=core.menu(),
        )
        log.info("Replied successfully to %s from user=%s", text, user_id)
    except Exception:
        log.exception("Failed to reply to %s from user=%s", text, user_id)
    message.stop_propagation()


async def main_async():
    core.load()

    log.info("Starting Cleanfi Telegram client...")
    await core.app.start()

    try:
        me = await core.app.get_me()
        log.info(
            "Telegram connection ready: @%s | id=%s | is_bot=%s",
            me.username or "-",
            me.id,
            me.is_bot,
        )
        log.info("Configured admin IDs: %s", sorted(core.ADMINS))
        log.info(
            "Source=%s | Target=%s | Queue=%d",
            core.settings.get("source", ""),
            core.settings.get("target", ""),
            len(core.queue),
        )

        if not me.is_bot:
            raise RuntimeError("The active Pyrogram session is not a bot account")

        if core.queue:
            core.queue_task = asyncio.create_task(
                core.worker(), name="cleanfi-queue-worker"
            )
            log.info("Recovered %d queued job(s)", len(core.queue))

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
