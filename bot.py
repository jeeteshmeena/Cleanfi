import asyncio
import logging
import signal

import cleanfi_v2 as core
from pyrogram import filters
from pyrogram.handlers import RawUpdateHandler


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("cleanfi")

SHUTDOWN_TIMEOUT = 15
HEALTH_INTERVAL = 15
HEALTH_TIMEOUT = 10


async def _raw_update(_, update, users, chats):
    """Lowest-level update probe; bypasses all Pyrogram message filters."""
    log.info("RAW TELEGRAM UPDATE: %s", type(update).__name__)


core.app.add_handler(RawUpdateHandler(_raw_update), group=-1000)


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


async def _start_client():
    if core.app.is_connected:
        return
    log.info("Starting Cleanfi Telegram client...")
    await core.app.start()
    me = await asyncio.wait_for(core.app.get_me(), timeout=HEALTH_TIMEOUT)
    if not me.is_bot:
        raise RuntimeError("The active Pyrogram session is not a bot account")
    log.info(
        "Telegram connection ready: @%s | id=%s | is_bot=%s",
        me.username or "-",
        me.id,
        me.is_bot,
    )


async def _restart_client(reason):
    log.warning("Telegram connection unhealthy: %s; restarting client", reason)

    try:
        await asyncio.wait_for(core.app.stop(), timeout=SHUTDOWN_TIMEOUT)
    except asyncio.TimeoutError:
        log.exception("Timed out while stopping unhealthy Telegram client")
    except Exception:
        log.exception("Error while stopping unhealthy Telegram client")

    await asyncio.sleep(1)
    await _start_client()
    log.info("Telegram client recovered successfully")


async def _connection_watchdog(stop_event):
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=HEALTH_INTERVAL)
            break
        except asyncio.TimeoutError:
            pass

        if stop_event.is_set():
            break

        try:
            me = await asyncio.wait_for(core.app.get_me(), timeout=HEALTH_TIMEOUT)
            if not me.is_bot:
                raise RuntimeError("Telegram session is no longer a bot account")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            try:
                await _restart_client(exc)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Telegram client recovery failed; will retry")
        else:
            log.debug("Telegram connection health check OK")


async def main_async():
    core.load()
    stop_event = asyncio.Event()
    watchdog_task = None

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass

    await _start_client()

    log.info("Configured admin IDs: %s", sorted(core.ADMINS))
    log.info(
        "Source=%s | Target=%s | Queue=%d",
        core.settings.get("source", ""),
        core.settings.get("target", ""),
        len(core.queue),
    )

    if core.queue:
        core.queue_task = asyncio.create_task(
            core.worker(), name="cleanfi-queue-worker"
        )
        log.info("Recovered %d queued job(s)", len(core.queue))

    watchdog_task = asyncio.create_task(
        _connection_watchdog(stop_event),
        name="cleanfi-telegram-watchdog",
    )

    try:
        await stop_event.wait()
    finally:
        log.info("Shutdown requested; stopping Cleanfi")

        if watchdog_task and not watchdog_task.done():
            watchdog_task.cancel()
            try:
                await watchdog_task
            except asyncio.CancelledError:
                pass

        task = core.queue_task
        core.queue_task = None
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=SHUTDOWN_TIMEOUT)
            except asyncio.CancelledError:
                pass
            except asyncio.TimeoutError:
                log.warning("Queue worker did not stop within %ss", SHUTDOWN_TIMEOUT)
            except Exception:
                log.exception("Queue worker shutdown failed")

        try:
            if core.app.is_connected:
                await asyncio.wait_for(core.app.stop(), timeout=SHUTDOWN_TIMEOUT)
        except asyncio.TimeoutError:
            log.warning("Pyrogram shutdown timed out")
        except Exception:
            log.exception("Pyrogram shutdown failed")

        log.info("Cleanfi stopped")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
