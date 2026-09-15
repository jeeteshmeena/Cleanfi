from __future__ import annotations

import asyncio
import os
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message

from metadata import clean_and_apply

load_dotenv()
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
SOURCE_CHAT_ID = int(os.environ["SOURCE_CHAT_ID"])
TARGET_CHAT_ID = int(os.environ["TARGET_CHAT_ID"])
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
MAX_WORKERS = max(1, int(os.getenv("MAX_WORKERS", "2")))
TEMP_DIR = Path(os.getenv("TEMP_DIR", "./tmp"))
TEMP_DIR.mkdir(parents=True, exist_ok=True)

app = Client("cleanfi_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

@dataclass
class Job:
    start: int
    end: int
    meta: dict = field(default_factory=dict)
    cover_path: Optional[str] = None
    status: str = "queued"
    processed: int = 0
    skipped: int = 0
    failed: list[int] = field(default_factory=list)
    task: Optional[asyncio.Task] = None

jobs: dict[int, Job] = {}
locks: set[tuple[int, int]] = set()


def admin(message: Message) -> bool:
    return message.from_user is not None and message.from_user.id in ADMIN_IDS


def parse_kv(text: str) -> dict:
    # Supports: artist="A B" genre="Romance" year=2026 cover="/tmp/x.jpg"
    out = {}
    pattern = re.compile(r'(artist|genre|year|title|album|album_artist|comment|cover)\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|(\S+))', re.I)
    for m in pattern.finditer(text):
        out[m.group(1).lower()] = next(v for v in m.groups()[1:] if v is not None)
    return out


async def safe_download(message: Message, path: Path):
    while True:
        try:
            return await app.download_media(message, file_name=str(path))
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)


async def safe_send_audio(path: Path, source: Message):
    while True:
        try:
            return await app.send_audio(
                TARGET_CHAT_ID,
                audio=str(path),
                caption=source.caption or None,
                duration=source.audio.duration if source.audio else None,
                title=source.audio.title if source.audio else None,
                performer=source.audio.performer if source.audio else None,
                file_name=source.audio.file_name if source.audio else path.name,
            )
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)


async def process_message(job: Job, msg_id: int, worker_dir: Path):
    msg = await app.get_messages(SOURCE_CHAT_ID, msg_id)
    if not msg or not msg.audio:
        job.skipped += 1
        return
    filename = msg.audio.file_name or f"audio_{msg_id}"
    path = worker_dir / Path(filename).name
    await safe_download(msg, path)
    try:
        clean_and_apply(str(path), job.meta, job.cover_path)
        await safe_send_audio(path, msg)
        job.processed += 1
    except Exception:
        job.failed.append(msg_id)
        raise
    finally:
        path.unlink(missing_ok=True)


async def run_job(job: Job, chat_id: int):
    key = (job.start, job.end)
    if key in locks:
        return
    locks.add(key)
    job.status = "running"
    worker_dir = Path(tempfile.mkdtemp(prefix="cleanfi_", dir=TEMP_DIR))
    sem = asyncio.Semaphore(MAX_WORKERS)

    async def one(mid: int):
        async with sem:
            try:
                await process_message(job, mid, worker_dir)
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
                try:
                    await process_message(job, mid, worker_dir)
                except Exception:
                    pass
            except Exception:
                pass

    try:
        await asyncio.gather(*(one(mid) for mid in range(job.start, job.end + 1)))
        job.status = "done"
        failed = ", ".join(map(str, job.failed)) if job.failed else "none"
        await app.send_message(chat_id, f"✦ Cleanfi job complete\n\nRange: {job.start}–{job.end}\nProcessed: {job.processed}\nSkipped: {job.skipped}\nFailed: {len(job.failed)}\nFailed IDs: {failed}")
    finally:
        shutil.rmtree(worker_dir, ignore_errors=True)
        locks.discard(key)


@app.on_message(filters.command("start") & filters.private)
async def start(_, m: Message):
    if not admin(m): return
    await m.reply_text("✦ Cleanfi ready.\n\n/range START END\n/meta artist=... genre=... year=... cover=...\n/startjob\n/status\n/cancel")


@app.on_message(filters.command("range") & filters.private)
async def range_cmd(_, m: Message):
    if not admin(m): return
    if len(m.command) != 3:
        return await m.reply_text("Usage: /range 1250 1300")
    start, end = map(int, m.command[1:])
    if start > end or end - start > 10000:
        return await m.reply_text("Invalid range. Maximum range size is 10,001 messages.")
    jobs[m.from_user.id] = Job(start, end)
    await m.reply_text(f"✦ Range saved: {start}–{end}\nNow send /meta ... then /startjob")


@app.on_message(filters.command("meta") & filters.private)
async def meta_cmd(_, m: Message):
    if not admin(m): return
    job = jobs.get(m.from_user.id)
    if not job:
        return await m.reply_text("Set a range first with /range START END")
    values = parse_kv(m.text.partition(" ")[2])
    if "cover" in values:
        cover = Path(values.pop("cover")).expanduser()
        if not cover.exists():
            return await m.reply_text("Cover file not found on server. Put the image in the configured server path first.")
        job.cover_path = str(cover)
    job.meta.update(values)
    await m.reply_text("✦ Metadata saved for this query. Title is preserved unless title=... is supplied.")


@app.on_message(filters.command("startjob") & filters.private)
async def startjob(_, m: Message):
    if not admin(m): return
    job = jobs.get(m.from_user.id)
    if not job:
        return await m.reply_text("Set /range first.")
    if job.task and not job.task.done():
        return await m.reply_text("A job is already running for you.")
    job.task = asyncio.create_task(run_job(job, m.chat.id))
    await m.reply_text(f"✦ Started {job.start}–{job.end}. Use /status for progress.")


@app.on_message(filters.command("status") & filters.private)
async def status(_, m: Message):
    if not admin(m): return
    job = jobs.get(m.from_user.id)
    if not job:
        return await m.reply_text("No job configured.")
    total = job.end - job.start + 1
    await m.reply_text(f"✦ Status: {job.status}\nProgress: {job.processed + job.skipped + len(job.failed)}/{total}\nProcessed: {job.processed}\nSkipped: {job.skipped}\nFailed: {len(job.failed)}")


@app.on_message(filters.command("cancel") & filters.private)
async def cancel(_, m: Message):
    if not admin(m): return
    job = jobs.get(m.from_user.id)
    if job and job.task and not job.task.done():
        job.task.cancel()
        job.status = "cancelled"
        await m.reply_text("✦ Job cancelled. Temporary files will be cleaned up.")
    else:
        await m.reply_text("No active job.")


if __name__ == "__main__":
    app.run()
