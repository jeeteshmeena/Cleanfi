# Cleanfi

Telegram audiobook metadata cleaner/repacker.

## Current production architecture

Pyrogram + Mutagen, persistent JSON job store, per-job metadata, inline job controls, FloodWait handling, temporary-file cleanup, and systemd deployment template.

## Setup

1. Copy `.env.example` to `.env` and fill API_ID, API_HASH, BOT_TOKEN, source/target chat IDs and ADMIN_IDS.
2. Install Python 3.10+ and FFmpeg if needed by future format handlers.
3. Create venv: `python3 -m venv .venv && . .venv/bin/activate`.
4. Install: `pip install -r requirements.txt`.
5. Test: `python bot.py`.
6. For systemd copy `deploy/cleanfi.service` to `/etc/systemd/system/`, then `sudo systemctl daemon-reload && sudo systemctl enable --now cleanfi`.

## Usage

`/range 1250 1300`

Then configure this job only:

`/meta artist="Artist A" genre="Romance" year=2026`

Then `/startjob` or the inline Start button.

Each job stores its own metadata, so later ranges can use different values.

## Commands

`/start`, `/help`, `/range START END`, `/meta key=value`, `/startjob`, `/status [JOB]`.

## Security

Only ADMIN_IDS can use the bot. Never commit `.env`, bot tokens, API hashes, or session files.

## Important

This repository is a functional baseline, not a guarantee that every audio container supports every metadata field or embedded artwork. Format-specific handlers should be extended and tested with representative files before processing a large library. Do not re-encode audio merely to change tags.
