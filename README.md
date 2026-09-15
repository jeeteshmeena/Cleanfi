# Cleanfi

Production-oriented Telegram audiobook metadata cleaner/repacker.

## Current workflow

1. Set the source and target channels with `/source @channel` and `/target @channel`.
2. Create a job with `/range START END`.
3. Configure Artist, Genre, Year, Album, Album Artist, Comment and Cover for that job.
4. Press **START** or use `/startjob JOB_ID`.
5. Cleanfi downloads each audio, removes old metadata, preserves the original title, writes the requested metadata, embeds artwork where supported, and uploads the result to the target channel.

Every job stores its own source/target and metadata profile, so different ranges can safely use different settings.

## Features

- Telegram-visible Artist/Performer and Title are explicitly supplied during upload.
- Original title is preserved exactly as Unicode text, including Hindi/Devanagari, unless a future title editor is added.
- MP3 ID3 uses UTF-8 Unicode frames and ID3v2.4 to avoid Hindi/Unicode mojibake.
- Existing tags are cleared before writing configured metadata.
- Artist, Genre, Year, Album, Album Artist and Comment support.
- Cover is embedded in supported containers and also sent as Telegram thumbnail.
- MP3, M4A/MP4, FLAC, OGG, Opus, WAV/AIFF and WMA handling through Mutagen where supported.
- Raw AAC/ADTS is never silently transcoded or corrupted when its container cannot safely carry metadata.
- Inclusive message ranges.
- Queue/worker processing with configurable workers.
- Live progress bar with processed/failed/skipped counts, speed, elapsed time and ETA.
- FloodWait-aware Telegram operations: the bot waits for Telegram's requested duration and resumes instead of crashing.
- Atomic persistent job state and startup recovery to paused state; already processed/skipped IDs are not repeated on resume/retry.
- Failed-file list and retry support.
- Admin-only private control surface.
- Structured inline menu plus command interface.
- Temporary files are removed after each item.

## Commands

```text
/start
/help
/source @channelusername
/target @channelusername
/range 1250 1300
/meta artist="Artist A" genre="Romance" year=2026 album="Book A"
/cover JOB_ID
/startjob JOB_ID
/status JOB_ID
/cancel JOB_ID
/retry JOB_ID
/failed JOB_ID
/test MESSAGE_ID
/jobs
```

## Metadata safety

Cleanfi never transliterates, ASCII-normalizes, or manually re-encodes the title. The original title is read from the source container and passed through unchanged. For MP3, ID3v2.4 UTF-8 frames are used. Telegram's `send_audio` upload also receives the title and performer explicitly, so the visible Telegram Artist/Author field does not depend only on the embedded tags. citeturn2search0

Audio streams are copied without re-encoding. Metadata changes therefore do not intentionally reduce audio quality.

## Important format note

Raw AAC/ADTS does not provide a portable container for arbitrary embedded cover/tag data. Cleanfi preserves the audio bytes rather than silently changing the container.

WAV/AIFF/WMA metadata capabilities depend on the exact container/player implementation. Failures are recorded instead of pretending unsupported metadata was written.

## VPS setup

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip ffmpeg git
sudo useradd --system --create-home --shell /usr/sbin/nologin cleanfi || true
sudo mkdir -p /opt/cleanfi
sudo chown -R cleanfi:cleanfi /opt/cleanfi
sudo -u cleanfi git clone https://github.com/jeeteshmeena/Cleanfi.git /opt/cleanfi
cd /opt/cleanfi
sudo -u cleanfi python3 -m venv .venv
sudo -u cleanfi .venv/bin/pip install --upgrade pip
sudo -u cleanfi .venv/bin/pip install -r requirements.txt
sudo cp .env.example .env
sudo chown cleanfi:cleanfi .env
sudo chmod 600 .env
sudo nano .env
```

Set `API_ID`, `API_HASH`, `BOT_TOKEN`, `ADMIN_IDS`. `SOURCE_CHAT_ID` and `TARGET_CHAT_ID` can be used as initial defaults; the bot can also resolve and persist channels with `/source` and `/target`.

Then:

```bash
sudo cp deploy/cleanfi.service /etc/systemd/system/cleanfi.service
sudo systemctl daemon-reload
sudo systemctl enable --now cleanfi
sudo systemctl status cleanfi
journalctl -u cleanfi -f
```

The systemd service starts `bot.py`, which loads the corrected Cleanfi runtime implementation.
