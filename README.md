# Cleanfi

Production-oriented Telegram audiobook metadata cleaner.

## Features

- Per-job metadata profiles: each range can have different Artist, Genre, Year, Album, Album Artist and Comment.
- Original title is preserved by default.
- Cleans existing tags before writing the configured values.
- MP3, M4A/MP4, FLAC, OGG, Opus, WAV/AIFF and WMA handling through Mutagen where supported.
- Cover upload per job for formats that support embedded artwork.
- Range processing is inclusive.
- FloodWait-aware download/upload loops.
- Job persistence in `jobs.json`.
- Admin-only private control surface.
- Inline metadata controls plus command interface.
- `/test`, `/status`, `/cancel`, `/retry`, `/failed` and `/jobs`.

## Important format note

Raw AAC/ADTS does not provide a portable container for arbitrary embedded cover/tag data. Cleanfi intentionally does not transcode it silently. It will preserve the audio bytes rather than claim unsupported metadata was written.

WAV/AIFF/WMA metadata capabilities depend on the exact container/tag implementation. The bot reports processing failures instead of pretending an unsupported tag was embedded.

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

Set API_ID, API_HASH, BOT_TOKEN, SOURCE_CHAT_ID, TARGET_CHAT_ID and ADMIN_IDS.

Then:

```bash
sudo cp deploy/cleanfi.service /etc/systemd/system/cleanfi.service
sudo systemctl daemon-reload
sudo systemctl enable --now cleanfi
sudo systemctl status cleanfi
journalctl -u cleanfi -f
```

## Usage

```text
/range 1250 1300
```

Configure with inline buttons, or:

```text
/meta artist="Artist A" genre="Romance" year=2026 album="Book A"
```

Then press **START** or:

```text
/startjob JOB_ID
```

For a cover, send the image with:

```text
/cover JOB_ID
```

The source message caption is preserved on upload. The original filename is preserved.
