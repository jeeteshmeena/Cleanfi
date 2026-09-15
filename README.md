# Cleanfi

Telegram audiobook metadata cleaner/repacker.

## Features

- Per-query metadata profiles: every `/range` job keeps its own metadata.
- Cleans old tags before writing the configured metadata.
- Preserves the original title unless `title=` is explicitly supplied.
- Supports common MP3, M4A/MP4, FLAC, OGG/Opus and FFmpeg-compatible containers.
- Avoids audio re-encoding where the container permits safe stream-copy metadata replacement.
- FloodWait retry handling.
- Progress and final failed-ID reporting.
- Temporary-file cleanup.
- Admin allowlist.

## Query workflow

```text
/range 1250 1300
/meta artist="Artist A" genre="Romance" year=2026
/startjob
```

A second range can use different metadata:

```text
/range 1301 1350
/meta artist="Artist B" genre="Thriller" year=2025
/startjob
```

Optional fields:

```text
artist="..." genre="..." year=2026 title="..." album="..." album_artist="..." comment="..."
```

`title` is optional. If omitted, Cleanfi reads the source title and writes it back after clearing the old tags.

## Setup

1. Install Python 3.10+.
2. Install FFmpeg on the VPS.
3. Install packages:

```bash
pip install -r requirements.txt
```

4. Copy `.env.example` to `.env` and fill in the values.
5. The Telegram account running the client must have access to both source and target channels.
6. Start:

```bash
python3 bot.py
```

### Environment

- `API_ID`, `API_HASH`: Telegram API credentials.
- `BOT_TOKEN`: BotFather token.
- `SOURCE_CHAT_ID`: source/database channel.
- `TARGET_CHAT_ID`: destination channel.
- `ADMIN_IDS`: comma-separated Telegram user IDs allowed to operate Cleanfi.
- `MAX_WORKERS`: controlled processing concurrency; start with 2 and increase only after testing FloodWait/resource usage.
- `TEMP_DIR`: temporary working directory.

## Important Telegram size note

The current implementation uses a Pyrogram bot session. Telegram Bot API limits still apply to bot accounts; Pyrogram does not magically remove those limits. If the audiobook files exceed the bot-account limits you encounter, Cleanfi should be switched to a user/MTProto worker session for downloading/uploading large files. Do not put a user session file or API secrets in GitHub.

## Cover notes

Cover embedding is format/container-specific. MP3, MP4/M4A and FLAC have direct cover paths in the metadata engine. OGG/Opus and some other containers need additional format-specific handling before a cover can be embedded safely. Cleanfi reports unsupported cover operations instead of silently corrupting audio.

## Security

- Never commit `.env`, Telegram session files, or API secrets.
- Keep `ADMIN_IDS` restricted.
- Keep source/target channel IDs private where appropriate.
- Use a dedicated VPS user and restricted permissions for the service.
