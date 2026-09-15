# Cleanfi

Telegram audiobook metadata cleaner/repacker.

> Status: project scaffold. The production metadata engine, queue/resume system, inline UI, and deployment hardening still need to be implemented.

## Planned commands

- `/range START END`
- `/meta artist="..." genre="..." year="..."`
- `/startjob`
- `/status`
- `/cancel`
- `/retry`
- `/failed`
- `/test MESSAGE_ID`

## Configuration

Copy `.env.example` to `.env` and set Telegram credentials and channel IDs.

## Deployment

Install dependencies in a Python virtual environment and run `bot.py`. A systemd unit should be added for production deployment.
