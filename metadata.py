from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from mutagen import File
from mutagen.id3 import ID3, TIT2, TPE1, TCON, TDRC, TALB, TPE2, COMM, APIC
from mutagen.mp4 import MP4, MP4Cover
from mutagen.flac import FLAC, Picture
from mutagen.oggvorbis import OggVorbis
from mutagen.oggopus import OggOpus
from mutagen.wave import WAVE
from mutagen.aiff import AIFF


def _read_title(path: Path) -> Optional[str]:
    try:
        audio = File(path, easy=False)
        if not audio:
            return None
        tags = audio.tags
        if not tags:
            return None
        candidates = ["TIT2", "title", "©nam", "TITLE"]
        for key in candidates:
            value = tags.get(key)
            if value:
                if isinstance(value, list):
                    return str(value[0])
                return str(value)
    except Exception:
        return None
    return None


def _clear_and_write_mp3(path: Path, meta: dict, title: Optional[str], cover: Optional[bytes]):
    tags = ID3()
    final_title = meta.get("title") if meta.get("title") is not None else title
    if final_title:
        tags.add(TIT2(encoding=3, text=final_title))
    if meta.get("artist"):
        tags.add(TPE1(encoding=3, text=meta["artist"]))
    if meta.get("genre"):
        tags.add(TCON(encoding=3, text=meta["genre"]))
    if meta.get("year"):
        tags.add(TDRC(encoding=3, text=str(meta["year"])))
    if meta.get("album"):
        tags.add(TALB(encoding=3, text=meta["album"]))
    if meta.get("album_artist"):
        tags.add(TPE2(encoding=3, text=meta["album_artist"]))
    if meta.get("comment"):
        tags.add(COMM(encoding=3, lang="eng", desc="", text=meta["comment"]))
    if cover:
        tags.add(APIC(encoding=3, mime="image/jpeg", type=3, desc="Cover", data=cover))
    tags.save(path, v2_version=3)


def _clear_and_write_mp4(path: Path, meta: dict, title: Optional[str], cover: Optional[bytes]):
    audio = MP4(path)
    audio.clear()
    final_title = meta.get("title") if meta.get("title") is not None else title
    if final_title: audio["©nam"] = [final_title]
    if meta.get("artist"): audio["©ART"] = [meta["artist"]]
    if meta.get("genre"): audio["©gen"] = [meta["genre"]]
    if meta.get("year"): audio["©day"] = [str(meta["year"])]
    if meta.get("album"): audio["©alb"] = [meta["album"]]
    if meta.get("album_artist"): audio["aART"] = [meta["album_artist"]]
    if meta.get("comment"): audio["©cmt"] = [meta["comment"]]
    if cover: audio["covr"] = [MP4Cover(cover, imageformat=MP4Cover.FORMAT_JPEG)]
    audio.save()


def _clear_and_write_flac(path: Path, meta: dict, title: Optional[str], cover: Optional[bytes]):
    audio = FLAC(path)
    audio.clear()
    final_title = meta.get("title") if meta.get("title") is not None else title
    if final_title: audio["title"] = [final_title]
    for key in ("artist", "genre", "album", "album_artist", "comment"):
        if meta.get(key): audio[key] = [str(meta[key])]
    if meta.get("year"): audio["date"] = [str(meta["year"])]
    if cover:
        picture = Picture(); picture.type = 3; picture.mime = "image/jpeg"; picture.desc = "Cover"; picture.data = cover
        audio.add_picture(picture)
    audio.save()


def _clear_and_write_vorbis(path: Path, meta: dict, title: Optional[str], cover: Optional[bytes]):
    cls = OggOpus if path.suffix.lower() == ".opus" else OggVorbis
    audio = cls(path)
    audio.clear()
    final_title = meta.get("title") if meta.get("title") is not None else title
    if final_title: audio["title"] = [final_title]
    for key in ("artist", "genre", "album", "album_artist", "comment"):
        if meta.get(key): audio[key] = [str(meta[key])]
    if meta.get("year"): audio["date"] = [str(meta["year"])]
    audio.save()
    # OGG/Opus cover embedding is container-specific; reportable fallback is preferable to corrupting audio.
    if cover:
        raise ValueError("Cover embedding is not implemented for OGG/Opus in this safe metadata path")


def _copy_then_ffmpeg(path: Path, meta: dict, title: Optional[str], cover: Optional[bytes]):
    # Generic fallback: use FFmpeg stream-copy so audio is not re-encoded.
    if not shutil.which("ffmpeg"):
        raise RuntimeError("ffmpeg is required for this audio format")
    temp = path.with_suffix(path.suffix + ".clean.tmp")
    cmd = ["ffmpeg", "-y", "-i", str(path), "-map", "0:a", "-map_metadata", "-1", "-c:a", "copy"]
    final_title = meta.get("title") if meta.get("title") is not None else title
    fields = {"title": final_title, "artist": meta.get("artist"), "genre": meta.get("genre"), "date": str(meta["year"]) if meta.get("year") else None, "album": meta.get("album"), "album_artist": meta.get("album_artist"), "comment": meta.get("comment")}
    for k, v in fields.items():
        if v is not None: cmd += ["-metadata", f"{k}={v}"]
    cmd += [str(temp)]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    os.replace(temp, path)
    if cover:
        raise ValueError("Cover embedding is not supported by the generic fallback without remuxing")


def clean_and_apply(path: str, meta: dict, cover_path: Optional[str] = None) -> None:
    p = Path(path)
    original_title = _read_title(p) if meta.get("title") is None else None
    cover = Path(cover_path).read_bytes() if cover_path else None
    ext = p.suffix.lower()
    if ext == ".mp3":
        _clear_and_write_mp3(p, meta, original_title, cover)
    elif ext in {".m4a", ".mp4", ".aac"}:
        _clear_and_write_mp4(p, meta, original_title, cover)
    elif ext == ".flac":
        _clear_and_write_flac(p, meta, original_title, cover)
    elif ext in {".ogg", ".opus"}:
        _clear_and_write_vorbis(p, meta, original_title, cover)
    elif ext in {".wav", ".aiff", ".aif", ".wma"}:
        _copy_then_ffmpeg(p, meta, original_title, cover)
    else:
        _copy_then_ffmpeg(p, meta, original_title, cover)
