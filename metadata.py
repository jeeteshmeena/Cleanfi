"""Unicode-safe, format-aware metadata cleaner for Cleanfi.

The audio stream is copied byte-for-byte; no re-encoding is performed.
Existing metadata is removed and only the requested metadata is written.
"""
from pathlib import Path
import base64
import shutil
from mutagen.id3 import ID3, APIC, TIT2, TPE1, TCON, TDRC, TALB, TPE2, COMM, ID3NoHeaderError
from mutagen.mp4 import MP4, MP4Cover
from mutagen.flac import FLAC, Picture
from mutagen.oggopus import OggOpus
from mutagen.oggvorbis import OggVorbis
from mutagen.wave import WAVE
from mutagen.aiff import AIFF
from mutagen.asf import ASF


def _mime(data: bytes) -> str:
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    return "image/jpeg"


def _text(v):
    return None if v is None else str(v)


def read_original_title(path: str):
    """Read title without normalising/transliterating Unicode."""
    p = Path(path); ext = p.suffix.lower()
    try:
        if ext == ".mp3":
            try:
                tags = ID3(p); value = tags.get("TIT2")
                return str(value.text[0]) if value and value.text else None
            except Exception:
                return None
        if ext in {".m4a", ".mp4"}:
            f = MP4(p); return (f.get("\xa9nam") or [None])[0]
        if ext == ".flac":
            f = FLAC(p); return (f.get("title") or [None])[0]
        if ext == ".ogg":
            f = OggVorbis(p); return (f.get("title") or [None])[0]
        if ext == ".opus":
            f = OggOpus(p); return (f.get("title") or [None])[0]
        if ext == ".wav":
            f = WAVE(p)
            if f.tags:
                value = f.tags.get("TIT2")
                return str(value.text[0]) if value and value.text else None
        if ext in {".aiff", ".aif"}:
            f = AIFF(p)
            if f.tags:
                value = f.tags.get("TIT2")
                return str(value.text[0]) if value and value.text else None
        if ext == ".wma":
            f = ASF(p); return (f.get("Title") or [None])[0]
    except Exception:
        return None
    return None


def _id3(path, title, artist, genre, year, album, album_artist, comment, cover):
    try: tags = ID3(path)
    except ID3NoHeaderError: tags = ID3()
    tags.clear()
    if title is not None: tags.add(TIT2(encoding=3, text=[_text(title)]))
    if artist: tags.add(TPE1(encoding=3, text=[_text(artist)]))
    if genre: tags.add(TCON(encoding=3, text=[_text(genre)]))
    if year: tags.add(TDRC(encoding=3, text=[_text(year)]))
    if album: tags.add(TALB(encoding=3, text=[_text(album)]))
    if album_artist: tags.add(TPE2(encoding=3, text=[_text(album_artist)]))
    if comment: tags.add(COMM(encoding=3, lang="eng", desc="", text=[_text(comment)]))
    if cover: tags.add(APIC(encoding=3, mime=_mime(cover), type=3, desc="Cover", data=cover))
    tags.save(path, v2_version=4)


def _comments(f, title, artist, genre, year, album, album_artist, comment):
    f.clear()
    if title is not None: f["title"] = [_text(title)]
    if artist: f["artist"] = [_text(artist)]
    if genre: f["genre"] = [_text(genre)]
    if year: f["date"] = [_text(year)]
    if album: f["album"] = [_text(album)]
    if album_artist: f["albumartist"] = [_text(album_artist)]
    if comment: f["comment"] = [_text(comment)]


def _ogg_cover(f, cover):
    if not cover: return
    p = Picture(); p.type = 3; p.mime = _mime(cover); p.desc = "Cover"; p.data = cover
    f["metadata_block_picture"] = [base64.b64encode(p.write()).decode("ascii")]


def _id3_object(tags, title, artist, genre, year, album, album_artist, comment, cover):
    tags.clear()
    if title is not None: tags.add(TIT2(encoding=3, text=[_text(title)]))
    if artist: tags.add(TPE1(encoding=3, text=[_text(artist)]))
    if genre: tags.add(TCON(encoding=3, text=[_text(genre)]))
    if year: tags.add(TDRC(encoding=3, text=[_text(year)]))
    if album: tags.add(TALB(encoding=3, text=[_text(album)]))
    if album_artist: tags.add(TPE2(encoding=3, text=[_text(album_artist)]))
    if comment: tags.add(COMM(encoding=3, lang="eng", desc="", text=[_text(comment)]))
    if cover: tags.add(APIC(encoding=3, mime=_mime(cover), type=3, desc="Cover", data=cover))


def clean_and_apply_metadata(input_path, output_path, *, title=None, artist=None, genre=None, year=None, cover=None, album=None, album_artist=None, comment=None):
    src = Path(input_path); dst = Path(output_path); shutil.copy2(src, dst)
    ext = src.suffix.lower(); cover_bytes = Path(cover).read_bytes() if cover and Path(cover).is_file() else None

    if ext == ".mp3": _id3(dst, title, artist, genre, year, album, album_artist, comment, cover_bytes); return

    if ext in {".m4a", ".mp4"}:
        f = MP4(dst); f.clear()
        if title is not None: f["\xa9nam"] = [_text(title)]
        if artist: f["\xa9ART"] = [_text(artist)]
        if genre: f["\xa9gen"] = [_text(genre)]
        if year: f["\xa9day"] = [_text(year)]
        if album: f["\xa9alb"] = [_text(album)]
        if album_artist: f["aART"] = [_text(album_artist)]
        if comment: f["\xa9cmt"] = [_text(comment)]
        if cover_bytes:
            fmt = MP4Cover.FORMAT_JPEG if _mime(cover_bytes) == "image/jpeg" else MP4Cover.FORMAT_PNG
            f["covr"] = [MP4Cover(cover_bytes, imageformat=fmt)]
        f.save(); return

    if ext == ".flac":
        f = FLAC(dst); _comments(f, title, artist, genre, year, album, album_artist, comment)
        if cover_bytes:
            p = Picture(); p.type = 3; p.mime = _mime(cover_bytes); p.desc = "Cover"; p.data = cover_bytes; f.add_picture(p)
        f.save(); return

    if ext == ".ogg":
        f = OggVorbis(dst); _comments(f, title, artist, genre, year, album, album_artist, comment); _ogg_cover(f, cover_bytes); f.save(); return
    if ext == ".opus":
        f = OggOpus(dst); _comments(f, title, artist, genre, year, album, album_artist, comment); _ogg_cover(f, cover_bytes); f.save(); return

    if ext == ".wav":
        f = WAVE(dst)
        if f.tags is None: f.add_tags()
        _id3_object(f.tags, title, artist, genre, year, album, album_artist, comment, cover_bytes)
        f.save(); return

    if ext in {".aiff", ".aif"}:
        f = AIFF(dst)
        if f.tags is None: f.add_tags()
        _id3_object(f.tags, title, artist, genre, year, album, album_artist, comment, cover_bytes)
        f.save(); return

    if ext == ".wma":
        f = ASF(dst); f.tags = {}
        mapping = {"Title": title, "Author": artist, "WM/Genre": genre, "WM/Year": year, "WM/AlbumTitle": album, "WM/AlbumArtist": album_artist, "Description": comment}
        for k, v in mapping.items():
            if v is not None and v != "": f[k] = [str(v)]
        f.save(); return

    if ext == ".aac":
        raise ValueError("Raw AAC has no portable metadata container")

    raise ValueError(f"Unsupported audio container: {ext}")
