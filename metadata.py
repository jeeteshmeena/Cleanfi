"""Format-aware metadata cleaner for Cleanfi.

Audio bytes are copied without re-encoding. Tags are wiped first, then requested
fields are written. Artwork is embedded where the container supports it.
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

IMAGE_JPEG = b"\xff\xd8\xff"

def _mime(data):
    if data.startswith(IMAGE_JPEG): return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"): return "image/png"
    return "image/jpeg"

def _id3(path, title, artist, genre, year, album, album_artist, comment, cover):
    try: tags=ID3(path)
    except ID3NoHeaderError: tags=ID3()
    tags.clear()
    if title is not None: tags.add(TIT2(encoding=3,text=[str(title)]))
    if artist: tags.add(TPE1(encoding=3,text=[str(artist)]))
    if genre: tags.add(TCON(encoding=3,text=[str(genre)]))
    if year: tags.add(TDRC(encoding=3,text=[str(year)]))
    if album: tags.add(TALB(encoding=3,text=[str(album)]))
    if album_artist: tags.add(TPE2(encoding=3,text=[str(album_artist)]))
    if comment: tags.add(COMM(encoding=3,lang="eng",desc="",text=[str(comment)]))
    if cover: tags.add(APIC(encoding=3,mime=_mime(cover),type=3,desc="Cover",data=cover))
    tags.save(path, v2_version=3)

def _comments(f, title, artist, genre, year, album, album_artist, comment):
    f.clear()
    if title is not None: f["title"]=[str(title)]
    if artist: f["artist"]=[str(artist)]
    if genre: f["genre"]=[str(genre)]
    if year: f["date"]=[str(year)]
    if album: f["album"]=[str(album)]
    if album_artist: f["albumartist"]=[str(album_artist)]
    if comment: f["comment"]=[str(comment)]

def _ogg_cover(f, cover):
    # Vorbis/Opus stores artwork in METADATA_BLOCK_PICTURE as base64.
    if not cover: return
    p=Picture(); p.type=3; p.mime=_mime(cover); p.desc="Cover"; p.data=cover
    f["metadata_block_picture"]=[base64.b64encode(p.write()).decode("ascii")]

def clean_and_apply_metadata(input_path, output_path, *, title=None, artist=None, genre=None, year=None, cover=None, album=None, album_artist=None, comment=None):
    src=Path(input_path); dst=Path(output_path)
    shutil.copy2(src,dst)
    ext=src.suffix.lower()
    cover_bytes=Path(cover).read_bytes() if cover and Path(cover).is_file() else None

    if ext==".mp3":
        _id3(dst,title,artist,genre,year,album,album_artist,comment,cover_bytes); return
    if ext in {".m4a",".mp4"}:
        f=MP4(dst); f.clear()
        if title is not None: f["\xa9nam"]=[str(title)]
        if artist: f["\xa9ART"]=[str(artist)]
        if genre: f["\xa9gen"]=[str(genre)]
        if year: f["\xa9day"]=[str(year)]
        if album: f["\xa9alb"]=[str(album)]
        if album_artist: f["aART"]=[str(album_artist)]
        if comment: f["\xa9cmt"]=[str(comment)]
        if cover_bytes: f["covr"]=[MP4Cover(cover_bytes, imageformat=MP4Cover.FORMAT_JPEG if _mime(cover_bytes)=="image/jpeg" else MP4Cover.FORMAT_PNG)]
        f.save(); return
    if ext==".flac":
        f=FLAC(dst); _comments(f,title,artist,genre,year,album,album_artist,comment)
        if cover_bytes:
            p=Picture(); p.type=3; p.mime=_mime(cover_bytes); p.desc="Cover"; p.data=cover_bytes; f.add_picture(p)
        f.save(); return
    if ext==".ogg":
        f=OggVorbis(dst); _comments(f,title,artist,genre,year,album,album_artist,comment); _ogg_cover(f,cover_bytes); f.save(); return
    if ext==".opus":
        f=OggOpus(dst); _comments(f,title,artist,genre,year,album,album_artist,comment); _ogg_cover(f,cover_bytes); f.save(); return
    if ext==".wav":
        # RIFF/WAVE commonly supports an ID3 chunk; Mutagen writes it without touching PCM.
        try: _id3(dst,title,artist,genre,year,album,album_artist,comment,cover_bytes)
        except Exception: pass
        return
    if ext in {".aiff",".aif"}:
        try: _id3(dst,title,artist,genre,year,album,album_artist,comment,cover_bytes)
        except Exception: pass
        return
    if ext==".wma":
        f=ASF(dst); f.tags={}
        mapping={"title":title,"author":artist,"genre":genre,"date":year,"WM/AlbumTitle":album,"WM/AlbumArtist":album_artist,"description":comment}
        for k,v in mapping.items():
            if v is not None and v!="": f[k]=[str(v)]
        f.save(); return
    if ext==".aac":
        # Raw ADTS AAC has no portable tag/artwork container. Do not corrupt the stream.
        return
    raise ValueError(f"Unsupported audio container: {ext}")
