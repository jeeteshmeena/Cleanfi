from pathlib import Path
from mutagen import File
from mutagen.id3 import ID3, APIC, TIT2, TPE1, TCON, TDRC, TALB, TPE2, COMM, ID3NoHeaderError
from mutagen.mp4 import MP4, MP4Cover
from mutagen.flac import FLAC, Picture
from mutagen.oggopus import OggOpus
from mutagen.oggvorbis import OggVorbis
from mutagen.wave import WAVE
from mutagen.aiff import AIFF
import shutil


def _clean_id3(path):
    try:
        tags=ID3(path)
    except ID3NoHeaderError:
        tags=ID3()
    tags.clear()
    tags.save(path)


def clean_and_apply_metadata(input_path, output_path, *, title=None, artist=None, genre=None, year=None, cover=None, album=None, album_artist=None, comment=None):
    src=Path(input_path); dst=Path(output_path)
    shutil.copy2(src,dst)
    ext=src.suffix.lower()
    cover_bytes=Path(cover).read_bytes() if cover and Path(cover).exists() else None

    if ext=='.mp3':
        _clean_id3(dst)
        tags=ID3(dst)
        if title is not None: tags.add(TIT2(encoding=3,text=title))
        if artist: tags.add(TPE1(encoding=3,text=artist))
        if genre: tags.add(TCON(encoding=3,text=genre))
        if year: tags.add(TDRC(encoding=3,text=str(year)))
        if album: tags.add(TALB(encoding=3,text=album))
        if album_artist: tags.add(TPE2(encoding=3,text=album_artist))
        if comment: tags.add(COMM(encoding=3,lang='eng',desc='',text=comment))
        if cover_bytes: tags.add(APIC(encoding=3,mime='image/jpeg' if cover_bytes[:3]==b'\xff\xd8\xff' else 'image/png',type=3,desc='Cover',data=cover_bytes))
        tags.save(dst); return

    if ext in {'.m4a','.mp4'}:
        m=MP4(dst); m.clear()
        if title is not None: m['\xa9nam']=[title]
        if artist: m['\xa9ART']=[artist]
        if genre: m['\xa9gen']=[genre]
        if year: m['\xa9day']=[str(year)]
        if album: m['\xa9alb']=[album]
        if album_artist: m['aART']=[album_artist]
        if comment: m['\xa9cmt']=[comment]
        if cover_bytes: m['covr']=[MP4Cover(cover_bytes, imageformat=MP4Cover.FORMAT_JPEG if cover_bytes[:3]==b'\xff\xd8\xff' else MP4Cover.FORMAT_PNG)]
        m.save(); return

    if ext=='.flac':
        f=FLAC(dst); f.clear();
        if title is not None: f['title']=[title]
        if artist: f['artist']=[artist]
        if genre: f['genre']=[genre]
        if year: f['date']=[str(year)]
        if album: f['album']=[album]
        if album_artist: f['albumartist']=[album_artist]
        if comment: f['comment']=[comment]
        if cover_bytes:
            p=Picture(); p.type=3; p.mime='image/jpeg' if cover_bytes[:3]==b'\xff\xd8\xff' else 'image/png'; p.data=cover_bytes; f.add_picture(p)
        f.save(); return

    if ext=='.ogg':
        f=OggVorbis(dst); f.clear()
        if title is not None: f['title']=[title]
        if artist: f['artist']=[artist]
        if genre: f['genre']=[genre]
        if year: f['date']=[str(year)]
        if album: f['album']=[album]
        if album_artist: f['albumartist']=[album_artist]
        if comment: f['comment']=[comment]
        f.save(); return

    if ext=='.opus':
        f=OggOpus(dst); f.clear()
        if title is not None: f['title']=[title]
        if artist: f['artist']=[artist]
        if genre: f['genre']=[genre]
        if year: f['date']=[str(year)]
        if album: f['album']=[album]
        if album_artist: f['albumartist']=[album_artist]
        if comment: f['comment']=[comment]
        f.save(); return

    if ext in {'.wav','.aiff','.aif'}:
        # Mutagen support varies by container; clear/write only when supported.
        f=File(dst,easy=True)
        if f is not None:
            try: f.clear()
            except Exception: pass
            if title is not None: f['title']=[title]
            if artist: f['artist']=[artist]
            if genre: f['genre']=[genre]
            if year: f['date']=[str(year)]
            if album: f['album']=[album]
            if album_artist: f['albumartist']=[album_artist]
            if comment: f['comment']=[comment]
            try: f.save()
            except Exception: pass
        return

    if ext=='.wma':
        f=File(dst,easy=True)
        if f is None: return
        try: f.clear()
        except Exception: pass
        if title is not None: f['title']=[title]
        if artist: f['artist']=[artist]
        if genre: f['genre']=[genre]
        if year: f['date']=[str(year)]
        if album: f['album']=[album]
        if album_artist: f['albumartist']=[album_artist]
        if comment: f['comment']=[comment]
        try: f.save()
        except Exception: pass
        return

    if ext=='.aac':
        # Raw AAC/ADTS has no portable embedded artwork/tag container.
        # Keep audio bytes intact rather than falsely claiming metadata was embedded.
        return

    raise ValueError(f'Unsupported audio container: {ext}')
