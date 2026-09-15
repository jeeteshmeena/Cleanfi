from pathlib import Path
from mutagen import File
from mutagen.id3 import ID3, ID3NoHeaderError, TIT2, TPE1, TCON, TDRC, TALB, TPE2, COMM, APIC

def clean_and_apply_metadata(input_path, output_path, *, title=None, artist=None, genre=None, year=None, cover=None, album=None, album_artist=None, comment=None):
    p=Path(input_path); q=Path(output_path); q.write_bytes(p.read_bytes())
    f=File(str(q), easy=False)
    if f is None: raise ValueError(f'Unsupported audio container: {p.suffix}')
    old_title=None
    try:
        if hasattr(f,'get'): old_title=(f.get('title') or [None])[0]
        if p.suffix.lower()=='.mp3':
            try: tags=ID3(str(q)); old_title=(tags.get('TIT2').text[0] if tags.get('TIT2') else old_title); tags.delete(str(q))
            except ID3NoHeaderError: pass
            tags=ID3();
            if title is not None: tags.add(TIT2(encoding=3,text=title))
            elif old_title: tags.add(TIT2(encoding=3,text=old_title))
            if artist: tags.add(TPE1(encoding=3,text=artist))
            if genre: tags.add(TCON(encoding=3,text=genre))
            if year: tags.add(TDRC(encoding=3,text=str(year)))
            if album: tags.add(TALB(encoding=3,text=album))
            if album_artist: tags.add(TPE2(encoding=3,text=album_artist))
            if comment: tags.add(COMM(encoding=3,lang='eng',desc='',text=comment))
            if cover:
                data=Path(cover).read_bytes(); mime='image/png' if Path(cover).suffix.lower()=='.png' else 'image/jpeg'; tags.add(APIC(encoding=3,mime=mime,type=3,desc='Cover',data=data))
            tags.save(str(q),v2_version=3)
        else:
            # Mutagen's easy tags are portable for common Vorbis/MP4 containers.
            if f.tags is None: f.add_tags()
            f.tags.clear()
            if title is not None: f.tags['title']=[title]
            elif old_title: f.tags['title']=[old_title]
            for k,v in [('artist',artist),('genre',genre),('date',str(year) if year else None),('album',album),('albumartist',album_artist),('comment',comment)]:
                if v: f.tags[k]=[v]
            f.save()
            # Cover support outside MP3 is container-specific; report unsupported embedding rather than corrupting audio.
            if cover: raise ValueError(f'Cover embedding is not implemented for {p.suffix.lower()} yet')
    return str(q)
