import psycopg2 as pg
import pandas as pd
import glob
import os
import re
import mutagen

##############################################################################
##                         PostgreSQL interaction                           ##
##############################################################################

def psql_to_df(query, config):
    """
    Runs a query against a database with the given configuration, and returns
    a dataframe as output.
    """
    conn = pg.connect(**config)
    cur = conn.cursor()
    cur.execute(query)
    columns = [col.name for col in cur.description]
    data = cur.fetchall()
    cur.close()
    conn.close()
    df = pd.DataFrame(data, columns=columns)
    return df

def psql_execute(query, config, values=None):
    """
    Executes a query with optional values. Useful for inserting/removing rows,
    creating/dropping tables, etc. Does not return anything.
    """
    conn = pg.connect(**config)
    cur = conn.cursor()
    if values is None:
        cur.execute(query)
    else:
        cur.execute(query, values)
    conn.commit()
    cur.close()
    conn.close()
    return None

##############################################################################
##                        Music database manipulation                       ##
##############################################################################

def find_new_songs(music_dir, db_config):
    """
    Compares the existing list of file names in the database with the list of
    local files in your music directory and finds 'new' files that aren't in
    the database yet.
    """
    # Pull all the song file names from the database
    query = """
    select song_files.file_nm
    from song_files;
    """
    db_files = set(psql_to_df(query, db_config).file_nm.values)
    # Pull all the song file paths from the directory
    local_files = glob.glob(music_dir+"*.*")
    # Strip the music directory from the file names
    local_files = [re.sub(f"^{music_dir}(.*)", r"\1", s) for s in local_files]
    # Ignore all file extensions not in those specified
    local_files = [s for s in local_files if len(s)>=4]
    valid_ext = ['.mp3','.wav','.m4a']
    local_files = {s for s in local_files if s[-4:].lower() in valid_ext}
    # Get files that are in <local_files> but not <db_files>
    new_files = list(local_files - db_files)
    
    return new_files

def gen_new_song_config(config, music_dir, file_nm, song_nm, artist_nm, genre_nm=None, album_nm=None):
    """
    Generates a configuration representing all available data about a new song based on
    its human-readable characteristics. Assumes that the song is truly new to the database,
    but its artist, genre, etc may not be.
    """
    song_config = dict(
        song_nm=song_nm,
        file_nm=file_nm,
        artist_nm=artist_nm,
        genre_nm=genre_nm,
        album_nm=album_nm,
        new_artist=False,
        new_genre=False,
        new_album=False
    )
    # Create song_id
    # Figure out what largest song_id in db is and increment that
    query = "select max(song_id) song_id from songs"
    song_id = psql_to_df(query, config).song_id.iloc[0] + 1
    song_config['song_id'] = int(song_id)
    
    # Create or retrieve artist_id
    query = f"select artist_id from artists where lower(artist_nm) = '{artist_nm.lower()}'"
    artist_id = psql_to_df(query, config)
    if len(artist_id) == 0:    # Artist doesn't exist
        query = "select max(artist_id) artist_id from artists"
        artist_id = psql_to_df(query, config).artist_id.loc[0] + 1
        song_config['new_artist'] = True
    elif len(artist_id) == 1:    # Single artist match
        artist_id = artist_id.artist_id.iloc[0]
    else:
        raise IndexError(f"artist_nm {artist_nm} has more than one entry in the database!")
    song_config['artist_id'] = int(artist_id)
    
    # Create or retrieve genre_id (if applicable)
    if genre_nm is None:
        genre_id = None
    else:
        query = f"select genre_id from genres where lower(genre_nm) = '{genre_nm.lower()}'"
        genre_id = psql_to_df(query, config)
        if len(genre_id) == 0:    # Genre doesn't exist yet
            query = "select max(genre_id) genre_id from genres"
            genre_id = psql_to_df(query, config).genre_id.iloc[0] + 1
            song_config['new_genre'] = True
        elif len(genre_id) == 1:    # Single genre match
            genre_id = genre_id.genre_id.iloc[0]
        else:
            raise IndexError(f"genre_nm {genre_nm} has more than one entry in the database!")
    song_config['genre_id'] = int(genre_id)
    
    # Create or retrieve album_id (if applicable)
    if album_nm is None:
        album_id = None
    else:
        query = f"""
        select albums.album_id
        from albums, artists
        where
            albums.artist_id = artists.artist_id
            and lower(albums.album_nm) = '{album_nm.lower()}'
            and lower(artists.artist_nm) = '{artist_nm.lower()}'
        """
        album_id = psql_to_df(query, config)
        if len(album_id) == 0:    # Album doesn't exist
            query = "select max(album_id) album_id from albums"
            album_id = psql_to_df(query, config).album_id.iloc[0] + 1
            song_config['new_album'] = True
        elif len(album_id) == 1:    # Single album match
            album_id = album_id.album_id.iloc[0]
        else:
            raise IndexError(f"(album_nm, artist_nm) pair ({album_nm}, {artist_nm}) " \
                             "has more than one entry in the database!")
        song_config['album_id'] = int(album_id)
    # Get some file metadata
    try:
        metadata = get_song_metadata(file_nm, music_dir)
        for key in ['bitrate','beats_per_min','duration','file_size']:
            song_config[key] = metadata.get(key, None)
    except mutagen.mp3.HeaderNotFoundError:
        pass
    return song_config

def add_new_song_to_db(song_config, db_config):
    """
    Adds a new song to the database based on the song configuration file generated
    in an earlier step.
    """
    # Open db connections
    conn = pg.connect(**db_config)
    cur = conn.cursor()
    # Add genre to `genres` table (if new)
    if song_config['new_genre']:
        query = """
        insert into genres
            (genre_id, genre_nm)
        values (%s, %s);
        """
        values = (
            song_config['genre_id'],
            song_config['genre_nm'],
        )
        cur.execute(query, values)
    # Add artist to `artists` table (if new)
    if song_config['new_artist']:
        query = """
        insert into artists
            (artist_id, artist_nm)
        values (%s, %s);
        """
        values = (
            song_config['artist_id'],
            song_config['artist_nm']
        )
        cur.execute(query, values)
    # Add album to `albums` table (if new)
    if song_config['new_album']:
        query = """
        insert into albums
            (album_id, artist_id, album_nm)
        values (%s, %s, %s);
        """
        values = (song_config[s] for s in ['album_id','artist_id','album_nm'])
        values = (
            song_config['album_id'],
            song_config['artist_id'],
            song_config['album_nm']
        )
        cur.execute(query, values)
    # Add new song to `songs` table
    query = """
    insert into songs
        (song_id, song_nm, artist_id, album_id, genre_id)
    values (%s, %s, %s, %s, %s);
    """
    values = (
        song_config['song_id'],
        song_config['song_nm'],
        song_config['artist_id'],
        song_config['album_id'],
        song_config['genre_id']
    )
    cur.execute(query, values)
    # Add new song file to `song_files` table
    query = """
    insert into song_files
        (song_id, file_nm)
    values (%s, %s);
    """
    values = (
        song_config['song_id'],
        song_config['file_nm']
    )
    cur.execute(query, values)
    # Commit changes to database
    conn.commit()
    cur.close()
    conn.close()
    return song_config

def delete_song_from_db(song_id: int, db_config):
    # Open db connections
    conn = pg.connect(**db_config)
    cur = conn.cursor()
    # Delete from `song_files` table
    query = "delete from song_files where song_id = %s;"
    values = (song_id,)
    cur.execute(query, values)
    # Delete from playlist_songs table
    query = "delete from playlist_songs where song_id = %s;"
    cur.execute(query, values)
    # Delete from `song` table
    query = "delete from songs where song_id = %s;"
    cur.execute(query, values)
    conn.commit()
    cur.close()
    conn.close()
    return None

def change_song_name(song_id, new_song_nm, config):
    """ Changes a song's name in the database. """
    query = """
    update songs
    set song_nm = %s
    where song_id = %s;
    """
    values = (new_song_nm, song_id)
    psql_execute(query, config, values)
    return None

##############################################################################
##                       File interaction/manipulation                      ##
##############################################################################

def get_song_metadata(file_nm, music_dir):
    """
    Gets as much metadata from the song file as possible.
    """
    file = mutagen.File(music_dir+file_nm)
    if file is None:
        return dict()
    mdata = {str(k[:4]): v for k, v in file.items()}
    ID3_dict = {
        'picture': 'APIC',
        'comment': 'COMM',
        'play_counter': 'PCNT',
        'popularimeter': 'POPM',
        'album': 'TALB',
        'beats_per_min': 'TBPM',
        'composer': 'TCOM',
        'copyright': 'TCOP',
        'encoding_time': 'TDEN',
        'recording_date': 'TDRC',
        'file_type': 'TFLT',
        #'length': 'TLEN',
        'track': 'TRCK',
        'title': 'TIT2',
        'artist': 'TPE1',
    }
    mdata = {k: file.get(v, None) for k, v in ID3_dict.items()}
    mdata = {k: v.text[0] for k, v in mdata.items() if (v is not None)}
    mdata['duration'] = round(file.info.length)
    mdata['bitrate'] = int(file.info.bitrate)
    mdata['file_size'] = os.path.getsize(music_dir+file_nm)
    return mdata