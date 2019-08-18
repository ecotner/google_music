# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# Imports
import pandas as pd
import numpy as np

# # Creating a music database

# I'm tired of having to migrate my music from service to service, losing all my playlists in the process and having to rebuild them manually. All these music services also have weird proprietary structures that I can't access very easily. So I'm going to create a `PostgreSQL` database out of my music so that I can maintain it easily.

# ## Import music from `Rhythmbox`

# Right now, my music is saved in a folder on my laptop's hard drive, and my main music player recently has been `Rhythmbox`, the default music player on Ubuntu. Rhythmbox saves all of its configuration files in an xml format under the `~/.local/share/rhythmbox` directory. In this directory you'll find two files: `rhythmdb.xml` and `playlists.xml`, which hold the music database and the playlist database.

import xml.etree.ElementTree as ET
RB_DIR = "/home/ecotner/.local/share/rhythmbox/"
playlist_tree = ET.parse(RB_DIR+"playlists.xml")
db_tree = ET.parse(RB_DIR+"rhythmdb.xml")

# ### Music database

# The music database is composed of a list of entries for each song that contain the title, artist, album, file location, etc...

# Get the first entry in the xml tree and display all the data:
root = db_tree.getroot()
for child in root[0]:
    key = child.tag
    value = child.text
    print(f"{key}: {value}")

# There's also other data like "iradio" and "ignore" (album art?):

for child in root.find("./entry/[@type='iradio']"):
    key = child.tag
    value = child.text
    print(f"{key}: {value}")
print()
for child in root.find("./entry/[@type='ignore']"):
    key = child.tag
    value = child.text
    print(f"{key}: {value}")

# Get the first entry in the xml tree and display all the data:
root = db_tree.getroot()
for child in root[-1]:
    key = child.tag
    value = child.text
    print(f"{key}: {value}")

# ### Playlist database

# The playlist database contains a list of entries for each playlist:

root = playlist_tree.getroot()
for child in root[:10]:
    attrib = child.attrib
    name = attrib['name']
    print(f"name: {name}")

# Each one of the playlists contains a list of file locations of the songs belonging to that playlist:

node = playlist_tree.find("./playlist[@name='Hip Hop']")
for loc in node[:10]:
    key = loc.tag
    value = loc.text
    print(f"{key}: {value}")

# ### Import XML databases to `pandas` dataframes

# The very first thing we're gonna want to do before saving this data to a database is we're gonna want to format it in a way such that we can upload it to our database in a simple manner. To do this, we're going to create the table structure of the database in `pandas`.

# First, we're going to get all the info from the main `rhythmdb.xml` file, where each song entry will be a row in our dataframe. We'll iterate through the xml one song at a time:

rhythmdb_df = list()
for song in db_tree.findall("./entry[@type='song']"):
    row = {e.tag: e.text for e in song}
    rhythmdb_df.append(row)
rhythmdb_df = pd.DataFrame(rhythmdb_df)

rhythmdb_df.head()

# There's a lot of `NaN`'s in this dataframe; this is because some of the songs have attributes that others don't, like discography numbers or BPM. Here's the number of non-`NaN` elements in each column:

print("Dataframe shape:", rhythmdb_df.shape)
print("Number of non-NaN values per column:")
rhythmdb_df.count().sort_values(ascending=True)[:15]

# Comments on the songs?
rhythmdb_df[~rhythmdb_df.comment.isna()].comment.head()

# Are there any duplicates?
duplicates = rhythmdb_df.title.str.lower().value_counts()
duplicates = duplicates[duplicates > 1].index
cols = ['title','artist','album']
df = rhythmdb_df[rhythmdb_df.title.str.lower().isin(duplicates)][cols].sort_values(cols)
for col in df.columns:
    df[col] = df[col].str.lower()
df[df.duplicated()]

# Ok, let's do the same thing with the playlist database.

playlist_df = list()
for playlist in playlist_tree.getroot():
    if (len(list(playlist)) == 0) or (playlist[0].tag == 'conjunction'):
        continue
    attr = playlist.attrib
    for loc in playlist:
        row = attr.copy()
        text = loc.text
        row['location'] = text if text != r'\n      ' else None
        playlist_df.append(row)
playlist_df = pd.DataFrame(playlist_df)

playlist_df.head()

# Also need to cast values to their proper types:

cast_pairs = {
    'title': str,
    'genre': str,
    'artist': str,
    'album': str,
    'duration': int,
    'file-size': int,
    'location': str,
    'mountpoint': str,
    'mtime': int,
    'first-seen': int,
    'last-seen': int,
    'bitrate': int,
    'date': int,
    'media-type': str,
    'composer': str,
    'track-number': int,
    'comment': str,
    'album-artist': str,
    'play-count': int,
    'last-played': int,
    'track-total': int,
    'beats-per-minute': float,
    'track-total': int,
    'disc-number': int,
    'disc-total': int,
    'mb-artistsortname': str,
    'mb-trackid': str,
    'mb-artistid': str,
    'mb-albumid': str,
    'mb-albumartistid': str,
    'rating': int,
    'album-sortname': str
}
for k, v in cast_pairs.items():
    if v == int:
        rhythmdb_df.fillna(0, inplace=True)
    try:
        rhythmdb_df[k] = rhythmdb_df[k].values.astype(v)
    except:
        print(k, v)
        raise Exception

cast_pairs = {
    'name': str,
    'show-browser': str,
    'browser-position': int,
    'search-type': str,
    'type': str,
    'location': str
}
for k, v in cast_pairs.items():
    playlist_df[k] = playlist_df[k].values.astype(v)

# ## Building the database

# So now that we have our `rhythmdb` and `playlist` dataframes, we can start to construct our database. Here's all the data we have create it from:

rhythmdb_df.columns

playlist_df.columns

# I want to create a database that has all the necessary information to provide easy access to any of my music data, but abstracts away pieces that I don't need or want to think about to other tables. This is kind of an outline of what it should look like. I can't make every piece all at once, but I'll start with a couple of the tables that I can make.

# ![Music database ER diagram](music_db_erd.png)

# ### Make `genres` table
# We will start on the outside at some of the simpler tables and work our way in. First is the `genres` table. Some of the genre names are really stupid, or duplicates/mispellings of each other, but we can fix these errors later.

genres_df = pd.DataFrame()
genres_df['genre_nm'] = sorted(rhythmdb_df.genre.unique())
genres_df['genre_id'] = genres_df.index
genres_df = genres_df[['genre_id','genre_nm']]

genres_df.head()

# ### Make `artists` table

artists_df = pd.DataFrame()
artists_df['artist_nm'] = sorted(rhythmdb_df.artist.unique())
artists_df['artist_id'] = artists_df.index
artists_df = artists_df[['artist_id','artist_nm']]

artists_df.head()

# ### Make `albums` table
# This one will be a little trickier because it has a foreign key (`artist_id`) which connects to the `artists` table, so we have to tread carefully. Also, a lot of albums from different artists potentially have the same name (e.g. 'Greatest Hits').

albums_df = rhythmdb_df[['artist','album']].drop_duplicates()
albums_df['album_id'] = np.arange(len(albums_df))
albums_df = pd.merge(albums_df, artists_df, left_on='artist', right_on='artist_nm', how='left')
albums_df.rename(columns={'album':'album_nm'}, inplace=True)
albums_df = albums_df[['album_id','artist_id','album_nm']]

albums_df.head()

# Verify nothing was lost or duplicated in the process
print(rhythmdb_df[['album','artist']].drop_duplicates().shape[0])
print(albums_df.album_id.nunique())
print(len(albums_df))

# ### Make `songs` table
# This one is probably going to be the most complicated so far because it is going to have foreign keys for the `genres`, `artists` and `albums` tables.

songs_df = rhythmdb_df[['title','artist','album','genre']].copy()
cols = {
    'title': 'song_nm',
    'artist': 'artist_nm',
    'album': 'album_nm',
    'genre': 'genre_nm'
}
songs_df.rename(columns=cols, inplace=True)
df = pd.merge(albums_df, artists_df, on='artist_id', how='inner')
songs_df = pd.merge(songs_df, df, on=['album_nm','artist_nm'], how='left')
songs_df = pd.merge(songs_df, genres_df, on='genre_nm', how='left')
songs_df['song_id'] = np.arange(len(songs_df))
songs_df = songs_df[['song_id','song_nm','artist_id','album_id','genre_id']]

songs_df.head()

print(len(songs_df))
print(len(rhythmdb_df))

# ### Make `song_files` table
# This one will require a bit of regex in order to extract the proper file name. Some of the file names have Japanese/Korean/special characters in them, so `Rhythmbox` uses "percent encoding" to save the file URI in a standard format. This format can be decoded by using `urllib.parse.unquote` (and encoded again using `urllib.parse.quote`). Postgres doesn't seem to have a problem with these special encodings, so I'm just going to decode them.

import re
from urllib.parse import (quote, unquote)

song_files_df = rhythmdb_df.copy()
cols = {
    'title': 'song_nm',
    'artist': 'artist_nm',
    'album': 'album_nm'
}
song_files_df.rename(columns=cols, inplace=True)
df = pd.merge(songs_df, artists_df, on='artist_id', how='inner')
df = pd.merge(df, albums_df, on='album_id', how='inner')
song_files_df = pd.merge(song_files_df, df, on=list(cols.values()), how='inner')
song_files_df['file_nm'] = song_files_df.location.str.replace(r'^.*/Saved/', '').apply(unquote)
song_files_df.rename(columns={'beats-per-minute':'beats_per_min', 'file-size':'file_size'}, inplace=True)
cols = ['song_id','file_nm','bitrate','beats_per_min','duration','file_size']
song_files_df = song_files_df[cols]

song_files_df.head()

# Verify nothing lost/created
print(len(rhythmdb_df))
print(len(songs_df))
print(len(song_files_df))

# ### Test out playing some songs from file
# Now that the `song_files` table is set up, we can look up songs and their associated files, then play them within the jupyter notebook using `IPython.display.Audio`.

from IPython.display import Audio

songs_df[songs_df.song_nm.str.lower().str.contains(r"don't know")]

print(song_files_df[song_files_df.song_id==3226].iloc[0].file_nm)

MUSIC_DIR = "/media/ecotner/HDD/Users/27182_000/Music/Saved/"
filename = MUSIC_DIR + song_files_df[song_files_df.song_id==3226].file_nm.iloc[0]
Audio(filename=filename, autoplay=False)

# ### Make `playlist_songs` and `playlists` table

# +
playlist_songs_df = playlist_df.copy()
playlist_songs_df['file_nm'] = playlist_songs_df.location.str.replace(r"^.*/Saved/", "").apply(unquote)
playlist_songs_df = pd.merge(playlist_songs_df, song_files_df, on='file_nm', how='left')
playlist_songs_df = pd.merge(playlist_songs_df, songs_df, on='song_id', how='left')
playlists_table_df = pd.DataFrame(sorted(playlist_songs_df.name.unique()), columns=['playlist_nm'])
playlists_table_df['playlist_id'] = playlists_table_df.index
playlist_songs_df = pd.merge(playlist_songs_df, playlists_table_df, left_on='name', right_on='playlist_nm', how='left')
playlist_songs_df = pd.merge(playlist_songs_df, artists_df, on='artist_id', how='left')
#playlist_songs_df = playlist_songs_df[['playlist_id','song_id']]
playlists_table_df = playlists_table_df[['playlist_id','playlist_nm']]

df = list()
for _, group in playlist_songs_df.groupby('playlist_id'):
    group = group.sort_values(by='artist_nm')
    group['playlist_order'] = np.arange(len(group))
    df.append(group)
playlist_songs_df = pd.concat(df, axis=0)
del df
playlist_songs_df = playlist_songs_df[['playlist_id','song_id','playlist_order']]
# -

playlist_songs_df.head()

playlists_table_df.head()

# Verify numbers check out
print(len(playlist_df))
print(len(playlist_songs_df))
print('-------')
print(playlist_df.name.nunique())
print(len(playlists_table_df))

# ## Upload to `PostgreSQL`
# Now that we have most of the primary tables in the database set up, we're going to actually create the database and populate the tables with data. We'll use the `psycopg2` package for interfacing with postgres, and all the login credentials (database name, host, password, etc.) are stored in a JSON configuration file.

import psycopg2 as pg
import json

# !ls ..

with open("../config.json", 'r') as fo:
    config = json.load(fo)
config = config['databases']['music']


# Convenience function for opening connection and passing arbitrary query
def psql_execute(query, config, values=None):
    conn = pg.connect(**config)
    cur = conn.cursor()
    if values is None:
        cur.execute(query)
    else:
        cur.execute(query, values)
    conn.commit()
    cur.close()
    conn.close()


# Ok, so let's set up the tables one by one. First, `genres`:

# Open the connection
conn = pg.connect(**config)
cur = conn.cursor()

# Drop table if it already exists
cur.execute("drop table if exists genres")
# Create the table
query = """
create table genres(
    genre_id integer unique not null,
    genre_nm varchar,
    primary key (genre_id)
);
"""
cur.execute(query)
conn.commit()

# Add rows
for _, row in genres_df.iterrows():
    query = """
    insert into genres
        (genre_id, genre_nm)
    values
        (%s, %s)
    """
    values = (
        row.genre_id,
        row.genre_nm,
    )
    cur.execute(query, values)
conn.commit()

# Then, `artists`:

cur.execute("drop table if exists artists;")
query = """
create table artists(
    artist_id integer unique not null,
    artist_nm varchar not null,
    primary key (artist_id)
);
"""
cur.execute(query)
conn.commit()

# Write data to table
for _, row in artists_df.iterrows():
    query = """
    insert into artists
        (artist_id, artist_nm)
    values
        (%s, %s);
    """
    values = (
        row.artist_id,
        row.artist_nm,
    )
    cur.execute(query, values)
conn.commit()

# Then, `albums`:

cur.execute("drop table if exists albums;")
query = """
create table albums(
    album_id integer unique not null,
    artist_id integer not null,
    album_nm varchar not null,
    primary key (album_id),
    foreign key (artist_id) references artists (artist_id)
);
"""
cur.execute(query)
conn.commit()

# Add data
for _, row in albums_df.iterrows():
    query = """
    insert into albums
        (album_id, artist_id, album_nm)
    values (%s, %s, %s);
    """
    values = (
        row.album_id,
        row.artist_id,
        row.album_nm,
    )
    cur.execute(query, values)
conn.commit()

# Then, `songs`:

# Drop table if it already exists
cur.execute("drop table if exists songs;")
# Create the table
query = """
create table songs(
    song_id int unique not null,
    song_nm varchar not null,
    artist_id int not null,
    album_id int,
    genre_id int,
    primary key (song_id),
    foreign key (artist_id) references artists (artist_id),
    foreign key (album_id) references albums (album_id),
    foreign key (genre_id) references genres (genre_id)
);
"""
cur.execute(query)
conn.commit()

# Add rows
for _, row in songs_df.iterrows():
    query = """
    insert into songs
        (song_id, song_nm, artist_id, album_id, genre_id)
    values
        (%s, %s, %s, %s, %s)
    """
    values = (
        row.song_id,
        row.song_nm,
        row.artist_id,
        row.album_id,
        row.genre_id
    )
    cur.execute(query, values)
conn.commit()

# Now, `playlists`:

cur.execute("drop table if exists playlists")
query = """
create table playlists(
    playlist_id integer unique not null,
    playlist_nm varchar not null,
    primary key (playlist_id)
);
"""
cur.execute(query)
conn.commit()

for _, row in playlists_table_df.iterrows():
    query = """
    insert into playlists
        (playlist_id, playlist_nm)
    values
        (%s, %s);
    """
    values = (
        row.playlist_id,
        row.playlist_nm,
    )
    cur.execute(query, values)
conn.commit()

# Then `playlist_songs`:

cur.execute("drop table if exists playlist_songs")
query = """
create table playlist_songs(
    playlist_id integer not null,
    song_id integer not null,
    playlist_order integer,
    foreign key (playlist_id) references playlists (playlist_id),
    foreign key (song_id) references songs (song_id)
);
"""
cur.execute(query)
conn.commit()

for _, row in playlist_songs_df.iterrows():
    query = """
    insert into playlist_songs
        (playlist_id, song_id, playlist_order)
    values
        (%s, %s, %s);
    """
    values = (
        row.playlist_id,
        row.song_id,
        row.playlist_order,
    )
    values = tuple(map(int, values))
    cur.execute(query, values)
conn.commit()

# And then finally... `song_files`

song_files_df.info()

conn = pg.connect(**config)
cur = conn.cursor()

cur.execute("drop table if exists song_files")
query = """
create table song_files(
    song_id integer not null,
    file_nm varchar not null,
    bitrate integer,
    beats_per_min float,
    duration integer,
    file_size integer,
    foreign key (song_id) references songs (song_id)
);
"""
cur.execute(query)
conn.commit()

for _, row in song_files_df.iterrows():
    query = """
    insert into song_files
        (song_id, file_nm, bitrate, beats_per_min, duration, file_size)
    values
        (%s, %s, %s, %s, %s, %s);
    """
    values = (
        row.song_id,
        row.file_nm,
        row.bitrate,
        row.beats_per_min,
        row.duration,
        row.file_size,
    )
    cur.execute(query, values)
conn.commit()

# Done!
cur.close()
conn.close()


# ## Test out the database
# Now that we have the database built, let's try it out!

# Define convenience function for reading data from database
def psql_to_df(query, config):
    conn = pg.connect(**config)
    cur = conn.cursor()
    cur.execute(query)
    columns = [col.name for col in cur.description]
    data = cur.fetchall()
    cur.close()
    conn.close()
    df = pd.DataFrame(data, columns=columns)
    return df


# Get all Beatles albums
query = """
select
    distinct albums.album_nm
from albums, artists
where 1=1
    and albums.artist_id = artists.artist_id
    and artists.artist_nm = 'The Beatles'
"""
df = psql_to_df(query, config)

df.album_nm.values

# Get all the artists who have an album called "Greatest Hits",
# and all the songs on those albums
query = """
select
    artists.artist_nm
    ,albums.album_nm
    ,songs.song_nm
from artists, albums, songs
where 1=1
    and artists.artist_id = albums.artist_id
    and songs.album_id = albums.album_id
    and lower(albums.album_nm) = 'greatest hits'
"""
df = psql_to_df(query, config)

df

# Get all the playlists that have ACDC songs in them
query = """
select
    playlists.playlist_nm,
    songs.song_nm
from playlists, playlist_songs, songs, artists
where 1=1
    and playlists.playlist_id = playlist_songs.playlist_id
    and playlist_songs.song_id = songs.song_id
    and songs.artist_id = artists.artist_id
    and artists.artist_nm = 'ACDC'
order by playlists.playlist_nm
"""
df = psql_to_df(query, config)

print(f"Number of playlists found: {df.playlist_nm.nunique()}")
print(*df.playlist_nm.unique(), sep=', ')
print(f"Number of songs found: {df.song_nm.shape[0]}")
print(f"Number of DISTINCT songs found: {df.song_nm.nunique()}")
df.song_nm.value_counts()

# Play the song "Dream On" by Aerosmith
query = """
select song_files.file_nm
from song_files, songs, artists
where 1=1
    and song_files.song_id = songs.song_id
    and songs.artist_id = artists.artist_id
    and lower(songs.song_nm) = 'dream on'
    and artists.artist_nm = 'Aerosmith'
"""
df = psql_to_df(query, config)

from IPython.display import Audio
MUSIC_DIR = r"/media/ecotner/HDD/Users/27182_000/Music/Saved/"
filename = MUSIC_DIR + df.file_nm.iloc[0]
Audio(filename=filename)

# Play my playlist "Favorites"
query = """
select
    playlist_songs.playlist_order
    ,songs.song_nm
    ,artists.artist_nm
    ,song_files.file_nm
from playlists, song_files, playlist_songs, songs, artists
where 1=1
    and playlists.playlist_id = playlist_songs.playlist_id
    and playlist_songs.song_id = song_files.song_id
    and playlist_songs.song_id = songs.song_id
    and songs.artist_id = artists.artist_id
    and playlists.playlist_nm = 'Favorites'
"""
df = psql_to_df(query, config)

#i = 0
i += 1
filename = MUSIC_DIR + df.file_nm.iloc[i]
Audio(filename=filename, autoplay=True)

df.iloc[i]


