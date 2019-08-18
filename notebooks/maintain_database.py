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

import pandas as pd
import numpy as np
import psycopg2 as pg
import json
import os
import sys
sys.path.insert(0, '../source')
import glob
import re
import mutagen
from IPython.display import Audio
from util import (
    psql_execute,
    psql_to_df,
    find_new_songs,
    gen_new_song_config,
    add_new_song_to_db,
    delete_song_from_db,
    change_song_name,
    get_song_metadata,
)
MUSIC_DIR = r"/media/ecotner/HDD/Users/27182_000/Music/Saved/"

# # Music database maintenance
# Assuming the database has been set up properly, we will need to maintain it. This means adding/removing/editing songs/playlists/artists/etc in a self-consistent way that maintains database integrity.

with open('../config.json', 'r') as fp:
    config = json.load(fp)
    config = config['databases']['music']


# ## Adding (new) songs
# We don't expect that our music tastes will remain static! We'll need a way to add music to our database. To do this, first we'll need to search for music files that are not in our database. To do that, we'll need to search our music directory for file names that aren't in the database. I'll also make a fake music file to test it out with.

# Make fake music file
def make_temp_fake(func):
    with open(MUSIC_DIR+'fake_music.mp3', 'w+') as fp:
        fp.write('THIS IS A FAKE MUSIC FILE! DELETE IT!')
    # Do something
    r = func()
    # Delete fake music file
    os.remove(MUSIC_DIR+'fake_music.mp3')
    return r


f = lambda : print(find_new_songs(MUSIC_DIR, config))
make_temp_fake(f)

# Then, we need some way to add this music to our database. At the bare minimum, the file will need a `song_nm` and `artist_nm` (and the predetermined `file_nm`). We might also want to provide a `genre_nm` or `album_nm`, but this will be optional.

f = lambda : gen_new_song_config(
    config=config,
    music_dir=MUSIC_DIR,
    file_nm='fake_music.mp3',
    song_nm='Fake Music',
    artist_nm='Aerosmith',
    genre_nm='Rock',
    album_nm='Toys in the attic',
)
fake_song_config = make_temp_fake(f)
fake_song_config

# Add the fake song from above to the database
add_new_song_to_db(fake_song_config, config)

# Confirm that it's actually in there
query = f"""
select *
from songs, artists, albums, genres, song_files
where 1=1
    and songs.artist_id = artists.artist_id
    and songs.album_id = albums.album_id
    and songs.genre_id = genres.genre_id
    and songs.song_id = song_files.song_id
    and songs.song_id = {fake_song_config['song_id']}
"""
psql_to_df(query, config)

# ## Deleting music
# Now that we can add music to our database, we might want to delete it too. For example, we just added a fake Aerosmith song above. Let's delete it. In order to avoid the confusion associated with songs with the same name, we will delete by `song_id`.

delete_song_from_db(3548, config)

# Confirm that it's actually gone
query = f"""
select *
from songs, artists, albums, genres, song_files
where 1=1
    and songs.artist_id = artists.artist_id
    and songs.album_id = albums.album_id
    and songs.genre_id = genres.genre_id
    and songs.song_id = song_files.song_id
    and songs.song_id = {fake_song_config['song_id']}
"""
psql_to_df(query, config)

# ## Modify song properties
# When adding songs to the database, maybe something was added with incorrect specifications. Instead of deleting and uploading the song again, it might be easier to simply modify some of its properties, like name, artist, etc.

# Re-upload that fake song from before
add_new_song_to_db(fake_song_config, config);

change_song_name(fake_song_config['song_id'], "Poop Music", config)

# Confirm that it's actually different
query = f"""
select *
from songs
where 1=1
    and songs.song_id = {fake_song_config['song_id']}
"""
psql_to_df(query, config)

delete_song_from_db(fake_song_config['song_id'], config)

# We might also want to get file metadata so that we can change it later to keep things updated.

file_nm = 'The Glitch Mob - Drive It Like You Stole It.mp3'
metadata = get_song_metadata(file_nm, MUSIC_DIR)

metadata


