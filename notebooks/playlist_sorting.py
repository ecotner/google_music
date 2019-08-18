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

# # Playlist sorting
# Google music mobile app doesn't have a way to sort by artist, so I'm just going to use the API to pull all the metadata for my playlists, sort it by artist alphabetically, then re-upload.

import gmusicapi
import time
import pandas as pd
import re
from sort_playlist import *

# Get songs

api = login()

library_df = get_all_songs(api)

library_df.head()

# !ls

# Look at logs to find missing stuff

with open('failed_song_upload2.log', 'r') as fo:
    missing_song_ids = fo.readlines()
    missing_song_ids = [s[:-1] for s in missing_song_ids]

missing_song_df = pd.DataFrame(missing_song_ids, columns=['song_id'])
missing_song_df = pd.merge(
    missing_song_df,
    library_df,
    on='song_id',
    how='left'
)

missing_song_df.head(20)

# Obviously looks like funk music. Add back into playlist...

playlist_json = api.get_all_playlists()
playlist_json = [e for e in playlist_json if e['name']=='Funk'][0]
pl_id = playlist_json['id']

api.add_songs_to_playlist(pl_id, missing_song_ids)


