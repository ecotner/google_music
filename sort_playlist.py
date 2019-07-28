import gmusicapi
import pandas as pd
from time import sleep


def login():
    api = gmusicapi.clients.Mobileclient()
    api.oauth_login(api.FROM_MAC_ADDRESS, 'oauth_token')
    return api

def _get_all_playlist_ids(api):
    """
    Gets all playlists from google music account. Only has playlist info
    and a unique song_id; have to merge with library to get song/artist
    names.
    """
    all_playlists = api.get_all_user_playlist_contents()
    all_playlists_df = list()
    for pl in all_playlists:
        pl_nm = pl['name']
        pl_id = pl['id']
        for track in pl['tracks']:
            row = [
                pl_nm,
                pl_id,
                track.get('trackId', None),
                track.get('id', None)
            ]
            all_playlists_df.append(row)
    col_names = ['playlist_nm','playlist_id','song_id','pl_entry_id']
    all_playlists_df = pd.DataFrame(all_playlists_df, columns=col_names)
    return all_playlists_df

def get_all_songs(api):
    """
    Gets all songs in the library, including all song/artist/album names.
    """
    library = api.get_all_songs()
    library_df = list()
    for track in library:
        row = [track[k] for k in ['id','title','artist','album']]
        library_df.append(row)
    col_names = ['song_id','title','artist','album']
    library_df = pd.DataFrame(library_df, columns=col_names)
    return library_df

def get_all_playlists(api):
    """
    Gets all playlists, the music library, then merges the two to get
    complete view of what's in the playlists.
    """
    playlists_df = _get_all_playlist_ids(api)
    library_df = get_all_songs(api)
    playlists_df = pd.merge(
        playlists_df,
        library_df,
        on='song_id',
        how='left'
    )
    return playlists_df

def sort_format(s):
    """ Converts names to appropriate sorting format """
    s = s.str.lower().copy()
    s = s.str.replace(r'^the ', '')
    return s

if __name__ == "__main__":
    api = login()
    print('Loading playlist data...')
    playlists_df = get_all_playlists(api)
    for pl_nm in sorted(playlists_df.playlist_nm.unique()):
        print(f'Sorting and uploading playlist {pl_nm}...')
        mask = (playlists_df.playlist_nm == pl_nm)
        # Sort playlist by artist
        pl_df = playlists_df[mask]
        pl_df['sort_key'] = sort_format(pl_df.artist).values
        pl_df = pl_df.sort_values(by='sort_key')
        pl_id = pl_df.playlist_id.iloc[0]
        # Delete all entries from playlist
        pl_entry_ids = pl_df.pl_entry_id.values.tolist()
        api.remove_entries_from_playlist(pl_entry_ids)
        sleep(1)
        # Add songs back into playlist
        song_ids = pl_df.song_id.values.tolist()
        try:
            api.add_songs_to_playlist(pl_id, song_ids)
            sleep(1)
        except:
            with open('failed_song_upload.log', 'w+') as fo:
                print(f"Playlist name: {pl_nm}")
                print(f"Playlist ID: {pl_id}")
                print("Song titles:")
                song_nms = pl_df.title.values.tolist()
                for nm in song_nms:
                    print(nm, file=fo)
                print("Song IDs:")
                for sid in song_ids:
                    print(sid, file=fo)
    print('Done!')








