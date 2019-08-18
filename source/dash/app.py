import sys
sys.path.insert(0, '..')
import json
import pandas as pd
import flask
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from urllib.parse import quote, unquote
from util import (
    psql_to_df,
)
MUSIC_DIR = '/media/ecotner/HDD/Users/27182_000/Music/Saved/'

def get_song_data(dbconfig, limit=10):
    # query data from postgres
    query = f"""
    select
        songs.song_nm
        ,artists.artist_nm
        ,albums.album_nm
        ,genres.genre_nm
        ,song_files.file_nm
    from songs
        ,artists
        ,albums
        ,genres
        ,song_files
        ,playlists
        ,playlist_songs
    where 1=1
        -- Filters
        and playlists.playlist_nm = 'Favorites'
        -- Join tables
        and playlists.playlist_id = playlist_songs.playlist_id
        and playlist_songs.song_id = songs.song_id
        and songs.artist_id = artists.artist_id
        and songs.album_id = albums.album_id
        and songs.genre_id = genres.genre_id
        and songs.song_id = song_files.song_id
    order by artists.artist_nm asc
    limit {limit}
    """
    df = psql_to_df(query, dbconfig)
    return df

def generate_table(df, max_rows=10):
    cols = df.columns.tolist()
    tab = html.Table(children=
        # Header
        [html.Tr([html.Th(col) for col in cols])] +

        # Body
        [html.Tr([
            html.Td(df.iloc[i][col]) for col in cols
        ]) for i in range(min(len(df), max_rows))]
    )
    return tab

def generate_audio_table(df, max_rows=10):
    cols = df.columns.tolist()
    cols.remove('file_nm')
    # Header
    header = html.Tr([html.Th(col) for col in cols]+[html.Th('player')])

    # Body
    body = list()
    for i in range(min(max_rows, len(df))):
        row = [html.Td(df.iloc[i][col]) for col in cols]
        row.append(html.Td(
            html.Audio(
                autoPlay=False,
                loop=False,
                preload='none',
                controls=True,
                src='/music/'+quote(df.iloc[i]['file_nm'])
            )
        ))
        body.append(html.Tr(row))
    table = html.Table([header]+body)
    return table


def generate_data_table(df, max_rows=10):
    tab = dash_table.DataTable(
        id='table',
        columns=[{'name': i, 'id': i} for i in df.columns],
        data=df.iloc[:min(max_rows, len(df))].to_dict('records'),
        css=[{
            'selector': '.dash-cell div.dash-cell-value',
            'rule': 'display: inline; white-space: inherit; overflow'
        }],
        style_header={'backgroundColor': 'rgb(30,30,30)'},
        style_cell={
            'backgroundColor': 'rgb(50,50,50)',
            'color': 'white',
            'whiteSpace': 'no-wrap',
            'overflow': 'hidden',
            'textOverflow': 'ellipsis',
            'maxWidth': 0
        }
    )
    return tab

def build_app(df, limit=10):
    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
    app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
    server = app.server
    
    @server.route('/music/<file_nm>')
    def serve_music(file_nm):
        return flask.send_from_directory(
            directory=MUSIC_DIR,
            filename=file_nm
        )

    app.layout = html.Div(children=[
        html.H1('Songs', style={'textAlign': 'center'}),
        generate_audio_table(df, limit)
    ])
    return app

if __name__ == '__main__':
    # Get database credentials
    with open('../../config.json', 'r') as fp:
        config = json.load(fp)
    config = config['databases']['music']

    # Query the song data
    limit = 50
    df = get_song_data(config, limit)

    # Build app
    app = build_app(df, limit=limit)
    app.run_server(debug=True)

