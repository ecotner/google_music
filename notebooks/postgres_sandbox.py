# -*- coding: utf-8 -*-
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

import psycopg2 as pg
import json
import pandas as pd
import sys
import sqlalchemy as sqa
sys.path.insert(0, '../source')
from util import psql_to_df

# # Fucking around with PostgreSQL and `psycopg2`

with open('../config.json', 'r') as fo:
    config = json.load(fo)
mcfg = config['databases']['music']

query = "select * from test"
df = psql_to_df(query, mcfg)

df

df['huh'].iloc[3]

conn = pg.connect(**mcfg)
cur = conn.cursor()

psql_cmd = """
insert into test
(text)
values (%s)
"""
values = (
    "☆ＳＥＩＮＷＡＶＥ☆２０００☆",
)
cur.execute(psql_cmd, values)

conn.commit()

conn.rollback()

import glob
import re
MUSIC_DIR = r"/media/ecotner/HDD/Users/27182_000/Music/Saved/"

glob.glob(MUSIC_DIR+'*.wav')


