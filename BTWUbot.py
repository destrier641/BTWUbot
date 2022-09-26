#!/usr/bin/env python3

import asyncio
import datetime
import discord
import json
import logging
import psycopg2
import re
import signal
import spotipy

from spotipy.oauth2 import SpotifyClientCredentials

################################################################################
# Setup
################################################################################

bot_prefix = "lp"
config_file = "./config.json"
with open(config_file, 'r') as config_fd:
    config = json.load(config_fd)

logging.basicConfig(filename = config["log_file"], format = '[%(asctime)s] %(levelname)s: %(message)s')

# Listening vars
logged_channel_ids = [
        917773144620670976, # BTUWU/listening-party-notifications
        917773757802749992, # BTUWU/bot-commands
        ]
logged_role_ids = [
        931328237441806356, # BTUWU@Listening Party
        ]

logged_channels = []
logged_roles = []

spotify_url_re_str = "[^\s]+open\.spotify\.com[^\s]+"
spotify_url_re = re.compile(spotify_url_re_str)

# Database vars
db_name = config["database"]["name"]
db_user = config["database"]["user"]
db_table = config["database"]["table"]
db_table_base_fields = config["database"]["table_fields"]

# Initialize
db_conn = psycopg2.connect(f"dbname={db_name} user={db_user}")

def sigint_handler(signum, frame):
    logging.info("Got SIGINT - cleaning...")
    db_conn.commit()
    db_conn.close()
    logging.info("Closed.")
signal.signal(signal.SIGINT, sigint_handler)

db_cursor = db_conn.cursor()
db_cursor.close()

spotify_auth_manager = SpotifyClientCredentials(
                        client_id = config["secrets"]["spotify"]["client-id"],
                        client_secret = config["secrets"]["spotify"]["client-secret"])
spotify_worker = spotipy.Spotify(auth_manager = spotify_auth_manager)

client_intents = discord.Intents.default()
client_intents.message_content = True
client = discord.Client(intents = client_intents)
client.run(config["secrets"]["discord"])

################################################################################
# Init
################################################################################

@client.event
async def on_guild_available(guild):
    logging.info(f"Available guild: {guild.name} [{guild.id}]")
    for chan_id in logged_channel_ids:
        check_chan = guild.get_channel_or_thread(chan_id)
        if check_chan is not None:
            logging.info(f"Found channel {check_chan.name} [{chan_id}] in guild {guild.name}.")
            logged_channels.append(check_chan)
    for role_id in logged_role_ids:
        check_role = guild.get_role(role_id)
        if check_role is not None:
            logging.info(f"Found role {check_role.name} [{role_id}] in guild {guild.name}.")
            logged_roles.append(check_role)
    if not logged_channels or not logged_roles:
        logging.error(f"Did not find any channels [{len(logged_channels)}] or roles [{len(logged_roles)}] to listen to!")
        exit(1)
    logging.info("Listening...")

################################################################################
# LP capturer
################################################################################

def validate_message(message):
    if not message.content.startswith(bot_prefix):
        return False
    if not any(role in message.mentions for role in logged_roles):
        return False
    if not message.channel in logged_channels:
        return False
    logging.info(f"Heard message {message.id} in channel {message.channel.name}.")
    return True

def insert_table_entry(db_fields, vals):
    db_cursor = db_conn.cursor()
    db_query = psycopg2.sql.SQL("INSERT INTO {table} ({fields}) VALUES ({vals})").format(
            table = psycopg2.sql.Identifier(db_table),
            fields = psycopg2.sql.SQL(',').join(db_fields),
            values = psycopg2.sql.SQL(',').join(len(db_fields) * ["%s"]))
    db_cursor.execute(db_query, vals)
    # db_cursor.execute(f"""
        # INSERT INTO sessions (id, date, issuer_id, issuer_name, issuer_nick, spotify_url)
        # VALUES (%(id)s, %(date)s, %(iss_id)s, %(iss_name)s, %(iss_nick)s, %(sp_url)s);
        # """,
        # ('id': db_next_id, 'date': date, 'iss_id': issuer,
                # 'iss_name': issuer_name, 'iss_nick': issuer_nick,
                # 'sp_url': spotify_url, 'sp_artist': spotify_artist,
                # 'sp_album': spotify_album))
    db_conn.commit()
    db_cursor.close()

@client.event
async def on_message(message):
    logging.info(f"Checking message id {message.id}")
    if not validate_message(message):
        return
    spotify_url = re.search(spotify_url_re, message.content)
    if not spotify_url:
        logging.info("Heard message has no Spotify string, ignoring.")
        return
    message_id = message.id
    issuer = message.author.id
    issuer_name = message.author.name
    issuer_nick = message.author.nick
    date = message.created_at

    db_fields = db_table_base_fields
    vals_to_insert = [ message_id, date, issuer, issuer_name, issuer_nick, spotify_url ]
    if "album" in spotify_url:
        result = spotify_worker.artist(spotify_url)
        spotify_album = result['name']
        spotify_artists = []
        for artist in result['artists']:
            spotify_artists.extend(artist['name'])
        vals_to_insert.extend([ spotify_artists, spotify_album ])
        db_fields.extend(config['database']['album_fields'])
    elif "playlist" in spotify_url:
        result = spotify_worker.playlist(spotify_url)
        spotify_pl_owner = result['owner']['display_name']
        spotify_pl_name = result['name']
        vals_to_insert.extend([ spotify_pl_owner, spotify_pl_name ])
        db_fields.extend(config['database']['playlist_fields'])
    else:
        logging.error(f"Unparseable URL: {spotify_url}")
        return

    if len(db_fields) != len(vals_to_insert):
        logging.error(f"Length mismatch: db_fields <{db_fields}> vs vals <{vals_to_insert}>")
        return
    insert_table_entry(db_fields, vals_to_insert)

    await message.channel.send(f"Logged URL <{spotify_url}>")

