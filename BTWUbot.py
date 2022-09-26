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
import sys

from spotipy.oauth2 import SpotifyClientCredentials
from logging import handlers
from psycopg2 import sql

################################################################################
# Setup
################################################################################

bot_prefix = "lp"
bot_undo_command = "undo"
config_file = "./config.json"
with open(config_file, 'r') as config_fd:
    config = json.load(config_fd)

logging.basicConfig(
        format = '[%(asctime)s] L%(lineno)s - %(levelname)s: %(message)s',
        level = logging.INFO,
        handlers = [
            handlers.RotatingFileHandler(
                filename = config["log_file"], mode = 'w',
                maxBytes = int(config["log_max_size"]), backupCount = 1),
            logging.StreamHandler(stream = sys.stdout)
            ]
        )

# Listening vars
logged_channel_ids = [
         917773144620670976, # BTUWU/listening-party-notifications
        1022926581133488139, # BTUWU/bot-testing
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

################################################################################
# Init
################################################################################

class BTWUClient(discord.Client):
    def __init__(self, intents):
        undo_available = -1
        discord.Client.__init__(self, intents = intents)

    async def on_guild_available(self, guild):
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

    def validate_message(self, message):
        if message.author == client.user:
            logging.info("Bot message")
            return
        if not message.content.startswith(bot_prefix):
            logging.info("No prefix")
            return False
        # if not any(role in message.mentions for role in logged_roles):
            # return False
        if not message.channel in logged_channels:
            logging.info(f"Wrong channel: {message.channel}")
            return False
        logging.info(f"Heard message {message.id} in channel {message.channel.name}.")
        return True

    def do_undo(self):
        if self.undo_available == -1:
            logging.warning("Undo not available.")
            return
        db_cursor = db_conn.cursor()
        db_query = sql.SQL("DELETE FROM {table} WHERE {key_col} = %s").format(
                table = sql.Identifier(db_table),
                key_col = sql.Identifier(db_table_base_fields[0]),
                last_val = sql.Identifier(self.undo_available))
        db_cursor.execute(db_query, [self.undo_available])
        db_conn.commit()
        db_cursor.close()
        self.undo_available = -1

    def insert_table_entry(self, db_fields, vals):
        db_cursor = db_conn.cursor()
        db_query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values})").format(
                table = sql.Identifier(db_table),
                fields = sql.SQL(',').join(map(sql.Identifier, db_fields)),
                values = sql.SQL(',').join(sql.Placeholder() * len(db_fields)))
        logging.info(f"Executing query: {db_query.as_string(db_conn)}")
        db_cursor.execute(db_query, vals)
        db_conn.commit()
        db_cursor.close()

    async def on_message(self, message):
        logging.info(f"Checking message id {message.id}")
        if not self.validate_message(message):
            logging.info(f"Invalid message id {message.id}")
            return
        spotify_url = re.search(spotify_url_re, message.content)
        if not spotify_url:
            logging.info("heard message has no spotify string, ignoring.")
            return
        spotify_url = spotify_url.group()
        message_id = message.id
        issuer = message.author.id
        issuer_name = message.author.name
        issuer_nick = message.author.nick
        date = message.created_at

        vals_to_insert = [ message_id, date, issuer, issuer_name, issuer_nick, spotify_url ]
        if "album" in spotify_url:
            result = spotify_worker.album(spotify_url)
            spotify_album = result['name']
            spotify_artists = []
            for artist in result['artists']:
                spotify_artists.append(artist['name'])
            vals_to_insert.extend([ spotify_artists, spotify_album ])
            db_fields = db_table_base_fields + config['database']['album_fields']
        elif "playlist" in spotify_url:
            result = spotify_worker.playlist(spotify_url)
            spotify_pl_owner = result['owner']['display_name']
            spotify_pl_name = result['name']
            vals_to_insert.extend([ spotify_pl_owner, spotify_pl_name ])
            db_fields = db_table_base_fields + config['database']['playlist_fields']
        else:
            logging.error(f"unparseable url: {spotify_url}")
            return

        print(db_table_base_fields)
        if len(db_fields) != len(vals_to_insert):
            logging.error(f"length mismatch: db_fields <{db_fields}> vs vals <{vals_to_insert}>")
            return
        self.insert_table_entry(db_fields, vals_to_insert)
        self.undo_available = message_id

        await message.channel.send(f"Logged URL <{spotify_url}>")

################################################################################
# LP capturer
################################################################################

# Initialize
db_conn = psycopg2.connect(f"dbname={db_name} user={db_user}")

def sigint_handler(signum, frame):
    logging.info("Got SIGINT - cleaning...")
    db_conn.commit()
    db_conn.close()
    logging.info("Closed.")
    exit(0)
signal.signal(signal.SIGINT, sigint_handler)

db_cursor = db_conn.cursor()
db_cursor.close()

spotify_auth_manager = SpotifyClientCredentials(
                        client_id = config["secrets"]["spotify"]["client-id"],
                        client_secret = config["secrets"]["spotify"]["client-secret"])
spotify_worker = spotipy.Spotify(auth_manager = spotify_auth_manager)

client_intents = discord.Intents.default()
client_intents.message_content = True
client = BTWUClient(intents = client_intents)
client.run(config["secrets"]["discord"])
