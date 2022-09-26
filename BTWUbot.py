#!/usr/bin/env python3

import asyncio
import csv
import discord
import datetime
import logging
import psycopg2
import re
import signal

from pyscopg2 import sql

################################################################################
# Setup
################################################################################

log_file = "./btwu.log"
logging.config(filename = log_file)

secrets_file = "./secrets.json"
with open(secrets_file, 'r') as secrets_fd:
    secrets = json.load(secrets_fd)

# Listening vars
logged_channel_ids = [
        917773144620670976, # BTUWU/listening-party-notifications
        ]
logged_role_ids = [
        931328237441806356, # BTUWU@Listening Party
        ]


logged_channels = []
logged_roles = []

spotify_url_re_str = "[^\s]+open\.spotify\.com[^\s]+"
spotify_url_re = re.compile(spotify_url_re_str)

# Database vars
db_name = secrets["database"]["name"]
db_user = secrets["database"]["user"]
db_table = secrets["database"]["table"]
db_table_fields = secrets["database"]["table_fields"]

# Initialize
client = discord.Client()
db_conn = psycopg2.connect(f"dbname={db_name} user={db_user}")
signal.signal(signal.SIGINT, sigint_handler)

db_cursor = db_conn.cursor()
db_cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}".format(sql.Identifier(db_table))))
db_next_id = db_cursor.fetchone()
db_cursor.close()

################################################################################
# Init
################################################################################

@client.event
async def on_guild_available(guild):
    logging.info(f"Available guild: {guild.name} [{guild.id}]")
    to_log = []
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

def sigint_handler(signum, frame):
    logging.info("Got SIGINT - cleaning...")
    db_conn.commit()
    db_conn.close()
    logging.info("Closed.")

################################################################################
# LP capturer
################################################################################

def validate_message(message):
    if not any(role in message.mentions for role in logged_roles):
        return False
    if not message.channel in logged_channels:
        return False
    logging.info(f"Heard message {message.id} in channel {message.channel.name}.")
    return True

def insert_table_entry(vals):
    db_cursor = db_conn.cursor()
    db_query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({vals})").format(
            table = sql.Identifier(db_table),
            fields = sql.SQL(',').join(db_table_fields),
            values = sql.SQL(',').join(len(db_table_fields) * ["%s"]))
    db_cursor.execute(db_query, vals_to_insert)
    # db_cursor.execute(f"""
        # INSERT INTO sessions (id, date, issuer_id, issuer_name, issuer_nick, spotify_url)
        # VALUES (%(id)s, %(date)s, %(iss_id)s, %(iss_name)s, %(iss_nick)s, %(sp_url)s);
        # """,
        # ('id': db_next_id, 'date': date, 'iss_id': issuer,
                # 'iss_name': issuer_name, 'iss_nick': issuer_nick,
                # 'sp_url': spotify_url, 'sp_artist': spotify_artist,
                # 'sp_album': spotify_album))
    db_conn.commit()
    db_next_id += 1
    db_cursor.close()

@client.event
async def on_message(message):
    if not validate_message(message):
        return
    spotify_url = re.search(spotify_url_re, message.content)
    if not spotify_url:
        logging.info("Heard message has no Spotify string, ignoring.")
        return
    issuer = message.author.id
    issuer_name = message.author.name
    issuer_nick = message.author.nick
    date = message.created_at
    spotify_artist = "me"
    spotify_album = "you"

    vals_to_insert = [ db_next_id, date, issuer, issuer_name, issuer_nick, spotify_url ]
    assert(len(vals_to_insert) == len(db_table_fields))
    insert_table_entry(vals)


