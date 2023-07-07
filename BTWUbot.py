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
        level = logging.DEBUG,
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

class BTWUMessage():
    def __init__(self, message):
        self.discord_message = message
        self.is_bot_cmd = message.content.startswith(bot_prefix)
        if self.is_bot_cmd:
            bot_cmd_msg = message.content.split(" ")
            self.bot_cmd = bot_cmd_msg[1]
            self.bot_cmd_args = bot_cmd_msg[2:]
        else:
            self.bot_cmd = None
            self.bot_cmd_args = None

    def validate(self):
        if self.discord_message.author == client.user:
            logging.info("Bot message")
            return False
        elif not (any(role in self.discord_message.role_mentions for role in logged_roles) or self.discord_message.content.startswith(bot_prefix)):
            logging.info(self.discord_message.role_mentions)
            logging.info(logged_roles)
            logging.info("No mention, nor prefix")
            return False
        elif not self.discord_message.channel in logged_channels:
            logging.info(f"Wrong channel: {self.discord_message.channel}")
            return False

        logging.info(f"Heard message {self.discord_message.id} in channel {self.discord_message.channel.name}.")
        return True

class BTWUClient(discord.Client):
    def __init__(self, intents):
        self.disc_guild = None
        self.curr_message = None

        self.undo_available = -1
        self.give_reply = True
        self.update_limit = 2
        # <command_name>: (<min_args>, <function>)
        self.command_map = {
                "update": (0, self.do_update),
                "undo": (0, self.do_undo),
                "set-limit": (1, self.do_set_update_limit),
                "stats": (2, self.do_stats),
                }

        discord.Client.__init__(self, intents = intents)

    async def on_guild_available(self, guild):
        if self.disc_guild:
            logging.error(f"Found guild {self.disc_guild.name} when connecting to guild {guild.name}")
            return
        self.disc_guild = guild
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

    async def validate_command(self, bot_message):
        command = bot_message.bot_cmd
        command_args = bot_message.bot_cmd_args
        if not command in self.command_map:
            logging.warning(f"Command {command} not supported")
            return
        command_info = self.command_map[command]
        if len(command_args) < command_info[0]:
            logging.warning(f"Expected {command_info[0]} arguments, got {len(command_args)}")
            return
        await command_info[1](command_args)

    async def do_update(self, args):
        self.give_reply = False
        logging.info(f"Logging {self.update_limit} messages")
        for channel in logged_channel_ids:
            channel = self.disc_guild.get_channel(channel)
            logging.debug(f"START {channel.id}")
            async for message in channel.history(limit = self.update_limit):
                logging.debug(f"START {message.id}")
                logging.info(f"Updating message {message.id}")
                history_message = BTWUMessage(message)
                if history_message.validate() and not history_message.is_bot_cmd:
                    await self.do_parse_new_lp(message)
                logging.debug(f"END {message.id}")
            logging.debug(f"END {channel.id}")
        self.give_reply = True

    async def do_set_update_limit(self, args):
        if not args :
            logging.warning("Missing argument for `set_update_limit`.")
            return
        try:
            new_limit = int(args[0])
        except ValueError:
            logging.warning(f"Invalid argument from `set_update_limit`: {args[0]}")
            return

        logging.info(f"Setting update history limit from {self.update_limit} to {new_limit}")
        self.update_limit = new_limit

    async def do_undo(self, args):
        if self.undo_available == -1:
            logging.warning("Undo not available.")
            return
        db_cursor = db_conn.cursor()
        db_query = sql.SQL("DELETE FROM {table} WHERE {key_col} = %s").format(
                table = sql.Identifier(db_table),
                key_col = sql.Identifier(db_table_base_fields[0]))
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

    async def do_parse_new_lp(self, message):
        spotify_url = re.search(spotify_url_re, message.content)
        if not spotify_url:
            logging.info("heard message has no spotify string, ignoring.")
            return

        db_cursor = db_conn.cursor()
        db_query = sql.SQL("SELECT * FROM {table} WHERE {key_field} = %s").format(
                table = sql.Identifier(db_table),
                key_field = sql.Identifier(db_table_base_fields[0]))
        db_cursor.execute(db_query, [message.id])
        result = db_cursor.fetchone()
        db_cursor.close()
        if result:
            logging.info(f"Existing record for message id {message.id}")
            return False

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
            spotify_artists = set()
            for track in result['tracks']['items']:
                print(track)
                for artist in track['track']['artists']:
                    spotify_artists.add(artist['name'])
            spotify_artists = list(spotify_artists)
            vals_to_insert.extend([ spotify_artists, spotify_pl_owner, spotify_pl_name ])
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

        if self.give_reply:
            await message.channel.send(content = f"Logged URL <{spotify_url}>")

    async def on_message(self, message):
        logging.info(f"Checking message id {message.id}")
        new_message = BTWUMessage(message)
        if not new_message.validate():
            logging.info(f"Invalid message id {message.id}")
            return
        self.curr_message = new_message
        if new_message.is_bot_cmd:
            await self.validate_command(new_message)
        else:
            await self.do_parse_new_lp(message)
        self.curr_message = None

    async def do_stats(self, args):
        db_cursor = db_conn.cursor()
        reply_a = discord.Embed(title = "BTWU Stat Manager o7", colour = discord.Colour.dark_green())
        reply_b = discord.Embed(title = "BTWU Stat Manager o7", colour = discord.Colour.dark_green())
        try:
            if args[0] == "issuer":
                issuer = self.disc_guild.get_member_named(args[1])
                if not issuer:
                    logging.error(f"Invalid user name {args[1]}")
                query = sql.SQL("SELECT * FROM {table} WHERE issuer_id = %s").format(
                            table = sql.Identifier(db_table))
                db_cursor.execute(query, [issuer.id])
                results = db_cursor.fetchall()

                reply_a.add_field(name = f"Total LPs for {issuer.name}", value = f"{len(results)}", inline = True)
                reply_a.add_field(name = "Most requested artist:", value = "THIS IS HARDER THAN IT LOOKS OK?")
                reply_a.add_field(name = "Wonder how this is formatted", value = "And how big it can be?")

                reply_desc = []
                reply_desc.append(f"Total LPs for {issuer.name} - {len(results)}")
                reply_desc.append(f"Most played artist - I'M STILL WORKING ON THIS")
                reply_desc.append(f"I assume this on the other hand gets wrapped at some point in the near distant eternal eonic atomic unlwaful Fallout future?")
                reply_b.description = "\n".join(reply_desc)
            else:
                logging.warning(f"Invalid argument: {args[0]}")
        finally:
            db_cursor.close()
        if reply_a.fields:
            await self.curr_message.channel.send(content= "A)")
            await self.curr_message.channel.send(embed = reply_a)
            await self.curr_message.channel.send(content= "B)")
            await self.curr_message.channel.send(embed = reply_b)

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
client_intents.members = True
client = BTWUClient(intents = client_intents)
client.run(config["secrets"]["discord"])
