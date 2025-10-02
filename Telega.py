import telethon.sync as telethon2
from Config import Config
import asyncio
import pandas as pd
import re

import sqlalchemy as sa

class Telega:
    def __init__(self):
        self.client = telethon2.TelegramClient(Config.tg_session_file, 
                                               Config.tg_api_id, 
                                               Config.tg_api_hash)
        #self.client.start()
        self.me = None
        self.commands = []

    def get_dialogs(self):
        dialogs = self.client.get_dialogs()
        return dialogs

    def get_me(self):
        self.me = self.client.get_me()
        return self.me

    def get_chats_by_kw(self, kw):
        reqs = self.client(telethon2.functions.contacts.SearchRequest(kw, limit=1000))
        return reqs.chats

    def get_chat_dialog(self, chat):
        req = self.client.get_messages(chat, limit=10)
        return req

    def send_me_message(self, message):
        r = self.client.send_message(self.me, message)

    def get_my_messages(self):
        messages = self.client.get_messages(limit=10)
        print(messages)
        self.commands.append(messages)

    def get_commands(self):
        return self.commands

    def find_channel_by_name(self, name):
        '''Найти наиболее посещаемый канал по имени'''
        t1 = self.get_chats_by_kw(name)
        if len(t1) > 0:
            return t1[0]
        return None
