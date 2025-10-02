#!/usr/bin/python3
from Telega import Telega


class tgChannelController:
    on_control = []
    messages = {}

    def __init__(self, tg, channel_names=None, db_connection=None, table_name='test1'):
        self.tg = tg  # Telega(Config.tg_session_file, Config.tg_api_id, Config.tg_api_hash)
        self.db_connection = db_connection
        self.table_name = table_name
        self.channel_names = channel_names

    def first_init(self):
        if self.channel_names:
            for channel in self.channel_names:
                ch = self.tg.find_channel_by_name(channel)
                if ch:
                    self.on_control.append(ch)
                    self.messages[ch.id] = []

    def get_new_messages(self, channel, limit=5):
        '''Возвращает новые сообщения'''

        def check_message_not_in_list(message, message_list):
            for m2 in message_list:
                if message.message == m2.message or \
                        message.date == m2.date:
                    return False
            return True

        new_msg = self.tg.client.get_messages(channel, limit=limit)
        new_messages = []
        for i in new_msg:
            if check_message_not_in_list(i, self.messages[channel.id]):
                # print('New Message: ', i)
                new_messages.append(i)
                self.messages[channel.id].append(i)
        return new_messages

    def process_row(self, r):
        if r['user_id']:
            user = self.tg.client.get_entity(r['user_id'])
            r['first_name'] = user.first_name
            r['last_name'] = user.last_name
            r['username'] = user.username
            r['phone'] = user.phone
        else:
            r['first_name'] = None
            r['last_name'] = None
            r['username'] = None
            r['phone'] = None

        return r

    def check_channels(self, max_messages_window=5):
        '''Проверяем каналы и новые сообщения добавляем в базу'''
        new_msg = []
        for channel in self.on_control:
            # print(self.get_new_messages(channel, limit=max_messages_window))
            new_msg2 = []
            for nm in self.get_new_messages(channel, limit=max_messages_window):
                print(nm)
                user_id = None
                if nm.from_id:
                    user_id = nm.from_id.user_id

                msg2 = {
                    'channel_id': channel.id,
                    'message': nm.message,
                    'date': nm.date,
                    'user_id': user_id
                }
                new_msg2.append(msg2)
            new_msg += new_msg2
        # print(new_msg)
        df = pd.DataFrame(new_msg, columns='channel_id message date user_id'.split())
        df = df.apply(self.process_row, axis=1)
        print(df)
        df.to_sql(self.table_name, con=self.db_connection, if_exists='append')


# In[2]:


import datetime

from time import sleep

import pandas as pd
import sqlalchemy as sa


def process_sending(tg):
    con = 'postgresql://postgres:postgres@192.168.0.24/postgres'
    en = sa.create_engine(con)
    with en.begin() as conn:
        to_send = pd.read_sql('tg_to_send', con=conn)
        for i, r in to_send[to_send['status'] != 'done'].iterrows():
            print(r['message'])
            tg.send_message(me, r['message'])
            with en.begin() as conn:
                sql = sa.text(
                    f"update tg_to_send set status='done' where message='{r['message']}' and status <>'done' ")
                conn.execute(sql)
                conn.commit()


def command(cmd):
    con = 'postgresql://postgres:postgres@192.168.0.24/postgres'
    en = sa.create_engine(con)
    with en.begin() as conn:

        if cmd not in done_commands:
            if cmd.message in ['/start','/stop','/status','/help']:
                pass
            else:
                return
        done_commands.append((cmd.date, cmd.message))
        com_df = pd.DataFrame([{'date': cmd.date,
                                'command': cmd.message,
                                'status': 'not_done'}])
        com_df.to_sql('commands', con=con, if_exists='append')
        print(com_df)


def process_commands(tg):
    res = tg.get_messages(me, limit=5)
    for m in res:
        if (m.date, m.message) not in done_commands:
            command(m)


# In[3]:


tg = Telega()

tg2 = tg.client.start()
me = tg.get_me()

print(me)

period = 10
done_commands = []

# In[4]:


tg_ch_controller = tgChannelController(
    tg=tg,
    channel_names=['Интерфакс', 'Finam Alert'],
    db_connection='postgresql://postgres:postgres@192.168.0.24/postgres',
    table_name='news_tg'
)
tg_ch_controller.first_init()

if __name__ == '__main__':
    while True:
        # print('process')
        process_commands(tg2)
        process_sending(tg2)
        tg_ch_controller.check_channels()
        # print('sleep')
        sleep(period)
