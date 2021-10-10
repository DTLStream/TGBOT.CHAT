from functools import reduce

from telegram import messageid
from .errc import * # error code
from .db import * # database methods, currently only postgresql supported
import telegram as t
from telegram import parsemode
import telegram.ext as te
import time as tm # timestamp
import json # decode db objects
import bz2 # for json compress and postgresql storage
import logging
logger = logging.getLogger()

# module global variables
botconfig = {}

def initbotconf(botconf):
    botconfig.update(botconf)
    # get bot token
    token = botconfig.get("token",'x').strip()
    if token=='x':
        logger.error('no token specified')
        exit(E_NOTOK)
    # get bot master
    masterid = botconfig.get("masterid",'x')
    if masterid=='x':
        logger.error('master uid not specified')
        exit(E_NOUID)
    else:
        botconfig['masterid'] = int(masterid)
    if botconfig.get('masterchatid','x')=='x':
        logger.info('no masterchatid,use masterid instead')
        botconfig['masterchatid'] = botconfig['masterid']
    botconfig['masterchatid'] = int(botconfig['masterchatid'])
    


def isMaster(uid): # check masterid
    masterid = botconfig.get('masterid','x')
    if int(uid)!=masterid:
        return False
    return True

def botwarn(msg,bot:t.Bot):
    bot.send_message(botconfig['masterchatid'],text=msg)

def time():
    return int(tm.time())

"""
user, chat, message = \
    update.effective_user, update.effective_chat, update.effective_message
"""

def startHandler(update: t.Update, context: te.CallbackContext):
    user = update.effective_user
    # uid = update.effective_user.id
    msg = 'Hello, {}.{}'.\
            format(
                user.full_name,
                '\nAt your service, Master!' if isMaster(user.id)
                else '\nPlease talk to me directly and wait for my reply.'
            )
    update.effective_message.reply_text(msg)

def userinfoHandler(update: t.Update, context: te.CallbackContext):
    user = update.effective_user
    try:
        msg = 'User: {}\nUsername: {}\nUserID: {}\nBot?: {}\nBotOwner?: {}'.format(
            user.first_name + (' {}'.format(user.last_name) if user.last_name else ''),
            user.username if user.username else '[not set]',
            user.id,
            'True' if user.is_bot else 'False',
            'True' if isMaster(user.id) else 'False'
        )
        update.effective_chat.send_message(msg)
    except Exception as e:
        errmsg = str(e)
        update.effective_chat.send_message(errmsg+'[userinfoHandler]')

# forward based on different message types, from others to master
# handle MESSAGE_HISTORY/MESSAGE_MAP
# message: from others, mastermsg: to masterchat
def forwardRoute(message: t.Message, chat: t.Chat, bot: t.Bot):
    mastermsg = None
    session = orm.sessionmaker(dbconfig['engine'])
    try:
        if message.invoice:
            logger.debug('processing message type invoice')
            msg = 'invoice message not forwarded'
            mastermsg = bot.send_message(
                botconfig['masterchatid'],
                text=msg
            )

        elif message.new_chat_members:
            logger.debug('processing message type new_chat_members')
            members = message.new_chat_members
            msg = 'new chat members:'
            for mb in members:
                usermarkup = '[{}](tg://user?id={}){}'.format(
                    mb.full_name,
                    mb.id,
                    mb.username if mb.username else ''
                )
                msg += ('\n' + usermarkup)
            mastermsg = bot.send_message(
                botconfig['masterchatid'],
                text=msg,
                parse_mode=t.ParseMode.MARKDOWN_V2
            )

        elif message.left_chat_member:
            logger.debug('processing message type left_chat_member')
            member = message.left_chat_member
            msg = 'left member: [{}](){}'.format(
                member.full_name,
                member.id,
                member.username if member.username else ''
            )
            mastermsg = bot.send_message(
                botconfig['masterchatid'],
                text=msg
            )

        elif message.new_chat_title:
            logger.debug('processing message type new_chat_title')
            msg = 'chat title updated: {}'.format(message.new_chat_title)
            mastermsg = bot.send_message(
                botconfig['masterchatid'],
                text=msg
            )

        elif message.new_chat_photo:
            logger.debug('processing message type new_chat_photo')
            ps = message.new_chat_photo
            photo = reduce(
                lambda p1,p2:p1 if p1.width>p2.width else p2, # file_size is Optional
                ps
            )
            mastermsg = bot.send_photo(
                botconfig['masterchatid'],
                photo=photo,
                caption='chat photo updated'
            )

        elif message.delete_chat_photo:
            logger.debug('processing message type delete_chat_photo')
            msg = 'chat photo removed'
            mastermsg = bot.send_message(
                botconfig['masterchatid'],
                text=msg
            )

        elif message.group_chat_created:
            logger.debug('processing message type group_chat_created')
            msg = 'group chat[{}] created'.format(message.chat.username)
            mastermsg = bot.send_message(
                botconfig['masterchatid'],
                text=msg
            )

        elif message.supergroup_chat_created:
            logger.debug('processing message type supergroup_chat_created')
            msg = 'supergroup chat[{}] created'.format(message.chat.username)
            mastermsg = bot.send_message(
                botconfig['masterchatid'],
                text=msg
            )

        elif message.channel_chat_created:
            logger.debug('processing message type channel_chat_created')
            msg = 'channel chat[{}] created'.format(message.chat.username)
            mastermsg = bot.send_message(
                botconfig['masterchatid'],
                text=msg
            )

        elif message.message_auto_delete_timer_changed:
            logger.debug('processing message type message_auto_delete_timer_changed')
            msg = 'autodelete timer set to {}s'.format(
                message.message_auto_delete_timer_changed.message_auto_delete_time
            )
            mastermsg = bot.send_message(
                botconfig['masterchatid'],
                text=msg
            )

        elif message.migrate_to_chat_id:
            logger.debug('processing message type migrate_to_chat_id')
            msg = 'The group has been migrated to a supergroup with id: {}'.format(
                message.migrate_to_chat_id
            )
            mastermsg = bot.send_message(
                botconfig['masterchatid'],
                text=msg
            )

        elif message.migrate_from_chat_id:
            logger.debug('processing message type migrate_from_chat')
            msg = 'The supergroup has been migrated from a group with id: {}'.format(
                message.migrate_from_chat_id
            )
            mastermsg = bot.send_message(
                botconfig['masterchatid'],
                text=msg
            )

        elif message.pinned_message:
            logger.debug('processing message type pinned_message')
            msg = 'pinned message' # [TODO] reply to a message in DB
            mastermsg = bot.send_message(
                botconfig['masterchatid'],
                text=msg
            )

        else:
            logger.debug('processing message type simple')
            mastermsg = message.forward(botconfig['masterchatid'])
            # hint reply_to if reply_message exists
            if message.reply_to_message:
                with session.begin() as sess:
                    # MESSAGE.ch_id==chat.id is this true? or can ch_id be another chat?
                    # dbq = sess.query(MESSAGE).\
                    #     filter(MESSAGE.ch_id==chat.id).\
                    #         filter(MESSAGE.msg_id==message.reply_to_message.message_id)
                    # directly lookup map table
                    dbq = sess.query(MESSAGE_MAP).\
                        filter(MESSAGE_MAP.s_ch_id==chat.id).\
                            filter(MESSAGE_MAP.s_msg_id==message.reply_to_message.message_id)
                    dbmap = dbq.first()
                    if dbmap:
                        # dbmap.m_ch_id should be masterchat
                        dbq = sess.query(MESSAGE).\
                            filter(MESSAGE.ch_id==dbmap.m_ch_id).\
                                filter(MESSAGE.msg_id==dbmap.m_msg_id)
                        dbmsg2reply = dbq.first()
                        if dbmsg2reply and dbmsg2reply.ch_id==botconfig['masterchatid']:
                            # do not add to mastermsg, since this is only a service msg
                            # [TODO] add debug
                            bot.send_message(
                                botconfig['masterchatid'],
                                text='reply message',
                                reply_to_message_id=dbmsg2reply.msg_id,
                                allow_sending_without_reply=True
                            )

    except Exception as e:
        # exception may be other problems including network ones,
        # however once the message is successfully sent, it should be added to MAP/HISTORY
        msg = str(e)
        botwarn(msg,bot)
    
    if (mastermsg!=None): # successfully sent
        try:
            cont,comp = msgcompress(mastermsg)
            # message
            dbmastermsg = MESSAGE(
                msg_id=mastermsg.message_id,
                ch_id=botconfig['masterchatid'],
                content=cont,
                compressed=comp,
                timestamp=time()
            )
            # message_map
            dbmsgmap = MESSAGE_MAP(
                m_ch_id=botconfig['masterchatid'],
                m_msg_id = mastermsg.message_id,
                s_ch_id = chat.id,
                s_msg_id = message.message_id,
                direction = MSGDIR.s2m,
                timestamp = time()
            )
            with session.begin() as sess:
                sess.add(dbmastermsg)
                sess.add(dbmsgmap)
            # message_history
            msghistory(mastermsg, bot)
        except Exception as e:
            botwarn('{}\nforwardRoute'.format(e),bot)
    else:
        botwarn('failed to sent', bot)

# receive message from others [TODO] quote/reply_to/switch
# 1: currentchat check
# 2.1[match]: save DB and forward
# 2.2[not match]: save DB
# src: update.chat, dst: masterchatid
def receiveHandler(update: t.Update, context: te.CallbackContext):
    chat = update.effective_chat
    message = update.effective_message
    # check chat and insert if chat not exists
    dbchat = None
    session = orm.sessionmaker(dbconfig['engine'])
    with session.begin() as sess:
        dbq = sess.query(CHAT).filter_by(ch_id=chat.id)
        dbqcount = dbq.count()
    if dbqcount!=1: # cannot call .count() directly, considered as a transaction?
        try:
            dbchat = CHAT(ch_id=chat.id,ch_name=chat.full_name,ch_type=chatype(chat))
            with session.begin() as sess:
                sess.add(dbchat)
            botwarn('{}\nnew chat available'.format(chat.full_name),context.bot)
        except Exception as e:
            botwarn('{},{}\ninsertion failure, rolled back'.format(dbchat,e),context.bot)
            return
    # save message to DB
    try:
        msg, cp = msgcompress(message)
        dbmsg = MESSAGE(
            ch_id=chat.id,
            msg_id=message.message_id,
            content=msg,
            compressed=cp,
            timestamp=time()
        )
        with session.begin() as sess:
            sess.add(dbmsg)
    except Exception as e:
        botwarn('{}'.format(e),context.bot)
    # check current chat
    with session.begin() as sess:
        dbq = sess.query(BOTSTATE).filter_by(s_k='currentchat')
        dbcurrentchat = dbq.first()
        currentchat = int(dbcurrentchat.s_v) if dbcurrentchat else None
    # if current chat matches chat, call forwardRoute,
    # in which MESSAGE_MAP/MESSAGE_HISTORY should be saved
    if currentchat==chat.id:
        forwardRoute(message,chat,context.bot)
    # else save in MESSAGE_QUEUE
    else:
        msgqueue(message,chat,context.bot)
    
    # in which MAP and HISTORY should be saved. HISTORY better refers to message in masterchat
    # when handling delete, the delete request shall not be added to message!
    # else save message in queue

# receive message from master
# 1: check if currentchat is available
# 2: message type
# 3: copyMessage
def receiveMasterHandler(update: t.Update, context: te.CallbackContext):
    chat = update.effective_chat
    message = update.effective_message
    # # [TODO] ERROR
    # check currentchat in db
    session = orm.sessionmaker(dbconfig['engine'])
    with session.begin() as sess:
        dbq = sess.query(CHAT).filter_by(ch_id=botconfig['masterchatid'])
        if dbq.count()==0:
            dbmasterchat = CHAT(
                ch_id=botconfig['masterchatid'],
                ch_name=chat.full_name,
                ch_type=chatype(chat)
            )
            sess.add(dbmasterchat)
        # save message
        msg, cp = msgcompress(message)
        dbmsg = MESSAGE(
            ch_id=chat.id,
            msg_id=message.message_id,
            content=msg,
            compressed=cp,
            timestamp=time()
        )
        sess.add(dbmsg)
        dbq = sess.query(BOTSTATE).filter_by(s_k='currentchat')
        dbotstate = dbq.first()
        currentchat = int(dbotstate.s_v) if dbotstate else None
    if not currentchat:
        botwarn('no chat selected',context.bot)
    else:
        if (message.invoice or
            message.new_chat_members or
            message.left_chat_member or
            message.new_chat_title or
            message.new_chat_photo or
            message.delete_chat_photo or
            message.group_chat_created or
            message.supergroup_chat_created or
            message.channel_chat_created or
            message.message_auto_delete_timer_changed or
            message.migrate_to_chat_id or
            message.migrate_from_chat_id or
            message.pinned_message
        ):
            botwarn('not supported type of message',context.bot)
        else:
            # [TODO] sendDice/basketball/.../poll(forward back)
            # check reply_to
            dbmsgmapres = {}
            if message.reply_to_message:
                with session.begin() as sess:
                    dbq = sess.query(MESSAGE_MAP).\
                        filter(MESSAGE_MAP.m_ch_id==botconfig['masterchatid']).\
                            filter(MESSAGE_MAP.m_msg_id==message.reply_to_message.message_id).\
                                filter(MESSAGE_MAP.s_ch_id==currentchat)
                    dbmsgmap = dbq.first()
                    if not dbmsgmap:
                        botwarn('message being replied to not found, not sent', context.bot)
                        return
                    dbmsgmapres['s_ch_id'] = dbmsgmap.s_ch_id
                    dbmsgmapres['s_msg_id'] = dbmsgmap.s_msg_id
            if dbmsgmapres and dbmsgmapres['s_ch_id']!=currentchat:
                botwarn('message being replied to not in current chat')
                return
            slavemsg = context.bot.copy_message(
                currentchat, # currentchat
                from_chat_id=botconfig['masterchatid'], # chat.id
                message_id=message.message_id,
                reply_to_message_id=dbmsgmapres['s_msg_id'] if dbmsgmapres else None,
                allow_sending_without_reply=True
            )
            # save to message/map
            with session.begin() as sess:
                msg, cp = msgcompress(slavemsg)
                dbslvmsg = MESSAGE(
                    ch_id=currentchat, # XXX int(dbotstate.s_v)
                    msg_id=slavemsg.message_id,
                    content=msg,
                    compressed=cp,
                    timestamp=time()
                )
                sess.add(dbslvmsg)
                dbnewmap = MESSAGE_MAP(
                    m_ch_id=botconfig['masterchatid'],
                    m_msg_id = message.message_id,
                    s_ch_id = currentchat,
                    s_msg_id = slavemsg.message_id,
                    direction = MSGDIR.m2s,
                    timestamp = time()
                )
                sess.add(dbnewmap)
            msghistory(message,context.bot)


# switch chat command handler, using inline keyboard1
def switchHandler(update: t.Update, context: te.CallbackContext):
    # get available chats
    session = orm.sessionmaker(dbconfig['engine'])
    chats = []
    with session.begin() as sess:
        dbq = sess.query(CHAT).filter(CHAT.ch_id!=botconfig['masterchatid'])
        for chat in dbq.all():
            chats.append({"ch_id":chat.ch_id,"ch_name":chat.ch_name})
    if not chats:
        botwarn('currently you have no chat available',context.bot)
    else:
        btnperline = 2
        btns = 0
        markupbuttons, markupbuttonline = [], []
        for chat in chats:
            kbtn = t.InlineKeyboardButton(
                '{}'.format(chat['ch_name']),
                callback_data=str(chat['ch_id']) # convert ch_id to string
            )
            markupbuttonline.append(kbtn)
            btns += 1
            if btns%btnperline==0:
                markupbuttons.append(markupbuttonline)
                markupbuttonline = []
        if markupbuttonline:
            markupbuttons.append(markupbuttonline)
        kbmarkup = t.InlineKeyboardMarkup(markupbuttons)
        context.bot.send_message(
            botconfig['masterchatid'],
            text='choose a chat',
            reply_markup=kbmarkup
        )


# switch chat callback query handler
def switchCallbackHandler(update: t.Update, context: te.CallbackContext):
    query = update.callback_query
    # check callback message sender first
    if int(query.from_user.id)!=botconfig['masterid'] and \
        int(query.message.chat_id)!=botconfig['masterchatid']:
        botwarn('invalid query source',context.bot)
        return
    ch_id = int(query.data)
    session = orm.sessionmaker(dbconfig['engine'])
    message_queue = []
    message_history_id = None
    with session.begin() as sess:
        dbqtargetchat = sess.query(CHAT).filter(CHAT.ch_id==ch_id).\
            filter(CHAT.ch_id!=botconfig['masterchatid'])
        if dbqtargetchat.count()!=1:
            botwarn('selected chat not available',context.bot)
            return
        dbqbotstate = sess.query(BOTSTATE).filter(BOTSTATE.s_k=='currentchat')
        botstate = dbqbotstate.first()
        if botstate:
            botstate.s_v = str(ch_id)
        else:
            botstate = BOTSTATE(s_k='currentchat',s_v=str(ch_id))
            sess.add(botstate)
        # hint history and purge data in queue
        # history
        dbqmaphist = sess.query(MESSAGE_MAP).filter_by(s_ch_id=ch_id).\
            filter_by(m_ch_id=botconfig['masterchatid']).\
                order_by(MESSAGE_MAP.timestamp.desc())
        histmsg = dbqmaphist.first()
        if histmsg:
            message_history_id = histmsg.m_msg_id
        # queue
        dbqque = sess.query(MESSAGE_QUEUE).filter(MESSAGE_QUEUE.ch_id==ch_id).\
            order_by(MESSAGE_QUEUE.timestamp.asc())
        for quemsg in dbqque.all():
            dbqquemsg = sess.query(MESSAGE).filter_by(ch_id=ch_id).\
                filter_by(msg_id=quemsg.msg_id).first()
            if dbqquemsg:
                msg_t = msgdecompress(dbqquemsg.content,dbqquemsg.compressed)
                msg = t.Message.de_json(msg_t,context.bot)
                message_queue.append(msg)
            sess.delete(quemsg) # dequeue
    try:
        context.bot.send_message(
            botconfig['masterchatid'],
            text='history message',
            reply_to_message_id=message_history_id
        )
    except Exception as e:
        botwarn('{}'.format(e),context.bot)
    for msg in message_queue:
        try:
            # cannot restore chat object, so just construct one, dirty solution
            with session.begin() as sess:
                dbqchat = sess.query(CHAT).filter(CHAT.ch_id==ch_id).first()
                ch_type = chatypestr(dbqchat.ch_type)
                chat_obj = t.Chat(id=ch_id,type=ch_type,title=dbqchat.ch_name)
            forwardRoute(msg,chat_obj,context.bot)
        except Exception as e:
            botwarn('{}'.format(e),context.bot)


# [TODO] register notification callback

# [TODO] delete notification callback

# return: (data,compressed?), json is encoded with utf-8
def msgcompress(message:t.Message):
    js = message.to_json().encode('utf-8')
    cjs = bz2.compress(js)
    if len(js)>len(cjs):
        return (cjs,True)
    else:
        return (js,False)

def msgdecompress(message,compressed):
    if compressed:
        message = bz2.decompress(message)
    dec = json.JSONDecoder()
    return dec.decode(message.decode('utf-8'))

# msgqueue: save message in queue, wait for forwarding
def msgqueue(message:t.Message, chat:t.Chat, bot:t.Bot):
    session = orm.sessionmaker(dbconfig['engine'])
    try:
        dbmsgque = MESSAGE_QUEUE(
            ch_id=chat.id,
            msg_id=message.message_id,
            timestamp=time()
        )
        with session.begin() as sess:
            sess.add(dbmsgque)
        # set botwarn schedule since current chat not matched
    except Exception as e:
        botwarn('{}\nmsgqueue'.format(e),bot)

# msghist: save message in history, for following proceeding (reply_to/...)
# history messages are those within masterchat, not that within other chats
def msghistory(message:t.Message, bot:t.Bot):
    session = orm.sessionmaker(dbconfig['engine'])
    try:
        dbmsghist = MESSAGE_HISTORY(
            ch_id=botconfig['masterchatid'],
            msg_id=message.message_id,
            timestamp=time()
        )
        with session.begin() as sess:
            sess.add(dbmsghist)
    except Exception as e:
        botwarn('{}\nmsghistory'.format(e),bot)

# return chat type enum
def chatype(chat:t.Chat):
    ttype = CHTYPE.private
    if chat.type=='channel':ttype=CHTYPE.channel
    elif chat.type=='group':ttype=CHTYPE.supergroup
    elif chat.type=='supergroup':ttype=CHTYPE.supergroup
    return ttype

def chatypestr(chtype:CHTYPE):
    if chtype==CHTYPE.private: return 'private'
    if chtype==CHTYPE.group: return 'group'
    if chtype==CHTYPE.supergroup: return 'supergroup'
    if chtype==CHTYPE.channel: return 'channel'