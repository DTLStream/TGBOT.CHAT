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
# handle MESSAGE_MAP
# message: from others, mastermsg: to masterchat
def forwardRoute(message: t.Message, chat: t.Chat, bot: t.Bot):
    mastermsg = None
    Session = dbconfig['session']
    session = Session()
    try:
        # filter special message first
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
        # [x] check pinned message and reply to it
        elif message.pinned_message:
            logger.debug('processing message type pinned_message')
            dbpinmsgmap = session.query(MESSAGE_MAP).\
                filter_by(m_ch_id=botconfig['masterchatid']).\
                filter_by(s_ch_id=chat.id).\
                filter_by(s_msg_id=message.pinned_message.message_id).first()
            pinmsgmapid = dbpinmsgmap.m_msg_id if dbpinmsgmap else None
            msg = 'pinned message' # [x] reply to a message in DB
            mastermsg = bot.send_message(
                botconfig['masterchatid'],
                text=msg,
                reply_to_message_id=pinmsgmapid,
                allow_sending_without_reply=True
            )
        # ordinary message
        else:
            logger.debug('processing ordinary message')
            mastermsg = message.forward(botconfig['masterchatid'])
            
        # insert message in MESSAGE/MESSAGE_MAP immediately in case reply_to_message throws
        if mastermsg: # mastermsg successfully sent
            with Session.begin() as sess:
                cont, cp = msgcompress(mastermsg)
                # MESSAGE
                dbmastermsg = MESSAGE(
                    msg_id=mastermsg.message_id,
                    ch_id=botconfig['masterchatid'],
                    content=cont,
                    compressed=cp,
                    timestamp=time()
                )
                sess.add(dbmastermsg)
                # MESSAGE_MAP
                dbmsgmap = MESSAGE_MAP(
                    m_ch_id=botconfig['masterchatid'],
                    m_msg_id = mastermsg.message_id,
                    s_ch_id = chat.id,
                    s_msg_id = message.message_id,
                    direction = MSGDIR.s2m,
                    timestamp = time()
                )
                sess.add(dbmsgmap)
        else:
            botwarn('forward failed, check mastermsg', bot)
            logger.info('mastermsg: {} forwardRoute'.format(mastermsg))
            return
        
        # hint reply_to if reply_message exists
        if message.reply_to_message:
            # directly lookup map table to find originally sent message
            dbmap = session.query(MESSAGE_MAP).filter_by(s_ch_id=chat.id).\
                filter_by(s_msg_id=message.reply_to_message.message_id).\
                    filter_by(m_ch_id=botconfig['masterchatid']).first()
            if dbmap: # found the reply_to message
                    # do not add to MESSAGE, since this is only a service msg
                    # [x] add debug
                bot.send_message(
                    botconfig['masterchatid'], # guarantee to be m_ch_id
                    text='reply',
                    reply_to_message_id=dbmap.m_msg_id,
                    # such service messages must be replies, else throw error, won't cause damage
                    allow_sending_without_reply=False
                )
            # the message replied to which is not found in db should be warned
            else:
                botwarn(
                    'reply_to message not found\n{}'.\
                        format(message.reply_to_message.message_id),
                    bot
                )
    # exception for any error if occured
    except Exception as e:
        # exception may be other problems including network ones,
        # however once the message is successfully sent, it should be added to MAP/HISTORY
        botwarn('{}'.format(e),bot)
        logger.warn('{} forwardRoute'.format(e))


# receive message from others [x] quote/reply_to/switch
# 1: currentchat check
# 2.1[match]: save DB and forward
# 2.2[not match]: save DB
# src: update.chat, dst: masterchatid
def receiveHandler(update: t.Update, context: te.CallbackContext):
    Session = dbconfig['session']
    session = Session()
    chat = update.effective_chat
    message = update.effective_message
    # check chat and insert if chat not exists
    dbchat = None
    dbqcount = session.query(CHAT).filter_by(ch_id=chat.id).count()
    if dbqcount!=1: # cannot call .count() directly, considered as a transaction?
        try:
            ch_name = getChatname(chat)
            with Session.begin() as sess:
                dbchat = CHAT(ch_id=chat.id,ch_name=ch_name,ch_type=chatype(chat))
                sess.add(dbchat)
            botwarn('new chat [{}] available'.format(ch_name),context.bot)
            logger.info('new chat [{}] available'.format(ch_name))
        except Exception as e:
            botwarn('{},{}'.format(dbchat,e),context.bot)
            logger.warn('{},{} receiveHandler'.format(dbchat,e))
            # return if chat cannot be inserted
            return
    # save message to DB
    try:
        # [x] edited message has same id, which should be updated in db
        with Session.begin() as sess:
            # check if new message is an edited message
            dbmsg = sess.query(MESSAGE).\
                filter_by(ch_id=chat.id).filter_by(msg_id=message.message_id).first()
            if dbmsg: # edited
                # save in bucket, delete map in DB, delete message in DB (ondelete=cascade)
                dbbucketmsg = OLD_MESSAGE_BUCKET(
                    ch_id=dbmsg.ch_id,
                    msg_id=dbmsg.msg_id,
                    content=dbmsg.content,
                    compressed=dbmsg.compressed,
                    timestamp=dbmsg.timestamp
                )
                sess.add(dbbucketmsg)
                sess.delete(dbmsg)
            # insert message into db
            cont, cp = msgcompress(message)
            dbmsg = MESSAGE(
                ch_id=chat.id,
                msg_id=message.message_id,
                content=cont,
                compressed=cp,
                timestamp=time()
            )
            sess.add(dbmsg)
    except Exception as e:
        botwarn('{}'.format(e),context.bot)
        logger.warn('{} receiveHandler'.format(e))
        # return if message cannot be inserted
        return
    # check current chat
    dbcurrentchat = session.query(BOTSTATE).filter_by(s_k='currentchat').first()
    currentchat = int(dbcurrentchat.s_v) if dbcurrentchat else None
    # current chat matches chat: call forwardRoute within which MESSAGE_MAP should be saved
    if currentchat==chat.id:
        forwardRoute(message,chat,context.bot)
    # else: save in MESSAGE_QUEUE
    else:
        msgqueue(message,chat,context.bot)


# receive message from master
# 1: check if currentchat is available
# 2: message type
# 3: copyMessage
def receiveMasterHandler(update: t.Update, context: te.CallbackContext):
    Session = dbconfig['session']
    session = Session()
    chat = update.effective_chat
    message = update.effective_message
    # # [x] ERROR if called /sw before any message
    # check currentchat
    dbotstate = session.query(BOTSTATE).filter_by(s_k='currentchat').first()
    currentchat = int(dbotstate.s_v) if dbotstate else None
    if not currentchat:
        botwarn('no chat selected, try /sw',context.bot)
        logger.info('{} receiveMasterHandler'.format('no chat selected, try /sw'))
        return
    # check masterchat CHAT, if not exist, warn user to call /sw
    masterchat_in_db = (session.query(CHAT).filter_by(ch_id=botconfig['masterchatid']).count()==1)
    if not masterchat_in_db:
        botwarn('{} receiveMasterHandler'.format('master chat not in db, try /sw'),context.bot)
        logger.info('{} receiveMasterHandler'.format('master chat not in db, try /sw'))
        return
    # begin saving message, checking currentchat, ...
    with Session.begin() as sess:
        # [x] check if the message is an edited one first
        MESSAGE2 = orm.aliased(MESSAGE)
        dbmsgtup = sess.query(MESSAGE,MESSAGE2).\
            join(
                MESSAGE_MAP,
                sql.and_(
                    MESSAGE_MAP.m_ch_id==MESSAGE.ch_id,
                    MESSAGE_MAP.m_msg_id==MESSAGE.msg_id
                )
            ).\
            join(
                MESSAGE2,
                sql.and_(
                    MESSAGE_MAP.s_ch_id==MESSAGE2.ch_id,
                    MESSAGE_MAP.s_msg_id==MESSAGE2.msg_id
                )
            ).\
            filter(MESSAGE.ch_id==chat.id).filter(MESSAGE.msg_id==message.message_id).first()
        if dbmsgtup: # edited
            dbmsg, dbslvmsg = dbmsgtup
            # delete original message first
            if not context.bot.delete_message(chat_id=dbslvmsg.ch_id,message_id=dbslvmsg.msg_id):
                botwarn('edited message not deleted',context.bot)
            # save in OLD_MESSAGE_BUCKET
            dbbucketmsg = OLD_MESSAGE_BUCKET(
                ch_id=dbmsg.ch_id,
                msg_id=dbmsg.msg_id,
                content=dbmsg.content,
                compressed=dbmsg.compressed,
                timestamp=dbmsg.timestamp
            )
            sess.add(dbbucketmsg)
            sess.delete(dbmsg)
            sess.delete(dbslvmsg)
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
        botwarn('unsupported type of message',context.bot)
    else:
        # [TODO] sendDice/basketball/.../poll(forward back)
        # check reply_to
        dbmsgmapres = {}
        if message.reply_to_message:
            with Session.begin() as sess:
                dbmsgmap = sess.query(MESSAGE_MAP).\
                    filter(MESSAGE_MAP.m_ch_id==botconfig['masterchatid']).\
                    filter(MESSAGE_MAP.m_msg_id==message.reply_to_message.message_id).\
                    filter(MESSAGE_MAP.s_ch_id==currentchat).\
                    first()
                if not dbmsgmap:
                    botwarn('message being replied to not found, not sent', context.bot)
                    return
                dbmsgmapres['s_ch_id'] = dbmsgmap.s_ch_id
                dbmsgmapres['s_msg_id'] = dbmsgmap.s_msg_id
        if dbmsgmapres and dbmsgmapres['s_ch_id']!=currentchat:
            botwarn('message being replied to not in current chat', context.bot)
            return
        slavemsg = context.bot.copy_message(
            currentchat, # currentchat
            from_chat_id=botconfig['masterchatid'], # chat.id
            message_id=message.message_id,
            reply_to_message_id=dbmsgmapres['s_msg_id'] if dbmsgmapres else None,
            allow_sending_without_reply=True # send without reply if original ones deleted
        )
        # save to message/map
        with Session.begin() as sess:
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
        # unnecessary
        # msghistory(message,context.bot)


# switch chat command handler, using inline keyboard1
def switchHandler(update: t.Update, context: te.CallbackContext):
    Session = dbconfig['session']
    session = Session()
    chat, message = update.effective_chat, update.effective_message
    # if master CHAT is not inserted, insert it now
    masterchat_in_db = (session.query(CHAT).filter_by(ch_id=botconfig['masterchatid']).count()==1)
    if not masterchat_in_db:
        with Session.begin() as sess:
            dbchat = CHAT(ch_id=chat.id,ch_name=getChatname(chat),ch_type=chatype(chat))
            sess.add(dbchat)
    # get available chats
    chats = []
    dbqchats = session.query(CHAT).filter(CHAT.ch_id!=botconfig['masterchatid']).all()
    for chat in dbqchats:
        chats.append({"ch_id":chat.ch_id,"ch_name":chat.ch_name})
    if not chats:
        botwarn('no chat currently available',context.bot)
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
    Session = dbconfig['session']
    session = Session()
    query = update.callback_query
    # check callback message sender first
    # strict check, not allow anyone else to fake request
    if int(query.from_user.id)!=botconfig['masterid'] or \
        not query.message or int(query.message.chat_id)!=botconfig['masterchatid']:
        botwarn('invalid query source',context.bot)
        return
    ch_id = int(query.data)
    dbqtargetchat = session.query(CHAT).filter_by(ch_id=ch_id).\
        filter(CHAT.ch_id!=botconfig['masterchatid']).count()
    if dbqtargetchat!=1:
        botwarn('{}'.format('chat {} not found'.format(ch_id)), context.bot)
        logger.info('{} switchCallbackHandler'.format('chat {} not found'.format(ch_id)))
        return
    # update currentchat and handle MESSAGE_QUEUE/MESSAGE forward, in a transaction
    queuedmessages = []
    with Session.begin() as sess:
        dbcurrentchat = sess.query(BOTSTATE).filter_by(s_k='currentchat').first()
        if dbcurrentchat:
            dbcurrentchat.s_v = str(ch_id)
        else:
            dbcurrentchat = BOTSTATE(s_k='currentchat',s_v=str(ch_id))
            sess.add(dbcurrentchat)
        # hint history and purge data in queue
        # history
        dbqhistmsgmap = sess.query(MESSAGE_MAP).filter_by(s_ch_id=ch_id).\
            filter_by(m_ch_id=botconfig['masterchatid']).\
                order_by(MESSAGE_MAP.timestamp.desc()).first()
        if dbqhistmsgmap:
            message_history_id = dbqhistmsgmap.m_msg_id
            # begin to hint history
            try:
                context.bot.send_message(
                    botconfig['masterchatid'],
                    text='history starts from here',
                    reply_to_message_id=message_history_id,
                    allow_sending_without_reply=False # must be a reply
                )
            except Exception as e:
                botwarn('{}'.format(e),context.bot)
                logger.warn('{}'.format(e))
                # continue even though the message to be replied does not exist
        # queue
        # try join, delete queued messages one by one
        dbqmsgtups = sess.query(MESSAGE,MESSAGE_QUEUE).\
            join(MESSAGE_QUEUE,
                sql.and_(
                    MESSAGE.ch_id==MESSAGE_QUEUE.ch_id,
                    MESSAGE.msg_id==MESSAGE_QUEUE.msg_id
                )
            ).filter(MESSAGE_QUEUE.ch_id==ch_id).order_by(MESSAGE.timestamp.asc()).all()
        # begin purging queue
        for msgtup in dbqmsgtups:
            dbmsg, msgque = msgtup
            sess.delete(msgque) # delete from queue
            # restore message
            msg_t = msgdecompress(dbmsg.content,dbmsg.compressed)
            msg = t.Message.de_json(msg_t,context.bot)
            queuedmessages.append(msg)
    # first create a chat for forwardRoute ( dirty trick, not recommended )
    dbqchat = session.query(CHAT).filter_by(ch_id=ch_id).first()
    ch_type = chatypestr(dbqchat.ch_type)
    chat_obj = t.Chat(id=ch_id,type=ch_type,title=dbqchat.ch_name)
    for msg in queuedmessages:
        forwardRoute(msg,chat_obj,context.bot)


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
    Session = dbconfig['session']
    session = Session()
    try:
        with Session.begin() as sess:
            dbmsgque = MESSAGE_QUEUE(
                ch_id=chat.id,
                msg_id=message.message_id,
                timestamp=time()
            )
            sess.add(dbmsgque)
        # [TODO] set botwarn schedule since current chat not matched
    except Exception as e:
        botwarn('{}'.format(e),bot)
        logger.warn('{} msgqueue'.format(e))


# MESSAGE_HISTORY unnecessary, history already recorded in MESSAGE_MAP.timestamp
# msghist: save message in history, for following proceeding (reply_to/...)
# history messages are those within masterchat, not that within other chats
def msghistory(message:t.Message, bot:t.Bot):
    pass
    # Session = dbconfig['session']
    # session = Session()
    # try:
    #     dbmsghist = MESSAGE_HISTORY(
    #         ch_id=botconfig['masterchatid'],
    #         msg_id=message.message_id,
    #         timestamp=time()
    #     )
    #     with Session.begin() as sess:
    #         sess.add(dbmsghist)
    # except Exception as e:
    #     botwarn('{}\nmsghistory'.format(e),bot)

# return chat type enum
def chatype(chat:t.Chat):
    return CHTYPE(chat.type)
    # ttype = CHTYPE.private
    # if chat.type=='channel':ttype=CHTYPE.channel
    # elif chat.type=='group':ttype=CHTYPE.supergroup
    # elif chat.type=='supergroup':ttype=CHTYPE.supergroup

# return name of enum var
def chatypestr(chtype:CHTYPE):
    return chtype.name
    # if chtype==CHTYPE.private: return 'private'
    # if chtype==CHTYPE.group: return 'group'
    # if chtype==CHTYPE.supergroup: return 'supergroup'
    # if chtype==CHTYPE.channel: return 'channel'

def getChatname(chat:t.Chat):
    ch_name = chat.username if chat.username else (
        chat.full_name if chat.full_name else (
            chat.title
        )
    )
    return ch_name