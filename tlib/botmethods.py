from functools import reduce

from .errc import * # error code
from .db import * # database methods, currently only postgresql supported
import telegram as t
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


# deleteHandler for master
# check MESSAGE_MAP for s_msg, remove relevant message from db
def deleteHandler(update:t.Update, context:te.CallbackContext):
    chat, message = update.effective_chat, update.effective_message
    Session = dbconfig['session']
    session = Session()
    if not message.reply_to_message:
        botwarn('{}'.format('usage: /d [ reply the message to be deleted ]'),context.bot)
        session.close() # close before return
        return
    target_message = message.reply_to_message
    with Session.begin() as sess:
        MESSAGE2 = orm.aliased(MESSAGE)
        dbq = msgmapjoin(sess, MESSAGE2)
        dbmsgtup = dbq.filter(MESSAGE.ch_id==chat.id).\
            filter(MESSAGE.msg_id==target_message.message_id).first()
        if not dbmsgtup:
            botwarn('{}'.format('target message not found in map, not deleted'),context.bot)
            sess.rollback()
            session.close() # close before return
            return
        # get mapped messages
        msg, slvmsg = dbmsgtup
        # save msg/slvmsg (only one copy of message is required, since the other one is almost the same)
        # save msg
        msgbucketsave(sess, msg)

        # IMPORTANT NOTICE: delete_message returns True on success, but raise error if failed
        # delete messages
        # command, can be deleted immediately without hesitation
        try:
            ret = context.bot.delete_message(
                chat_id=chat.id,
                message_id=message.message_id
            )
            # if not ret: botwarn('/d command not deleted',context.bot)
        except Exception as e:
            botwarn('{} (continue removal) deleteHandler'.format(e), context.bot)
            logger.warn('{} (continue removal) deleteHandler'.format(e))

        # slave
        try:
            ret = context.bot.delete_message(
                chat_id=slvmsg.ch_id,
                message_id=slvmsg.msg_id
            )
            # only slave message cannot be observed directly
        except Exception as e:
            # if slave message not deleted, then warn and rollback
            botwarn('{} (slave message not removed, rollback) deleteHandler'.format(e), context.bot)
            logger.warn('{} (slave message not removed, rollback) deleteHandler')
            sess.rollback()
            session.close() # close before return
            return
        
        # master
        try:
            context.bot.delete_message(
                chat_id=msg.ch_id,
                message_id=msg.msg_id
            )
        # if not ret: botwarn('master message not deleted',context.bot)
        except Exception as e:
            botwarn('{} (force removal in db) deleteHandler'.format(e), context.bot)
            logger.warn('{} (force removal in db) deleteHandler'.format(e))

        # remove msg,slvmsg in db, on delete cascade for MAP
        sess.delete(msg)
        sess.delete(slvmsg)
    # close session at the end
    session.close()


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
            msg = 'left member: [{}](tg://user?id={}){}'.format(
                member.full_name,
                member.id,
                member.username if member.username else ''
            )
            mastermsg = bot.send_message(
                botconfig['masterchatid'],
                text=msg,
                parse_mode=t.ParseMode.MARKDOWN_V2
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
            msg = 'pinned message' # [x] reply to the message in DB
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
                # MESSAGE
                msgsave(sess, botconfig['masterchatid'], mastermsg)
                msgsave(sess, chat.id, message) # not save in receiveHandler, save here
                # MESSAGE_MAP
                msgmapsave(
                    sess,
                    botconfig['masterchatid'],mastermsg.message_id,
                    chat.id,message.message_id,
                    MSGDIR.s2m
                )
        else:
            botwarn('forward failed, check mastermsg', bot)
            logger.info('mastermsg: {} forwardRoute'.format(mastermsg))
            session.close() # close before return
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
    session.close()

# receive message from others [x] quote/reply_to/switch
# 1: currentchat check
# 2.1[match]: save DB and forward
# 2.2[not match]: save DB
# src: update.chat, dst: masterchatid
def receiveHandler(update: t.Update, context: te.CallbackContext):
    chat, message = update.effective_chat, update.effective_message
    Session = dbconfig['session']
    session = Session()
    # check chat and insert if chat not exists
    dbchat = None
    dbqcount = session.query(CHAT).filter_by(ch_id=chat.id).count()
    if dbqcount!=1:
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
            session.close() # close before return
            return
    # save message to DB
    try:
        # [x] edited message has same id, which should be updated in db
        with Session.begin() as sess:
            # check if new message is an edited message
            dbmsg = sess.query(MESSAGE).\
                filter_by(ch_id=chat.id).\
                filter_by(msg_id=message.message_id).first()
            if dbmsg: # message is an edited one
                # check map record, if exists then remove it
                MESSAGE2 = orm.aliased(MESSAGE)
                dbq = msgmapjoin(sess, MESSAGE2)
                dbmsgmap = dbq.filter(MESSAGE2.ch_id==chat.id).\
                    filter(MESSAGE2.msg_id==message.message_id).\
                    first()
                if dbmsgmap: # edited message has been forwarded
                    dbmastermsg, dbslavemsg = dbmsgmap # dbslavemsg==dbmsg
                    # save in bucket, delete map in DB, delete message in DB
                    # (ondelete=cascade not work due to same trasaction, manually delete all)
                    sess.delete(dbmastermsg)
                    sess.delete(dbslavemsg)
                    # try to delete mstmsg
                    try:
                        context.bot.delete_message(dbmastermsg.ch_id,dbmastermsg.msg_id)
                    except Exception as e:
                        botwarn('{} (original message not deleted) receiveHandler'.\
                            format(e), context.bot)
                        logger.warn('{} (original message not deleted) receiveHandler'.\
                            format(e))
                else: # not in map, possibly in queue
                    # remove it from MESSAGE, cause queued one to be removed
                    msgbucketsave(sess, dbmsg)
                    sess.delete(dbmsg)
            # insert message in db
            # not save here
            # do msgsave in queue/forwardRoute as a transaction
            # to avoid redundant MESSAGE if the mastermsg/msgque is not processed successfully
            # msgsave(sess, chat.id, message)
    except Exception as e:
        botwarn('{}'.format(e),context.bot)
        logger.warn('{} receiveHandler'.format(e))
        # return if message cannot be inserted
        session.close() # close before return
        return
    # check current chat
    dbcurrentchat = session.query(BOTSTATE).filter_by(s_k='currentchat').first()
    currentchat = int(dbcurrentchat.s_v) if dbcurrentchat else None
    # current chat matches chat: call forwardRoute within which MESSAGE_MAP should be saved
    if currentchat==chat.id:
        # msgsave in forwardRoute within a transaction
        forwardRoute(message,chat,context.bot)
    # else: save in MESSAGE_QUEUE
    else:
        # msgsave in messageQueue within a transaction
        messageQueue(message,chat,context.bot) # messageQueue has the same position as forwardRoute
    session.close()

# receive message from master, special messages are handled by calling other handlers
# 1: check if currentchat is available
# 2: message type
# 3: copyMessage
def receiveMasterHandler(update: t.Update, context: te.CallbackContext):
    chat, message = update.effective_chat, update.effective_message
    # hook special messages at the very beginning
    # dice
    if message.dice: return diceMasterHandler(update,context)
    # poll
    if message.poll: return pollMasterHandler(update,context)
    # start main procedure
    Session = dbconfig['session']
    session = Session()
    # # [x] ERROR if called /sw before any message
    # check currentchat and masterchat, if anyone is missing, warn user to call /switch
    currentchat = getCurrentChat(session)
    masterchat_in_db = hasChat(session, botconfig['masterchatid'])
    if not currentchat:
        botwarn('no chat selected, try /sw',context.bot)
        logger.info('{} receiveMasterHandler'.format('no chat selected, try /sw'))
        session.close() # close before return
        return
    if not masterchat_in_db:
        botwarn('{} receiveMasterHandler'.format('master chat not in db, try /sw'),context.bot)
        logger.info('{} receiveMasterHandler'.format('master chat not in db, try /sw'))
        session.close() # close before return
        return

    # begin checking edited message
    with Session.begin() as sess:
        # [x] check if the message is an edited one first
        # unlike receiveHandler, in which the MESSAGE_MAP may not be saved due to MESSAGE_QUEUE
        # receiveMasterHandler need not check MESSAGE, because MESSAGE_MAP must have been saved
        # so it's sufficient to check only MESSAGE_MAP join MESSAGES
        MESSAGE2 = orm.aliased(MESSAGE)
        dbq = msgmapjoin(sess, MESSAGE2)
        dbmsgtup = dbq.filter(MESSAGE.ch_id==chat.id).\
            filter(MESSAGE.msg_id==message.message_id).first()
        if dbmsgtup: # edited
            dbmsg, dbslvmsg = dbmsgtup
            # delete original message first
            try:
                context.bot.delete_message(
                    chat_id=dbslvmsg.ch_id,
                    message_id=dbslvmsg.msg_id
                )
            except Exception as e:
                botwarn('{} (edited message not deleted) receiveMasterHandler'.format(e), context.bot)
                logger.warn('{} (edited message not deleted) receiveMasterHandler'.format(e))
                # continue
            # save in OLD_MESSAGE_BUCKET
            msgbucketsave(sess, dbmsg)
            sess.delete(dbmsg)
            sess.delete(dbslvmsg)
        # save message
        # save message here may cause redundant message in db, if the following process throws
        # not save here, save after slavemsg is successfully sent.
        # if failed to send slavemsg, then the MAP will not be back
        # msgsave(sess, chat.id, message)
        
    if (
        message.invoice or
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
        # check reply_to
        reply_msg_id = None
        if message.reply_to_message:
            dbmsgmap = session.query(MESSAGE_MAP).\
                filter(MESSAGE_MAP.m_ch_id==botconfig['masterchatid']).\
                filter(MESSAGE_MAP.m_msg_id==message.reply_to_message.message_id).\
                filter(MESSAGE_MAP.s_ch_id==currentchat).\
                first() # check s_ch_id==current in case reply to another chat
            if not dbmsgmap:
                botwarn('message being replied to not found, not sent', context.bot)
                session.close() # close before return
                return
            reply_msg_id = dbmsgmap.s_msg_id
        # IMPORTANT NOTICE: copy_message returns MessageId object with message_id
        # so it's not a full Message
        # It works here because only message_id is used and to_json works for all tgobj
        slavemsg = context.bot.copy_message(
            currentchat, # currentchat
            from_chat_id=botconfig['masterchatid'], # chat.id
            message_id=message.message_id,
            reply_to_message_id=reply_msg_id,
            allow_sending_without_reply=True # send without reply if original ones deleted
        )
        # save to message/map
        with Session.begin() as sess:
            msgsave(sess, currentchat, slavemsg)
            msgsave(sess, chat.id, message)
            msgmapsave(
                sess,
                botconfig['masterchatid'],message.message_id,
                currentchat,slavemsg.message_id,
                MSGDIR.m2s
            )
    session.close()

# diceMasterHandler: handle messages with Dice, and dice,basketball,bowling,football,dart,slot
# (🎲🎯🎳-6)(🏀⚽-5)(🎰-64)
def diceMasterHandler(update: t.Update, context: te.CallbackContext):
    chat, message = update.effective_chat, update.effective_message
    Session = dbconfig['session']
    session = Session()
    # check currentchat,masterchat
    currentchat = getCurrentChat(session)
    masterchat_in_db = hasChat(session, botconfig['masterchatid'])
    if not currentchat:
        botwarn('no chat selected, try /sw',context.bot)
        logger.info('{} receiveDiceHandler'.format('no chat selected, try /sw'))
        session.close() # close before return
        return
    if not masterchat_in_db:
        botwarn('{} receiveMasterHandler'.format('master chat not in db, try /sw'),context.bot)
        logger.info('{} receiveDiceHandler'.format('master chat not in db, try /sw'))
        session.close() # close before return
        return
    # get message dice type
    if message.dice:
        dicetype = message.dice.emoji
    else:
        dicetype = parseDiceType(message.text)
    # check reply_to message
    reply_msg_id = None
    # (copy paste modify from receiveMasterHandler)
    if message.reply_to_message:
        dbmsgmap = session.query(MESSAGE_MAP).\
            filter(MESSAGE_MAP.m_ch_id==botconfig['masterchatid']).\
            filter(MESSAGE_MAP.m_msg_id==message.reply_to_message.message_id).\
            filter(MESSAGE_MAP.s_ch_id==currentchat).\
            first()
        if not dbmsgmap:
            botwarn('message being replied to not found, not sent', context.bot)
            session.close() # close before return
            return
        reply_msg_id = dbmsgmap.s_msg_id
    # send to slavechat, forward back, delete original message
    try:
        slavemsg = context.bot.send_dice(
            chat_id=currentchat,
            emoji=dicetype,
            reply_to_message_id=reply_msg_id,
            allow_sending_without_reply=True # not strict
        )
        # not hint reply...
        mastermsg = slavemsg.forward(botconfig['masterchatid'])
        # from API:
        # A dice message in a private chat can only be deleted if it was sent more than 24 hours ago.
        # delete original message if it's a command message, not check if deletion was sucessful
        if not message.dice:
            try:
                context.bot.delete_message(
                    chat_id=chat.id, # botconfig['masterchatid']
                    message_id=message.message_id
                )
            except Exception as e:
                botwarn('{} diceMasterHandler'.format(e),context.bot)
                logger.warn('{} diceMasterHandler'.format(e))
                # continue
    except Exception as e:
        botwarn('{} diceMasterHandler'.format(e), context.bot)
        logger.warn('{} diceMasterHandler'.format(e))
        session.close() # close before return
        return
    # on success, save in DB
    try:
        with Session.begin() as sess:
            msgsave(sess, currentchat, slavemsg)
            msgsave(sess, chat.id, mastermsg)
            msgmapsave(
                sess,
                chat.id, mastermsg.message_id,
                currentchat, slavemsg.message_id,
                MSGDIR.m2s
            )
    except Exception as e:
        botwarn('{}'.format(e),context.bot)


def pollMasterHandler(update: t.Update, context: te.CallbackContext):
    pass


# switch chat command handler, using inline keyboard1
def switchHandler(update: t.Update, context: te.CallbackContext):
    chat, message = update.effective_chat, update.effective_message
    Session = dbconfig['session']
    session = Session()
    # if master CHAT is not inserted, insert it now
    masterchat_in_db = hasChat(session, botconfig['masterchatid'])
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
    session.close()


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
        session.close() # close before return
        return
    ch_id = int(query.data)
    dbqtargetchat = session.query(CHAT).filter_by(ch_id=ch_id).\
        filter(CHAT.ch_id!=botconfig['masterchatid']).count()
    if dbqtargetchat!=1:
        botwarn('{}'.format('chat {} not found'.format(ch_id)), context.bot)
        logger.info('{} switchCallbackHandler'.format('chat {} not found'.format(ch_id)))
        session.close() # close before return
        return
    # update currentchat and handle MESSAGE_QUEUE/MESSAGE forward, in a transaction
    # if failed then rollback, the currentchat will then be unchanged
    queuedmessages = []
    # not well designed, send_message within transaction...
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
                # continue even even if the message to be replied does not exist
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
    # first create a chat for forwardRoute (dirty trick, not recommended)
    dbqchat = session.query(CHAT).filter_by(ch_id=ch_id).first()
    ch_type = chatypestr(dbqchat.ch_type)
    chat_obj = t.Chat(id=ch_id,type=ch_type,title=dbqchat.ch_name)
    for msg in queuedmessages:
        forwardRoute(msg,chat_obj,context.bot)
    session.close()

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

# messageQueue: save message in queue, wait for forwarding
def messageQueue(message:t.Message, chat:t.Chat, bot:t.Bot):
    Session = dbconfig['session']
    session = Session()
    try:
        with Session.begin() as sess:
            msgsave(sess, chat.id, message)
            dbmsgque = MESSAGE_QUEUE(
                ch_id=chat.id,
                msg_id=message.message_id,
                timestamp=time()
            )
            sess.add(dbmsgque)
        # [TODO] set botwarn schedule since current chat not matched
    except Exception as e:
        botwarn('{}'.format(e),bot)
        logger.warn('{} messageQueue'.format(e))
    session.close()

# return a query, the (MESSAGE, MESSAGE2) tuple joined with MESSAGE_MAP
def msgmapjoin(sess,MESSAGE2):
    dbq = sess.query(MESSAGE,MESSAGE2).\
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
        )
    return dbq

# save message
def msgsave(sess, ch_id, message:t.Message):
    msg, cp = msgcompress(message)
    dbmsg = MESSAGE(
        ch_id=ch_id,
        msg_id=message.message_id,
        content=msg,
        compressed=cp,
        timestamp=time()
    )
    sess.add(dbmsg)
    return dbmsg # return dbmsg for further use

# save message map
def msgmapsave(sess, m_ch_id, m_msg_id, s_ch_id, s_msg_id, direction):
    dbmsgmap = MESSAGE_MAP(
        m_ch_id=m_ch_id,
        m_msg_id=m_msg_id,
        s_ch_id=s_ch_id,
        s_msg_id=s_msg_id,
        direction=direction,
        timestamp = time()
    )
    sess.add(dbmsgmap)
    return dbmsgmap

# save old message buckekt
def msgbucketsave(sess, dbmsg):
    dbbucketmsg = OLD_MESSAGE_BUCKET(
        ch_id=dbmsg.ch_id,
        msg_id=dbmsg.msg_id,
        content=dbmsg.content,
        compressed=dbmsg.compressed,
        timestamp=dbmsg.timestamp
    )
    sess.add(dbbucketmsg)
    return dbbucketmsg

# get currentchat in int, if not exists then None
def getCurrentChat(session):
    currentchat = session.query(BOTSTATE).\
        filter_by(s_k='currentchat').first()
    if currentchat: return int(currentchat.s_v)
    else: return None

# check if chat is in db
def hasChat(session, ch_id):
    chat = session.query(CHAT).filter_by(ch_id=ch_id)
    if chat.count()==1: return True
    else: return False

# return chat type enum
def chatype(chat:t.Chat):
    return CHTYPE(chat.type)

# return name of enum var
def chatypestr(chtype:CHTYPE):
    return chtype.name

def getChatname(chat:t.Chat):
    ch_name = chat.username if chat.username else (
        chat.full_name if chat.full_name else (
            chat.title
        )
    )
    return ch_name

# parse dice type (command message)
def parseDiceType(text):
    # 🎲🎯🎳-6)(🏀⚽-5)(🎰-64
    if not text: return '🎲' # dice
    if '/slot' in text: return '🎰' # slotmachine
    if '/basket' in text: return '🏀' # basketball
    if '/soccer' in text or '/football' in text: return '⚽️' # soccer
    if '/dart' in text: return '🎯' # dart,dartboard
    if '/bowl' in text: return '🎳' # bowling
    return '🎲'

