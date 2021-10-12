import os
import json
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

import telegram as t
import telegram.ext as te
from tlib.errc import * # error code
from tlib.botmethods import * # handlers
import tlib.db as db # database

# read config file
with open('tgbot.json','r') as f:
    botconfig_s = f.read()
j = json.JSONDecoder()
botconfig_temp = j.decode(botconfig_s)

# init tlib botconfig, can be used else where
initbotconf(botconfig_temp)

# get dev/heroku env from env vars, to switch between socks5/webhook
# isdev==True: local run, with proxy
# isdev==False: heroku run, check webhook_url_base
isdev = os.environ.get("tgbotdev","0")
if isdev!='0': isdev = True
else: isdev = False

uargs = {}
if isdev:
    proxyurl = botconfig.get('proxy_url',False)
    if proxyurl: uargs['proxy_url'] = proxyurl
    # pp = t.utils.request.Request(proxy_url='socks5://127.0.0.1:1080')
else:
    webhook_url_base = botconfig.get('webhook_url_base','x')
    if webhook_url_base=='x':
        exit(E_NOWEBHOOK)
    botconfig['webhook_url'] = webhook_url_base + '/' + botconfig['token']


# initialize bot
# bot = telegram.Bot(token=my_token, request=pp)
updater = te.Updater(token=botconfig['token'], request_kwargs=uargs)


# setup SQL, postgresql url should be modified explicitly due to psycopg2/sqlalchemy updates
dbconf = {}
dburl = os.environ.get('DATABASE_URL')
sidx = dburl.find(':') # postgresql://xxx
dbtype = dburl[:sidx]
if dbtype=='postgres': dbtype = 'postgresql'
dbconf['db_url'] = dbtype+r'+psycopg2'+dburl[sidx:]
sslmode = os.environ.get('DATABASE_SSL')
sslmode = False if sslmode=='0' else True
dbconf['sslmode'] = sslmode
dbconf['dbverbose'] = os.environ.get('dbverbose','0')
db.initdb(dbconf)


# register handlers in dispatcher

# basic info handlers
updater.dispatcher.add_handler(
    te.CommandHandler('start',startHandler)
)
updater.dispatcher.add_handler(
    te.CommandHandler('getme',userinfoHandler)
)

# switch handler
updater.dispatcher.add_handler(
    te.MessageHandler(
        te.Filters.chat(botconfig['masterchatid']) & \
        te.Filters.user(botconfig['masterid']) & \
        te.Filters.regex(r'^/s(w(itch)?)?\s*(@[0-9a-zA-Z_]*[Bb][Oo][Tt])?\s*$'),
        switchHandler
    )
)
# delete handler
updater.dispatcher.add_handler(
    te.MessageHandler(
        te.Filters.chat(botconfig['masterchatid']) & \
        te.Filters.user(botconfig['masterid']) & \
        te.Filters.regex(r'^/d(e(lete)?)?\s*(@[0-9a-zA-Z_]*[Bb][Oo][Tt])?\s*$'),
        deleteHandler
    )
)
# dice handler
# slot basketball soccer football dart bowl
updater.dispatcher.add_handler(
    te.MessageHandler(
        te.Filters.chat(botconfig['masterchatid']) & \
        te.Filters.user(botconfig['masterid']) & \
        te.Filters.regex(
            r'^/(dice)|(slot(machine)?)|(basket(ball)?)|(soccer)|'+
            r'(football)|(dart(board)?)|(bowl(ing)?)$'
        ),
        diceMasterHandler
    )
)
# switch callback handler
updater.dispatcher.add_handler(
    te.CallbackQueryHandler(
        switchCallbackHandler
    )
)
# master chat handler
updater.dispatcher.add_handler(
    te.MessageHandler(
        te.Filters.chat(botconfig['masterchatid']) & \
            te.Filters.user(botconfig['masterid']),
        receiveMasterHandler
    )
)
# slave chat handler
updater.dispatcher.add_handler(
    te.MessageHandler(
        (~te.Filters.chat(int(botconfig['masterchatid']))) & \
            (~te.Filters.user(botconfig['masterid'])),
        receiveHandler
    )
)

# Long Polling
if isdev:
    updater.start_polling()
else:
# Webhook
    port = int(os.environ.get('PORT', '8443')) # provided by heroku
    updater.start_webhook(
        listen='0.0.0.0', port=port,
        url_path=botconfig['token'],
        webhook_url=botconfig['webhook_url']
    )

updater.idle()

logger.info('bot terminated')
