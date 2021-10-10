# TGBOT.CHAT

##### Description:

a simple telegram chat bot working on both local hosts and Heroku, with database support, utilizing the `copyMessage` API which makes anonymous forwarding much simpler

##### Capabilities:

- [x] forward from other users / copy owner's message
- [x] switch chat context, support multiple chats
- [x] database support, with message_queue and message_history
- [x] reply_to
- [x] delete message sent by owner
- [x] delete message sent by peers
- [x] edit message for both owner and peers (not exactly the same behaviour as editMessage)
- [ ] dice/basketball/.../poll/quiz/... special message support
- [ ] scheduled notification when new messages arrive in another chat
- [ ] scheduled database cleanup (due to Heroku db free plan limit)

##### How to Use the Bot:

0. install dependencies in requirements.txt, setup db (only test in PostgreSQL)
1. create a `tgbot.json` configuration file, which includes `masterid`,`masterchatid`,`token` keys with values
2. export environment variables (check devrun): 
   1. `DATABASE_SSL=0` (optional) db does not require SSL
   2. `DATABASE_URL='postgresql://localhost/tgbot'` (mandatory) database url, which has already been given when running on Heroku
   3. `tgbotdev=1` (optional) bot is run on local host, using long poll; otherwise on Heroku using webhook 
   4. `dbverbose=1` (optional) turn on echo
3. run `bot.py`

##### Other Functions:

- `/start` to check if the user is the master(owner) of the bot
- `/getme` to check the user's basic information including uid and fullname

