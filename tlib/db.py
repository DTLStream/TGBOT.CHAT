import sqlalchemy as sql
import sqlalchemy.orm as orm
import enum
from .errc import *
import logging
logger = logging.getLogger()

dbconfig = {}

BASE = orm.declarative_base()

class BOTSTATE(BASE):
    __tablename__ = 'BOTSTATE'
    s_k = sql.Column('s_k',sql.String(255),primary_key=True)
    s_v = sql.Column('s_v',sql.String(255),nullable=False)
    def __repr__(self):
        return 'BOTSTATE s_k->s_v: ({})->({})'.format(self.s_k,self.s_v)

class CHTYPE(enum.Enum):
    private = 'private'
    group = 'group'
    supergroup = 'supergroup'
    channel = 'channel'
class CHAT(BASE):
    __tablename__ = 'CHAT'
    __table_args__ = (
        sql.PrimaryKeyConstraint('ch_id',name='c_pk'),
    )
    ch_id = sql.Column('ch_id',sql.BigInteger)
    ch_name = sql.Column('ch_name',sql.String(255))
    ch_type = sql.Column('ch_type',sql.Enum(CHTYPE))
    def __repr__(self):
        return 'CHAT ch_id:{}, ch_name:{}, ch_type:{}'.\
            format(self.ch_id,self.ch_name,self.ch_type)

class MESSAGE(BASE):
    __tablename__ = 'MESSAGE'
    __table_args__ = (
        sql.PrimaryKeyConstraint('ch_id','msg_id',name='m_pk'),
        sql.ForeignKeyConstraint(['ch_id'],['CHAT.ch_id']),
    )
    ch_id = sql.Column('ch_id',sql.BigInteger)
    msg_id = sql.Column('msg_id',sql.BigInteger)
    content = sql.Column('content',sql.LargeBinary)
    compressed = sql.Column('compressed',sql.Boolean)
    timestamp = sql.Column('timestamp',sql.BigInteger)

class MSGDIR(enum.Enum):
    m2s = 'm2s'
    s2m = 's2m'
class MESSAGE_MAP(BASE):
    __tablename__ = 'MESSAGE_MAP'
    __table_args__ = (
        sql.PrimaryKeyConstraint('m_ch_id','m_msg_id','s_ch_id','s_msg_id',name='mm_pk'),
        sql.ForeignKeyConstraint(['m_ch_id','m_msg_id'],['MESSAGE.ch_id','MESSAGE.msg_id'],\
            ondelete='cascade'),
        sql.ForeignKeyConstraint(['s_ch_id','s_msg_id'],['MESSAGE.ch_id','MESSAGE.msg_id'],\
            ondelete='cascade')
    )
    m_ch_id = sql.Column('m_ch_id',sql.BigInteger)
    m_msg_id = sql.Column('m_msg_id',sql.BigInteger)
    s_ch_id = sql.Column('s_ch_id',sql.BigInteger)
    s_msg_id = sql.Column('s_msg_id',sql.BigInteger)
    direction = sql.Column('direction',sql.Enum(MSGDIR),nullable=False)
    timestamp = sql.Column('timestamp',sql.BigInteger)

# unnecessary
# class MESSAGE_HISTORY(BASE):
#     __tablename__ = 'MESSAGE_HISTORY'
#     __table_args__ = (
#         sql.PrimaryKeyConstraint('ch_id','msg_id',name='mh_pk'),
#         sql.ForeignKeyConstraint(['ch_id','msg_id'],['MESSAGE.ch_id','MESSAGE.msg_id'])
#     )
#     ch_id = sql.Column('ch_id',sql.BigInteger)
#     msg_id = sql.Column('msg_id',sql.BigInteger)
#     timestamp = sql.Column('timestamp',sql.BigInteger)

class MESSAGE_QUEUE(BASE):
    __tablename__ = 'MESSAGE_QUEUE'
    __table_args__ = (
        sql.PrimaryKeyConstraint('ch_id','msg_id',name='mq_pk'),
        sql.ForeignKeyConstraint(['ch_id','msg_id'],['MESSAGE.ch_id','MESSAGE.msg_id'],\
            ondelete='cascade')
    )
    ch_id = sql.Column('ch_id',sql.BigInteger)
    msg_id = sql.Column('msg_id',sql.BigInteger)
    timestamp = sql.Column('timestamp',sql.BigInteger)

class OLD_MESSAGE_BUCKET(BASE):
    __tablename__ = 'OLD_MESSAGE_BUCKET'
    entity_id = sql.Column('entity_id',sql.BigInteger, primary_key=True, autoincrement=True)
    ch_id = sql.Column('ch_id',sql.BigInteger)
    msg_id = sql.Column('msg_id',sql.BigInteger)
    content = sql.Column('content',sql.LargeBinary)
    compressed = sql.Column('compressed',sql.Boolean)
    timestamp = sql.Column('timestamp',sql.BigInteger)

def initdb(dbconf):
    dbconfig.update(dbconf) # db_url: sqlalchemy url
    if dbconfig.get('db_url',False)==False:
        logger.error('db_url [{}] error')
        exit(E_DBURL)
    dbconfig['engine'] = sql.create_engine(
        dbconfig['db_url'],
        connect_args={
            'sslmode': 'require' if dbconfig['sslmode'] else 'prefer'
        },
        echo=(True if dbconf['dbverbose']!='0' else False)
    )
    e = dbconfig['engine']
    # create a Session and save in dbconfig
    sess = orm.sessionmaker(e)
    dbconfig['session'] = sess
    # check tables existence
    logger.info('checking whether tables exist')
    all_tables_created = True
    inspector = sql.inspect(e)
    if not inspector.has_table('BOTSTATE'):
        logger.info('BOTSTATE table not exists')
        all_tables_created = False
    if not inspector.has_table('CHAT'):
        logger.info('CHAT table not exists')
        all_tables_created = False
    if not inspector.has_table('MESSAGE'):
        logger.info('MESSAGE table not exists')
        all_tables_created = False
    if not inspector.has_table('MESSAGE_MAP'):
        logger.info('MESSAGE_MAP table not exists')
        all_tables_created = False
    # unnecessary
    # if not inspector.has_table('MESSAGE_HISTORY'):
    #     logger.info('MESSAGE_HISTORY table not exists')
    #     all_tables_created = False
    if not inspector.has_table('MESSAGE_QUEUE'):
        logger.info('MESSAGE_QUEUE table not exists')
        all_tables_created = False
    if not inspector.has_table('OLD_MESSAGE_BUCKET'):
        logger.info('OLD_MESSAGE_BUCKET table not exists')
        all_tables_created = False
    if not all_tables_created:
        BASE.metadata.create_all(e)
        logger.info('tables created')
    else:
        logger.info('tables have already been created')

