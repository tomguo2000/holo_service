# 启动时调用，解析dbc到几个dict，写入redis。
# 写入时注意增加失效时间
# 提供其他函数的读接口。如果redis已经失效，则先写入redis。再返回函数调用结果
# 提供管理端主动更新redis的dict的接口

import redis, time, cantools, json, ujson
from common.config import CONFIG, env


class RedisDBConfig:
    HOST = CONFIG['redis_setting'][env]['server']
    PORT = CONFIG['redis_setting'][env]['port']
    DBID = 10
    PASSWORD = CONFIG['redis_setting'][env]['password']


def operator_status(func):
    '''''get operatoration status
    '''
    def gen_status(*args, **kwargs):
        error, result = None, None
        try:
            result = func(*args, **kwargs)
        except Exception as e:
            error = str(e)
        return {'result': result, 'error': error}
    return gen_status


class CacheDBCService(object):
    def __init__(self):
        if not hasattr(CacheDBCService, 'pool'):
            CacheDBCService.create_pool()
        self._connection = redis.Redis(connection_pool = CacheDBCService.pool)

    @staticmethod
    def create_pool():
        CacheDBCService.pool = redis.ConnectionPool(
            host = RedisDBConfig.HOST,
            port = RedisDBConfig.PORT,
            db  = RedisDBConfig.DBID,
            password = RedisDBConfig.PASSWORD)

    @operator_status
    def set_data(self, key, value):
        '''''set data with (key, value)
        '''
        return self._connection.set(key, value)

    @operator_status
    def get_data(self, key):
        '''''get data by key
        '''
        return self._connection.get(key)

    @operator_status
    def del_data(self, key):
        '''''delete cache by key
        '''
        return self._connection.delete(key)


    def uploadDBCDict(self):
        candb_ME7_310_500 = cantools.db.load_file('dbcfile/ME7_TboxCAN_CMatrix_V310.210712_500km.dbc', encoding='GBK')
        candb_ME5_01 = cantools.db.load_file('dbcfile/IC321_TboxCAN_CMatrix_V3.0.dbc', encoding='GBK')
        self._connection.hset('ME7', 'fullCanDBDict', json.dumps(self.genMessagesSignals(candb_ME7_310_500)))
        self._connection.hset('ME5', 'fullCanDBDict', json.dumps(self.genMessagesSignals(candb_ME5_01)))
        return True


    def downloadFromRedis(self, vehicleMode):
        return self._connection.hget(vehicleMode, 'fullCanDBDict')


    def genMessagesSignals(self, canDB):
        respDict = {}
        messages = canDB.messages
        for _msg in messages:
            signals = _msg.signals
            respDict[_msg.name] = [_sig.name for _sig in signals]
        return respDict



if __name__ == '__main__':

    time0 = time.time()
    print(CacheDBCService().uploadDBCDict())
    print (f"uploaded all dict. spent me {time.time() - time0 } s")

    time0 = time.time()
    print(CacheDBCService().downloadFromRedis('ME7'))
    print (f"download. spent me {time.time() - time0 } s")
