# 启动时调用，解析dbc到几个dict，写入redis。
# 写入时注意增加失效时间
# 提供其他函数的读接口。如果redis已经失效，则先写入redis。再返回函数调用结果
# 提供管理端主动更新redis的dict的接口

import redis, time, cantools, json, ujson

class RedisPool(object):
    def __init__(self):
        self.pool = redis.ConnectionPool(host='192.168.0.239', port=6379, db=10, password='Dearccbj2018@02', decode_responses=True)
        # self.pool = redis.ConnectionPool(host='192.168.10.89', port=6379, db=10, password='Dearcc2021_11!',  decode_responses=True)
        self.r = redis.Redis(connection_pool=self.pool)

class DBCCache(RedisPool):
    candb_ME7_310_500 = cantools.db.load_file('../dbcfile/ME7_TboxCAN_CMatrix_V310.210712_500km.dbc', encoding='GBK')
    # candb_ME7_310_400 = cantools.db.load_file('dbcfile/ME7_TboxCAN_CMatrix_V310.210712_400km.dbc', encoding='GBK')
    # candb_ME5_00 = cantools.db.load_file('dbcfile/IC321_TboxCAN_CMatrix_V1.8.dbc', encoding='GBK')
    candb_ME5_01 = cantools.db.load_file('../dbcfile/IC321_TboxCAN_CMatrix_V3.0.dbc', encoding='GBK')
    candbPool = {
        'ME7': {
            # '0a': candb_ME7_310_500,
            # '0b': candb_ME7_310_500,
            # '0c': candb_ME7_310_400,
            # '0d': candb_ME7_310_400,
            # '0e': candb_ME7_310_500,
            # '0f': candb_ME7_310_500,
            # '10': candb_ME7_310_400,
            # '11': candb_ME7_310_400,
            'last': candb_ME7_310_500
        },
        'ME5': {
            # '00': candb_ME5_00,
            # '01': candb_ME5_01,
            'last': candb_ME5_01
        }
    }

    def __init__(self):
        super().__init__()



    def genMessagesSignals(self, canDB):
        respDict = {}
        messages = canDB.messages
        for _msg in messages:
            signals = _msg.signals
            respDict[_msg.name] = [_sig.name for _sig in signals]
        return respDict


    def uploadToRedis(self):
        me7_fullCanDBDict = self.genMessagesSignals(self.candbPool['ME7']['last'])
        me5_fullCanDBDict = self.genMessagesSignals(self.candbPool['ME5']['last'])
        self.r.hset('ME7', 'fullCanDBDict', json.dumps(me7_fullCanDBDict))
        self.r.hset('ME5', 'fullCanDBDict', json.dumps(me5_fullCanDBDict))


    def downloadFromRedis(self, vehicleMode):
        return self.r.hget(vehicleMode, 'fullCanDBDict')


if __name__ == '__main__':
    pool = redis.ConnectionPool(host='192.168.10.89', port=6379, db=10, password='Dearcc2021_11!',  decode_responses=True)
    r = redis.Redis(connection_pool=pool)


    a=DBCCache()
    time0 = time.time()
    a.uploadToRedis()
    print (f"upload to redis. spent me {time.time() - time0 } s")

    time0 = time.time()
    a.downloadFromRedis('ME7')
    print (f"download. spent me {time.time() - time0 } s")

    time0 = time.time()
    b=DBCCache()
    b.downloadFromRedis('ME7')
    print (f"download. spent me {time.time() - time0 } s")


    time0 = time.time()

    _str = r.hget('ME7', 'fullCanDBDict')
    _json = json.loads(_str)
    print (f"download. spent me {time.time() - time0 } s")