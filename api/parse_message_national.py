# 载入工具包
import time, _thread
from multiprocessing import Pool

import service.gb32960.tj_gb32960 as tj

import time


def tjgb(data):
    dd = tj.parse_tj_ns_message(data)
    return dd.dict


time1 = time.time()*1000
resp = []


a=["1507010e01020102010103e8ffffffffffffffffff020f03e8000002010103ffffffffffffffffffff080101ffffffff00a80001a8ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff090101000effffffffffffffffffffffffffff060101ffff0101ffff0101ff0101ff0501000000000000000007000000000000000000"]

p = Pool(6)
for item in a:
    res = p.apply_async(tjgb,args=(item,))
    resp.append(res)
print("done!!!!!!!")
for res in resp:
    print(res.get())

time2 = time.time()*1000
print (time2-time1)