import time
import datetime, math, os, json, psutil
from flask import Blueprint, request
from common.setlog2 import logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode, EnterpriseTransportProtolVer
from multiprocessing import Pool
import cantools
import numpy, binascii
import ujson

'''
版本10,11	A223_500KM_A点 A223_500KM_B点	ME7_TboxCAN_CMatrix_V307.201203_500km_SOP+2.dbc
版本12,13	A223_400KM 						ME7_TboxCAN_CMatrix_V305.201203_400km_SOP+2.dbc	
版本14，15	A226_500KM_A点，A226_500KM_B点	ME7_TboxCAN_CMatrix_V309.210409_500km_SOP+6_TBOX.DBC
版本16，17	A221_400KM A226_400KM			ME7_TboxCAN_CMatrix_V307.210409_400km_SOP+6_TBOX.DBC

V310版本是最终版，tbox发送的10----17，都可以用V310进行报文解析    2021/10/29 郭亮
'''

# candb = cantools.db.load_file('dbcfile/ME7_TboxCAN_CMatrix_V307.210409_400km_SOP+6_TBOX.DBC',cache_dir='./cache')
# candb_0a = cantools.db.load_file('dbcfile/ME7_TboxCAN_CMatrix_V309.210409_500km_SOP+6_TBOX.DBC', encoding='GBK')
candb_0a = cantools.db.load_file('dbcfile/ME7_TboxCAN_CMatrix_V310.210712_500km.dbc', encoding='GBK')
candb_0e = cantools.db.load_file('dbcfile/ME7_TboxCAN_CMatrix_V307.210409_400km_SOP+6_TBOX.DBC', encoding='GBK')
candb_ME7_310_500 = cantools.db.load_file('dbcfile/ME7_TboxCAN_CMatrix_V310.210712_500km.dbc', encoding='GBK')
candb_ME7_310_400 = cantools.db.load_file('dbcfile/ME7_TboxCAN_CMatrix_V310.210712_400km.dbc', encoding='GBK')
candb_ME5_00 = cantools.db.load_file('dbcfile/IC321_TboxCAN_CMatrix_V1.8.dbc', encoding='GBK')
candb_ME5_01 = cantools.db.load_file('dbcfile/IC321_TboxCAN_CMatrix_V3.0.dbc', encoding='GBK')

candbPool = {
    'ME7': {
        '0a': candb_ME7_310_500,
        '0b': candb_ME7_310_500,
        '0c': candb_ME7_310_400,
        '0d': candb_ME7_310_400,
        '0e': candb_ME7_310_500,
        '0f': candb_ME7_310_500,
        '10': candb_ME7_310_400,
        '11': candb_ME7_310_400
    },
    'ME5': {
        '00': candb_ME5_00,
        '01': candb_ME5_01
    }
}


def parse_tjms_message(data, vehicleMode='ME7', protocol='0e', signal=False):

    time1 = time.time() * 1000

    canDb = candbPool[vehicleMode][protocol]
    CanIDList = EnterpriseTransportProtolVer[vehicleMode][protocol]

    CanIDListLen = len(CanIDList)

    mainCursor = 0

    # canIDAmount = numpy.zeros(CanIDListLen, dtype=int)
    # canMessageOffset = numpy.zeros(CanIDListLen, dtype=int)

    canIDAmount = {}
    canMessageOffset = {}

    for y in range(CanIDListLen):
        canIDAmount[y] = int(data[mainCursor:mainCursor+2], 16)
        canMessageOffset[y] = mainCursor+2
        mainCursor = mainCursor + 2 + canIDAmount[y]*16

    if len(data) != mainCursor:
        logger.warning(f"企标秒包的解析错误，按照canid的顺序取走后有剩余字节")

    resp = {}

    for x in range(CanIDListLen):
        canMessageList = []
        signalMessageList = []
        for y in range(0, int(canIDAmount[x])):
            _canMessage = data[canMessageOffset[x] + y*16 : canMessageOffset[x] + y*16 + 16]
            canMessageList.append(_canMessage)

            if signal:
                canID = CanIDList[x]
                signalMessageList.append(canDb.decode_message(canID,binascii.unhexlify(_canMessage),0,1))

                resp[CanIDList[x]] = signalMessageList
            else:

                resp[CanIDList[x]] = canMessageList

    return resp


def cropmessage2dict(oriAllMessage, startTime, endTime):
    startTimeStr = Timeutils.timeStamp2timeString(startTime)
    endTimeStr = Timeutils.timeStamp2timeString(endTime)

    # 要求传入的dataList里，每行是一个Dict，并且Dict里要包含一个叫timestamp的key/value
    bufferCursor = 0

    # 找到需要处理的第一条
    find = False
    while bufferCursor < len(oriAllMessage):

        if oriAllMessage[bufferCursor].split(',')[0] < startTimeStr:
            # 跳过这条废数据
            bufferCursor += 1
        else:
            # print (f"First content {dataList[bufferCursor]} 找到了!")
            find = True
            break

    if find:
        logger.debug(f"找到了需要处理的第一条：content:{oriAllMessage[bufferCursor]}")
    else:
        logger.debug(f'最后一个文件读完了，啥也没有')
        return {}

    respMessage = {}

    while oriAllMessage[bufferCursor].split(',')[0] < endTimeStr:
        # respMessage.append(oriAllMessage[bufferCursor])

        # TODO 临时处理
        _vehicleMode = oriAllMessage[bufferCursor].split(',')[4] if oriAllMessage[bufferCursor].split(',')[4] != 'None' else 'ME7'

        respMessage[oriAllMessage[bufferCursor].split(',')[0]] = (oriAllMessage[bufferCursor].split(',')[2],
                                                                  oriAllMessage[bufferCursor].split(',')[3],
                                                                  _vehicleMode)

        # 如果buffer还没到底，就cursor+1
        if bufferCursor < (len(oriAllMessage) - 1):
            bufferCursor += 1
        else:
            break
    return respMessage



def cropmessage(oriAllMessage, startTime, endTime):
    startTimeStr = Timeutils.timeStamp2timeString(startTime)
    endTimeStr = Timeutils.timeStamp2timeString(endTime)

    # 要求传入的dataList里，每行是一个Dict，并且Dict里要包含一个叫timestamp的key/value
    bufferCursor = 0

    # 找到需要处理的第一条
    find = False
    while bufferCursor < len(oriAllMessage):

        if oriAllMessage[bufferCursor].split(',')[0] < startTimeStr:
            # 跳过这条废数据
            bufferCursor += 1
        else:
            # print (f"First content {dataList[bufferCursor]} 找到了!")
            find = True
            break

    if find:
        logger.debug(f"找到了需要处理的第一条：content:{oriAllMessage[bufferCursor]}")
    else:
        logger.debug(f'最后一个文件读完了，啥也没有')
        return {}

    respMessage = []

    while oriAllMessage[bufferCursor].split(',')[0] < endTimeStr:
        respMessage.append(oriAllMessage[bufferCursor])

        # 如果buffer还没到底，就cursor+1
        if bufferCursor < (len(oriAllMessage) - 1):
            bufferCursor += 1
        else:
            break
    return respMessage


def getPureContents(fullPathList):
    _combinedContents = []
    for fullPath in fullPathList:
        if os.path.exists(fullPath):
            step0 = time.time()*1000
            with open (fullPath, 'r') as f:
                _contents = f.readlines()
            logger.debug(f"getPureContents。读{fullPath}到内存, 花了 {time.time()*1000 - step0} ms")
            _combinedContents += _contents
        else:
            pass
    return _combinedContents

def getOriMessageList(fullPathList, readKeys):

    # info = psutil.virtual_memory()
    # print(f"内存使用：{psutil.Process(os.getpid()).memory_info().rss}")
    # print(f"总内存：{info.total}")
    # print(f"内存占比：{info.percent}")
    # print(f"cpu个数：{psutil.cpu_count()}")
    time1=time.time()*1000
    print(f"要读取这些文件：{fullPathList}")
    # 定义最后返回的List
    respContents = []

    if len(fullPathList) == 1:
        # 单个文件，用同步方式
        respContents = getOriMessageSingleFileAsync(fullPathList[0], readKeys)
        if not respContents:
            respContents=[]

    else:
        # 多个文件，异步方式工作
        readfileFunPools = Pool(7)
        asyncResult = []

        for _path in fullPathList:
            asyncResult.append(readfileFunPools.apply_async(getOriMessageSingleFileAsync, args=(_path, readKeys)))
        readfileFunPools.close()
        readfileFunPools.join()

        for res in asyncResult:
            _res = res.get()
            if _res:
                respContents = respContents + _res

    time2=time.time()*1000
    logger.info(f"本次读了{len(fullPathList)}个这样 {fullPathList[0]} 的文件，共耗时:{time2-time1}毫秒")
    return respContents


def getOriMessageSingleFileAsync(_path, readKeys):
    # 处理一个具体的文件
    if os.path.exists(_path):
        step0 = time.time()*1000
        logger.debug(f"OptStep1: getOriMessageSingleFileAsync要打开这个文件了。。。。{_path}")

        fileContents = []
        with open (_path, 'r') as f:
            _contents = f.readlines()

        logger.debug(f"OptStep2: 读{_path}到内存，花费到 {time.time()*1000-step0}毫秒")

        # timelineDict = transformer2TimelineDict(_contents)
        # logger.debug(f"OptStep3: 将文件内容转成timelineDict，花费到 {time.time()*1000-step0}毫秒")

        # 咱试试一开始就用字典，速度如何
        
        try:
            for _row in _contents:
                try:
                    _rowDict = ujson.loads(_row)
                except:
                    continue

                if readKeys[0] == '.':
                    fileContents.append(_row)
                else:
                    # 按照readKeys的定义，构造一行返回内容：newRow
                    newRow = ''
                    for key in readKeys:
                        # costValue = eval('_rowDict'+''.join([f"['{x}']" for x in key]))
                        # 临时复制costKey和costValue。这种方式居然比上面的eval方式快
                        costKey = key[:]
                        costValue = _rowDict.copy()
                        while costKey:
                            _subKey = costKey.pop(0)
                            costValue = costValue.get(_subKey)

                        # costValue是最后找到要输出的值.如果不是str，就强制转一下
                        if isinstance(costValue, str):
                            pass
                        else:
                            costValue = str(costValue)

                        newRow = newRow + ',' + costValue

                    # 一行的数据组织完了，写入fileContents
                    fileContents.append(newRow.strip(','))

            logger.debug(f"OptStep4: 解析{_path}内容到新的list，花费到 {time.time()*1000-step0}毫秒")
            

        except Exception as ex:
            logger.warning(ex)
            logger.error(f"文件解析内容错误：{_path}")
            return None
        
        return fileContents

    else:
        # logger.info(f"要读取的这个文件不存在:{_path}")
        return None

def transformer2TimelineDict(contents):
    _timelineDict = {}
    for _line in contents:
        _dict = ujson.loads(_line)
        if _dict.get('MCUTime') and _dict.get('contents'):
            _timelineDict[_dict['MCUTime']] = _dict['contents']
    return _timelineDict


def getFullPathList(vin, dateList, dataSources, env=None):
    # 通过环境变量找到主程序的名字
    env_dist = os.environ
    for key in env_dist:
        if key == 'HOLO_APPNAME':
            appname = env_dist[key]

    try:
        appname
    except NameError:
        logger.error("HOLO_APPNAME这个环境变量没有定义，检查入口程序的设置")

    # 2021/10/27 上线前修改：去掉这里对env的判断，用函数传入的env参数，如果没有传，则用原来的判断逻辑
    if not env:
        # 通过引入的CONFIG得到运行环境
        env = CONFIG['env']

    # 拼接完整path
    fullPathList = []
    for _item in dateList:
        _path = os.path.join(CONFIG[appname][env]['Storage_Prefix'], env, vin, _item, dataSources)
        fullPathList.append(_path)
    return fullPathList


def createDateList(Xaxis):
    firstDate = Xaxis[0][:10]
    lastDate = Xaxis[-1][:10]
    dateList = []
    while firstDate <= lastDate:
        dateList.append(firstDate)
        _firstDateTimestamp = Timeutils.timeString2timeStamp(firstDate, format='%Y-%m-%d')
        firstDate = Timeutils.timeStamp2timeString(_firstDateTimestamp + 86400, format='%Y-%m-%d')
    return dateList


def createDateListByDuration(startTimestamp, endTimestamp):
    Xaxis = [Timeutils.timeStamp2timeString(startTimestamp), Timeutils.timeStamp2timeString(endTimestamp)]
    return createDateList(Xaxis)


def getVINList():
    # 通过环境变量找到主程序的名字
    env_dist = os.environ
    for key in env_dist:
        if key == 'HOLO_APPNAME':
            appname = env_dist[key]

    try:
        appname
    except NameError:
        logger.error("HOLO_APPNAME这个环境变量没有定义，检查入口程序的设置")

    # 通过引入的CONFIG得到运行环境
    env = CONFIG['env']

    # 拼接出存在vin码dict的path
    path = os.path.join(CONFIG[appname][env]['Storage_Prefix'], env)
    files = os.listdir(path)

    vinList = []

    for file in files:
        if os.path.isdir(os.path.join(path,file)):
            if len(file) == 17:
                vinList.append(file)

    return vinList