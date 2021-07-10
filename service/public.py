import time
import datetime, math, os, json, psutil
from flask import Blueprint, request
from common.setlog2 import logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode, EnterpriseTransportProtolVer
from multiprocessing import Pool
import cantools
import numpy, binascii

# candb = cantools.db.load_file('dbcfile/ME7_TboxCAN_CMatrix_V307.210409_400km_SOP+6_TBOX.DBC',cache_dir='./cache')
candb_0e = cantools.db.load_file('dbcfile/ME7_TboxCAN_CMatrix_V307.210409_400km_SOP+6_TBOX.DBC')
candbPool = {
    'ME7': {
        '0e': candb_0e,
        '0f': candb_0e
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
                signalMessageList.append(canDb.decode_message(canID,binascii.unhexlify(_canMessage),0,0))

                resp[CanIDList[x]] = signalMessageList
            else:

                resp[CanIDList[x]] = canMessageList

    print(resp)
    return resp


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



def getOriMessageList(fullPathList, readKeys):

    # info = psutil.virtual_memory()
    # print(f"内存使用：{psutil.Process(os.getpid()).memory_info().rss}")
    # print(f"总内存：{info.total}")
    # print(f"内存占比：{info.percent}")
    # print(f"cpu个数：{psutil.cpu_count()}")
    time1=time.time()*1000

    # 定义最后返回的List
    respContents = []

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
        # logger.debug(f"要打开这个文件了。。。。{_path}")

        fileContents = []
        with open (_path, 'r') as f:
            _contents = f.readlines()

        # logger.debug(f"读{_path}到内存，花费到 {time.time()*1000-step0}毫秒")
        try:
            for _row in _contents:
                try:
                    _rowDict = json.loads(_row)
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

            # logger.debug(f"解析{_path}内容到新的list，花费到 {time.time()*1000-step0}毫秒")

        except Exception as ex:
            logger.warning(ex)
            logger.error(f"文件解析内容错误：{_path}")
            return None

        return fileContents

    else:
        # logger.info(f"要读取的这个文件不存在:{_path}")
        return None



def getFullPathList(vin, dateList, dataSources):
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

    # 拼接完整path
    fullPathList = []
    for _item in dateList:
        _path = os.path.join(CONFIG[appname][env]['Storage_Prefix'], env, vin, _item, dataSources)
        fullPathList.append(_path)
    return fullPathList


def createDateList(Xaxis):
    # pathTotal: 涉及到的文件的数量
    workingStartTime = Timeutils.timeString2timeArray(Xaxis[0])
    workingEndTime = Timeutils.timeString2timeArray(Xaxis[-1])

    pathTotal = workingEndTime.toordinal() - workingStartTime.toordinal() + 1
    fullDateList = []
    pathCursor = workingStartTime
    while pathTotal:
        _path = str(pathCursor.year) + '-' + str(pathCursor.month).zfill(2) + '-' + str(pathCursor.day).zfill(2)
        fullDateList.append(_path)
        pathCursor = pathCursor + datetime.timedelta(days=1)
        pathTotal -= 1

    return fullDateList


def createDateListByDuration(startTimestamp, endTimestamp):
    # pathTotal: 涉及到的文件的数量
    Xaxis = [Timeutils.timeStamp2timeString(startTimestamp), Timeutils.timeStamp2timeString(endTimestamp)]
    return createDateList(Xaxis)
