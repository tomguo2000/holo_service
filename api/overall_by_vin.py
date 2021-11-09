import time
import datetime, math, os, json, psutil
from flask import Blueprint, request
from common.setlog2 import logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode
from multiprocessing import Pool
import service.public
import ujson
import orjson

overall_by_vin = Blueprint("overall_by_vin", __name__)


@overall_by_vin.route('/vin', methods=["GET"])
def overall_by_vin_vin():
    try:

        # 检查入参
        try:
            params = request.args.to_dict()
            vin = params['vin']
            startTime = int(params['startTime']) if int(params['startTime']) > 9999999999 else int(params['startTime'])*1000
            endTime = int(params['endTime']) if int(params['endTime']) > 9999999999 else int(params['endTime'])*1000
            interval = int(params['interval'])  # 单位：分钟

        except:
            raise Exception ("110900")

        # 检查数据量.如果超过1000个格的查询范围，抛异常不与执行
        MaxRange = 1000
        ss = ((endTime-startTime) / 1000 / (interval*60))
        if (endTime-startTime) / 1000 / (interval*60) > MaxRange:
            raise Exception ("110901")
        if interval < 1 or interval > 60:
            raise Exception ("110901")

        time0 = time.time()*1000
        logger.debug(f"hhhh开始。。。。。。{time0}")



        # 构建一个X轴
        time1 = time.time()*1000
        Xaxis = createXaxis(vin, startTime, endTime, interval)
        logger.debug(f"hhhh构建一个X轴完毕。。。{time.time()*1000-time1}")


        # 根据X轴，得到日期List，用于拼接数据源的path
        time1 = time.time()*1000
        dateList = service.public.createDateList(Xaxis)
        logger.debug(f"hhhh得到日期List完毕。。。{time.time()*1000-time1}")

        # 传入X轴和dateList，获取国标的string结果
        # tj32960 = getTJ32960Details(vin, Xaxis, dateList)

        # 传入X轴和dateList，获取国标的报文条数结果
        time1 = time.time()*1000
        message_tj32960List = getTJ32960(vin, Xaxis, dateList)
        logger.debug(f"hhhh获取国标的报文条数结果完毕。。。{time.time()*1000-time1}")


        # 传入X轴和dateList，获取企标的聚合结果
        time1 = time.time()*1000
        message_MSList = getMS(vin, Xaxis, dateList)
        logger.debug(f"hhhh获取企标的聚合结果完毕。。。{time.time()*1000-time1}")



        # 传入X轴和dateList，获取MISC的聚合结果
        time1 = time.time()*1000
        message_MiscList = getMisc(vin, Xaxis, dateList)
        logger.debug(f"hhhh获取MISC的聚合结果完毕。。。{time.time()*1000-time1}")



        # 传入X轴和dateList，获取登入登出心跳的聚合结果
        time1 = time.time()*1000
        message_HeartbeatList = getHeartbeat(vin, Xaxis, dateList)
        logger.debug(f"hhhh获取登入登出心跳的聚合结果完毕。。。{time.time()*1000-time1}")



        # 传入X轴和dateList，获取SDK初始化的聚合结果
        time1 = time.time()*1000
        event_VehicleLoginList = getVehicleLoginEvents(vin, Xaxis, dateList)
        logger.debug(f"hhhh获取SDK初始化的聚合结果 完毕。。。{time.time()*1000-time1}")



        # 传入X轴和dateList，获取控车event的结果
        time1 = time.time()*1000
        event_RemoteCmdList = getRemoteCmdEvents(vin, Xaxis, dateList)
        logger.debug(f"hhhh获取控车event的结果 完毕。。。{time.time()*1000-time1}")



        # 传入X轴和dateList，获取emq连接的event结果
        time1 = time.time()*1000
        event_ConnStatusList = getConnStatus(vin, Xaxis, dateList)
        logger.debug(f"hhhh获取emq连接的event结果 完毕。。。{time.time()*1000-time1}")


        time1 = time.time()*1000
        resp = {}
        resp['Xaxis'] = Xaxis
        resp['dateList'] = dateList
        resp['message_tj32960List'] = message_tj32960List
        resp['message_MSList'] = message_MSList
        resp['message_MiscList'] = message_MiscList
        resp['message_HeartbeatList'] = message_HeartbeatList
        resp['event_VehicleLoginList'] = event_VehicleLoginList
        resp['event_RemoteCmdList'] = event_RemoteCmdList
        resp['event_ConnStatusList'] = event_ConnStatusList

        logger.debug(f"hhhh组织resp 完毕。。。{time.time()*1000-time1}")

        resp = makeResponse(resp)

        return {
                   "code": 200,
                   "message": None,
                   "businessObj": resp
               }, 200
    except Exception as ex:
        return {
                   "code": ex.args[0],
                   "message": ReturnCode[ex.args[0]],
                   "businessObj": None
               }, 200


def createXaxis(vin, startTimeStamp, endTimeStamp, interval):
    # 秒清0，调整startTimeStamp向上到workingStartTimeStamp
    _tempTimeArray = Timeutils.timeStamp2timeArray(startTimeStamp)
    _delta = datetime.timedelta(seconds=_tempTimeArray.second, microseconds=_tempTimeArray.microsecond)
    workingStartTime = _tempTimeArray - _delta

    # workingStartTimeStamp是否被3整除，如不能就分钟减1再试。
    while workingStartTime.minute % interval != 0:
        workingStartTime = workingStartTime - datetime.timedelta(minutes=1)
    workingStartTimeStamp = Timeutils.timeArray2timeStamp(workingStartTime, ms=True)

    # 找到恰当的workingEndTimeStamp
    _tempTimeArray = Timeutils.timeStamp2timeArray(endTimeStamp)
    _delta = datetime.timedelta(seconds=60 - _tempTimeArray.second, microseconds=-_tempTimeArray.microsecond)
    workingEndTime = _tempTimeArray + _delta
    while workingEndTime.minute % interval != 0:
        workingEndTime = workingEndTime + datetime.timedelta(minutes=1)
    workingEndTimeStamp = Timeutils.timeArray2timeStamp(workingEndTime, ms=True)

    # xAxisTotal: X轴的点位数量
    xAxisTotal = math.ceil((workingEndTimeStamp - workingStartTimeStamp) / (interval * 60 * 1000))

    # respXaxis: 用于绘图的X轴的坐标list
    respXaxis = []
    timeCursor = workingStartTimeStamp
    while xAxisTotal:
        respXaxis.append(Timeutils.timeStamp2timeString(timeCursor))
        timeCursor += interval * 60 * 1000
        xAxisTotal -= 1

    return respXaxis


def getTJ32960(vin, Xaxis, dateList):
    # 天际国标的报文文件名
    dataSourcesLive = 'message_national_live.txt'
    # 天际国标补发报文文件名
    dataSourcesResent = 'message_national_resent.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSourcesLive)
    fullPathList2 = service.public.getFullPathList(vin, dateList, dataSourcesResent)

    # readKeys = [['MPUTime'], ['TYPE_CMD']]
    readKeys = [['MPUTime'], ['TYPE_CMD']]
    # 获取内容
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)
    oriMessageListResent = service.public.getOriMessageList(fullPathList2, readKeys)
    # 组合实发和补发报文
    oriAllMessage = oriMessageList + oriMessageListResent

    # 假设实发和补发没有重复的部分。不需要进行去重的动作

    Y32960 = assignAmount2TimeSlot(Xaxis, oriAllMessage, needSort=True)
    return Y32960


def getMS(vin, Xaxis, dateList):
    time0 = time.time()*1000

    # 天际企标的报文文件名
    dataSourcesLive = 'message_enterprise_live.txt'
    dataSourcesResent = 'message_enterprise_resent.txt'
    dataSourcesWarning = 'message_enterprise_warning.txt'


    # 获取需要读取的文件列表
    time1 = time.time()*1000
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSourcesLive)
    fullPathList2 = service.public.getFullPathList(vin, dateList, dataSourcesResent)
    fullPathList3 = service.public.getFullPathList(vin, dateList, dataSourcesWarning)


    # 读取必要的message
    time1 = time.time()*1000
    readKeys = [['MPUTime'], ['TYPE_CMD']]
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)
    oriMessageListResent = service.public.getOriMessageList(fullPathList2, readKeys)
    oriMessageListWarning = service.public.getOriMessageList(fullPathList3, readKeys)


    # 组合实发,补发,报警报文
    time1 = time.time()*1000
    oriAllMessage = oriMessageList + oriMessageListResent + oriMessageListWarning


    # TODO 排序及处理实发补发故障之间的重复报文


    # 把企标分发到Y轴
    time1 = time.time()*1000
    YMSdict = assignAmount2TimeSlot(Xaxis, oriAllMessage, needSort=True)

    return YMSdict


def getMisc(vin, Xaxis, dateList):
    # 天际的报文文件名
    dataSources = 'message_misc.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources)

    readKeys = [['MPUTime'], ['TYPE_CMD']]
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    YMisc = assignAmount2TimeSlot(Xaxis, oriMessageList)

    return YMisc


def getHeartbeat(vin, Xaxis, dateList):
    # 天际的报文文件名
    dataSources = 'message_hearbeat.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources)

    readKeys = [['MPUTime'], ['TYPE_CMD']]
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    YHeartbeat = assignAmount2TimeSlot(Xaxis, oriMessageList)

    return YHeartbeat


def getVehicleLoginEvents(vin, Xaxis, dateList):
    # 天际的报文文件名
    dataSources = 'event_vehicle.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources)

    # readKeys = [['timestamp'], ['event']]
    readKeys = ['.']
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    # readKeys用['.']的时候，通畅需要对返回的List做二次加工
    respMessageList = []
    for _item in oriMessageList:
        _temp = orjson.loads(_item)

        if _temp['timestamp'] < 9999999999:
            _temp['timestamp'] = _temp['timestamp'] * 1000

        respMessageList.append({'timestamp': _temp['timestamp'],
                                'timestr': Timeutils.timeStamp2timeString(_temp['timestamp']),
                                'event': _temp['event']})

    YHeartbeat = assignArray2TimeSlot(Xaxis, respMessageList)

    return YHeartbeat


def getRemoteCmdEvents(vin, Xaxis, dateList):
    # 天际的报文文件名
    dataSources = 'event_vehicle.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources)

    # readKeys = [['timestamp'], ['event']]
    readKeys = ['.']
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    # readKeys用['.']的时候，通畅需要对返回的List做二次加工
    respMessageList = []
    for _item in oriMessageList:
        _temp = orjson.loads(_item)

        if _temp['timestamp'] < 9999999999:
            _temp['timestamp'] = _temp['timestamp'] * 1000

        respMessageList.append({'timestamp': _temp['timestamp'],
                                'timestr': Timeutils.timeStamp2timeString(_temp['timestamp']),
                                'event': _temp['event']})

    YHeartbeat = assignArray2TimeSlot(Xaxis, respMessageList)

    return YHeartbeat


def getConnStatus(vin, Xaxis, dateList):
    previousConnStatus = getPreviousConnStatus(vin, dateList[0])

    dataSource = 'event_emq_conn.txt'
    fullPathList = service.public.getFullPathList(vin, dateList, dataSource)

    respMessageList = []
    respMessageList.append({'timestamp': Timeutils.timeString2timeStamp(Xaxis[0], ms=True),
                            'timestr': Xaxis[0],
                            'event': previousConnStatus['event']})


    readKeys = ['.']
    oriMessageList = service.public.getOriMessageList(fullPathList, readKeys)

    # readKeys用['.']的时候，通畅需要对返回的List做二次加工
    for _item in oriMessageList:
        _temp = orjson.loads(_item)

        if _temp['timestamp'] < 9999999999:
            _temp['timestamp'] = _temp['timestamp'] * 1000

        respMessageList.append({'timestamp': _temp['timestamp'],
                                'timestr': Timeutils.timeStamp2timeString(_temp['timestamp']),
                                'event': _temp['event']})

    YConnList = assignArray2TimeSlot(Xaxis, respMessageList)

    return YConnList


def getPreviousConnStatus(vin, date):
    dataSource = 'event_emq_conn.txt'
    startDateStr = Timeutils.timeArray2timeString(Timeutils.timeString2timeArray(date, format="%Y-%m-%d")
                                                  + datetime.timedelta(days=-1), format="%Y-%m-%d")
    while True:
        fullPathList = service.public.getFullPathList(vin=vin,
                                                      dateList=[startDateStr],
                                                      dataSources=dataSource)
        if os.path.exists(fullPathList[0]) or startDateStr < '2021-06-01':
            break
        else:
            startDateStr = Timeutils.timeArray2timeString(Timeutils.timeString2timeArray(startDateStr, format="%Y-%m-%d")
                                                          + datetime.timedelta(days=-1), format="%Y-%m-%d")

    _fullPath = fullPathList[0]
    if os.path.split(os.path.split(_fullPath)[0])[1] == '2021-05-31':
        previousConnStatus = {'event': 'unknown'}
    else:
        connStatusList = service.public.getOriMessageList([_fullPath], ['.'])
        connEvent = orjson.loads(connStatusList[-1])
        previousConnStatus = {'event': connEvent.get('event')}

    return previousConnStatus


# 按照Xaxis的刻度，把没有值的刻度填充无效值
def makeResponse(resp):
    makeResp = {}
    makeResp['Xaxis'] = resp['Xaxis']
    makeResp['dateList'] = resp['dateList']
    del(resp['Xaxis'])
    del(resp['dateList'])

    for _item in resp:
        _itemResp = []

        try:
            if isinstance(next(iter(resp[_item].values())), int):
                type = 'int'
            else:
                type = 'unknown'
        except:
            type = 'unknown'


        # 遍历每个刻度
        for _x in makeResp['Xaxis']:

            # 取出_item这个分类下，每个刻度对应的实际值
            _value = resp[_item].get(_x)

            if _value:
                _itemResp.append(_value)
            elif type == 'int':
                _itemResp.append(0)
            else:
                _itemResp.append([])

        makeResp[_item] = _itemResp

    return makeResp


def getTJ32960Details(vin, Xaxis, dateList):
    # 天际国标的报文文件名
    dataSourcesLive = 'message_national_live.txt'

    # 获取需要读取的文件列表
    fullPathList = service.public.getFullPathList(vin, dateList, dataSourcesLive)

    # 定义一下要读数据源的哪些节点和顺序.每行的一个key用一个[]来描述。一行3个字段，就由3个[]组成大list[[.....],[...],[..]]

    readKeys = [['contents', 'message_ns'], ['TYPE_CMD']]

    # 一把读进来，注意看内存占用和读取时间。先判断vin所在的目录是否存在，不存在代表vin非法或者在很长时间段内没有任何报文和event信息
    oriMessageList = service.public.getOriMessageList(fullPathList, readKeys)

    return len(oriMessageList)


def assignAmount2TimeSlot(Xaxis, dataList, needSort=False):

    # Xaxis = []
    # for _x in Xaxis1:
    #     Xaxis.append(Timeutils.timeStamp2timeString(_x))


    # 设2个指针：Xaxis指针， buffer指针。 赋值到Yaxis的字典里。
    XaxisCursor = 0
    bufferCursor = 0
    workingStartTimeString = Xaxis[0]
    # step = Xaxis[1] - Xaxis[0]
    # workingEndTimeStamp = Xaxis[-1] + step

    # 看看排序花了多久
    time1 = time.time()*1000

    if needSort:
        dataList.sort()
        logger.debug(f"对{len(dataList)}条数据（例如：{dataList[0]}）的排序，共花了{time.time()*1000 - time1} ms")
    # 找到需要处理的第一条
    find = False
    while bufferCursor < len(dataList):
        # if Timeutils.timeString2timeStamp(dataList[bufferCursor].split(',')[0], ms=True) < workingStartTimeStamp:
        # if int(dataList[bufferCursor].split(',')[0]) < workingStartTimeStamp:
        if dataList[bufferCursor].split(',')[0] < workingStartTimeString:
            # 跳过这条废数据
            bufferCursor += 1
        else:
            # print (f"First content 找到了!")
            find = True
            break

    if find:
        # logger.debug(f"找到了需要处理的第一条：content:{dataList[bufferCursor]}")
        pass
    else:
        # logger.debug(f'最后一个文件读完了，啥也没有')
        return {}

    Yaxis = {}
    # 开始根据respXaxis的刻度，生成对应的respYdict
    while XaxisCursor < len(Xaxis):
        # 这是用转成时间戳方式，太慢，废掉
        # if Xaxis[XaxisCursor] < Timeutils.timeString2timeStamp(dataList[bufferCursor].split(',')[0], ms=True):
        # 改成直接字符串对比的方式，要求来源要有序
        if Xaxis[XaxisCursor] <= dataList[bufferCursor].split(',')[0]:
            XaxisCursor += 1
        else:
            if Yaxis.get(str(Xaxis[XaxisCursor-1])):
                Yaxis[str(Xaxis[XaxisCursor-1])] += 1
            else:
                Yaxis[str(Xaxis[XaxisCursor-1])] = 1

            # 如果buffer还没到底，就cursor+1
            if bufferCursor < (len(dataList) - 1):
                bufferCursor += 1
            else:
                break
    return Yaxis


def assignArray2TimeSlot(Xaxis, dataList, needSort=False):
    # 要求传入的dataList里，每行是一个Dict，并且Dict里要包含一个叫timestamp的key/value
    XaxisCursor = 0
    bufferCursor = 0
    workingStartTimeStamp = Timeutils.timeString2timeStamp(Xaxis[0], ms=True)
    # step = Xaxis[1] - Xaxis[0]
    # workingEndTimeStamp = Xaxis[-1] + step

    if needSort:
        dataList.sort()

    # 找到需要处理的第一条
    find = False
    while bufferCursor < len(dataList):

        if int(dataList[bufferCursor].get('timestamp')) < workingStartTimeStamp:
            # 跳过这条废数据
            bufferCursor += 1
        else:
            # print (f"First content {dataList[bufferCursor]} 找到了!")
            find = True
            break

    if find:
        logger.debug(f"找到了需要处理的第一条：content:{dataList[bufferCursor]}")
    else:
        logger.debug(f'最后一个文件读完了，啥也没有')
        return {}

    Yaxis = {}
    # 开始根据respXaxis的刻度，生成对应的respYdict
    while XaxisCursor < len(Xaxis):

        # if Xaxis[XaxisCursor] < dataList[bufferCursor].split(',')[0]:
        if Timeutils.timeString2timeStamp(Xaxis[XaxisCursor], ms=True) <= dataList[bufferCursor].get('timestamp'):
            XaxisCursor += 1
        else:
            if Yaxis.get(str(Xaxis[XaxisCursor-1])):
                Yaxis[str(Xaxis[XaxisCursor-1])].append(dataList[bufferCursor])
            else:
                Yaxis[str(Xaxis[XaxisCursor-1])] = [dataList[bufferCursor]]

            # 如果buffer还没到底，就cursor+1
            if bufferCursor < (len(dataList) - 1):
                bufferCursor += 1
            else:
                break
    return Yaxis


def loadFileContents(fullFilePath, filename):
    if os.path.exists(fullFilePath):
        try:
            logger.info(f'loading file: {os.path.join(fullFilePath, filename)}')
            with open(os.path.join(fullFilePath, filename), 'r') as f:
                _contents = f.readlines()
                # 以后判断是否需要排序
                # _contents.sort()
                return _contents
        except:
            return []