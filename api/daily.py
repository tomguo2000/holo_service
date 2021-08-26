import time
import datetime, math, os, json, psutil
from flask import Blueprint, request
from common.setlog2 import logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode
from multiprocessing import Pool
import service.public

daily = Blueprint("daily", __name__)


@daily.route('/', methods=["GET"])
def overall_by_vin_vin():
    try:

        # 检查入参
        try:
            params = request.args.to_dict()
            vin = params['vin']
            date = params['date']  # 格式: 2021-06-26
            date = date+'_00:00:00'  # 新格式: 2021-06-26_00:00:00

        except:
            raise Exception ("110900")


        time0 = time.time()*1000
        logger.debug(f"hhhh开始。。。。。。{time0}")

        # 构建一个X轴
        time1 = time.time()*1000
        Xaxis = createXaxis(date)
        logger.debug(f"hhhh构建一个X轴完毕。。。{time.time()*1000-time1}")



        # 传入X轴和dateList，获取国标的报文条数结果
        time1 = time.time()*1000
        message_tj32960Live = getTJ32960(vin, Xaxis, 'message_national_live.txt', [date[:10]])
        message_tj32960Resent = getTJ32960(vin, Xaxis, 'message_national_resent.txt', [date[:10]])
        logger.debug(f"hhhh获取国标的报文条数结果完毕。。。{time.time()*1000-time1}")


        # 传入X轴和dateList，获取企标的聚合结果
        time1 = time.time()*1000
        message_MSLive = getMS(vin, Xaxis, 'message_enterprise_live.txt', [date[:10]])
        message_MSResent = getMS(vin, Xaxis, 'message_enterprise_resent.txt', [date[:10]])
        message_MSWarning = getMS(vin, Xaxis, 'message_enterprise_warning.txt', [date[:10]])

        logger.debug(f"hhhh获取企标的聚合结果完毕。。。{time.time()*1000-time1}")



        # 传入X轴和dateList，获取MISC的聚合结果
        time1 = time.time()*1000
        message_MiscList = getMisc(vin, Xaxis, [date[:10]])
        logger.debug(f"hhhh获取MISC的聚合结果完毕。。。{time.time()*1000-time1}")



        # 传入X轴和dateList，获取登入登出心跳的聚合结果
        time1 = time.time()*1000
        message_HeartbeatList = getHeartbeat(vin, Xaxis, [date[:10]])
        logger.debug(f"hhhh获取登入登出心跳的聚合结果完毕。。。{time.time()*1000-time1}")



        # 传入X轴和dateList，获取SDK初始化的聚合结果
        time1 = time.time()*1000
        event_VehicleLoginList = getVehicleLoginEvents(vin, Xaxis, [date[:10]])
        logger.debug(f"hhhh获取SDK初始化的聚合结果 完毕。。。{time.time()*1000-time1}")



        # 传入X轴和dateList，获取控车event的结果
        time1 = time.time()*1000
        event_RemoteCmdList = getRemoteCmdEvents(vin, Xaxis, [date[:10]])
        logger.debug(f"hhhh获取控车event的结果 完毕。。。{time.time()*1000-time1}")



        # 传入X轴和dateList，获取emq连接的event结果
        time1 = time.time()*1000
        event_ConnStatusList = getConnStatus(vin, Xaxis, [date[:10]])
        logger.debug(f"hhhh获取emq连接的event结果 完毕。。。{time.time()*1000-time1}")


        time1 = time.time()*1000
        resp = {}
        resp['Xaxis'] = Xaxis
        resp['dateList'] = [date]
        resp['message_国标实发报文'] = message_tj32960Live
        resp['message_国标补发报文'] = message_tj32960Resent
        resp['message_企标实发报文'] = message_MSLive
        resp['message_企标补发报文'] = message_MSResent
        resp['message_企标告警报文'] = message_MSWarning
        resp['message_Misc报文'] = message_MiscList
        resp['message_心跳报文'] = message_HeartbeatList
        resp['event_车辆登录Vehicle服务事件'] = event_VehicleLoginList
        resp['event_远程控车指令事件'] = event_RemoteCmdList
        resp['event_Tbox到平台连接事件'] = event_ConnStatusList

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


def createXaxis(date, interval=10):

    workingStartTimeStamp = Timeutils.timeString2timeStamp(date, ms=True)
    workingEndTimeStamp = workingStartTimeStamp + 86400*1000 # 1天的毫秒数

    # xAxisTotal: X轴的点位数量
    xAxisTotal = math.ceil((workingEndTimeStamp - workingStartTimeStamp) / (interval*1000))

    # respXaxis: 用于绘图的X轴的坐标list
    respXaxis = []
    timeCursor = workingStartTimeStamp
    while xAxisTotal:
        respXaxis.append(Timeutils.timeStamp2timeString(timeCursor))
        timeCursor += interval * 1000
        xAxisTotal -= 1

    return respXaxis


def getTJ32960(vin, Xaxis, messagetype, dateList):
    # 天际国标的报文文件名
    dataSources = messagetype

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources)

    # readKeys = [['MPUTime'], ['TYPE_CMD']]
    readKeys = [['MPUTime']]
    # 获取内容
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    # 假设实发和补发没有重复的部分。不需要进行去重的动作

    Y32960 = assignAmount2TimeSlot(Xaxis, oriMessageList, needSort=False)
    return Y32960


def getMS(vin, Xaxis, messagetype, dateList):
    time0 = time.time()*1000

    # 天际企标的报文文件名
    dataSourcesLive = messagetype


    # 获取需要读取的文件列表
    time1 = time.time()*1000
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSourcesLive)


    # 读取必要的message
    time1 = time.time()*1000
    readKeys = [['MPUTime']]
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)


    # 把企标分发到Y轴
    time1 = time.time()*1000
    YMSdict = assignAmount2TimeSlot(Xaxis, oriMessageList, needSort=False)

    return YMSdict


def getMisc(vin, Xaxis, dateList):
    # 天际的报文文件名
    dataSources = 'message_misc.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources)

    readKeys = [['MPUTime']]
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    YMisc = assignAmount2TimeSlot(Xaxis, oriMessageList)

    return YMisc


def getHeartbeat(vin, Xaxis, dateList):
    # 天际的报文文件名
    dataSources = 'message_hearbeat.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources)

    readKeys = [['MPUTime']]
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
        _temp = json.loads(_item)

        if _temp['timestamp'] < 9999999999:
            _temp['timestamp'] = _temp['timestamp'] * 1000

        respMessageList.append({'timestamp': _temp['timestamp'],
                                'timestr': Timeutils.timeStamp2timeString(_temp['timestamp']),
                                'event': _temp['event']})

    YHeartbeat = assignArray2TimeSlot(Xaxis, respMessageList)

    return YHeartbeat


def getRemoteCmdEvents(vin, Xaxis, dateList):
    # 天际的报文文件名
    dataSources = 'event_remote_cmd.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources)

    # readKeys = [['timestamp'], ['event']]
    readKeys = ['.']
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    # readKeys用['.']的时候，通畅需要对返回的List做二次加工
    respMessageList = []
    for _item in oriMessageList:

        try:

            _temp = json.loads(_item)

            # 判断是平台发出，还是tbox回执
            if _temp.get('status') == '258':
                # 这是平台发出的控车指令，新增记录
                if _temp['timestamp'] < 9999999999:
                    _temp['timestamp'] = _temp['timestamp'] * 1000

                respMessageList.append({'timestamp': _temp['timestamp'],    # 1626159853760
                                        'timestr': Timeutils.timeStamp2timeString(_temp['timestamp']),
                                        'cmd': _temp['cmd'],            # "Diagnose_Read_DTC"
                                        'cmdId': _temp['cmdId'],        # "1626159852952hMrO5FawR5v"
                                        'sn': _temp.get('sn'),          # "4988"
                                        'vin': _temp['vin'],            # "LTWA35K14LS000540"
                                        'QA': _temp['status']           # "258"
                                        })
            elif _temp.get('ACK'):
                # 这个tbox返回的回执，修改记录
                _vin = _temp.get('VIN')         # "LTWA35K14LS000540"
                _sn = _temp.get('SN')           # "137c"
                _sn = str(int(_sn, 16))         # 转10进制的str
                _ACK = _temp.get('ACK')         # "01"

                for _update in respMessageList:
                    if _update['vin'] == _vin and _update['sn'] == _sn:
                        _update['QA'] = _ACK

            else:
                logger.warning(f"不予处理的event内容，既不是258已发出，也不是tbox的ACK:{_temp}")
                continue
        except:
            logger.warning(f"dirty message 读不懂:{_temp}")

    # 2021-08-27 发生过remotecmd的文件里，时间乱序，所以在这里排序一下
    print(type(respMessageList))
    print(respMessageList)

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
        _temp = json.loads(_item)

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
        connEvent = json.loads(connStatusList[-1])
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

