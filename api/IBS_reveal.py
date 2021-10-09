import time
import datetime, math, os, json, psutil
from flask import Blueprint, request
from common.setlog2 import logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode
from multiprocessing import Pool
import service.public, service.msService



ibsreveal = Blueprint("ibsreveal", __name__)


@ibsreveal.route('/', methods=["GET"])
def ibsreveal_index():
    try:
        # 检查入参
        try:
            params = request.args.to_dict()
            logger.info(f"有人调用ibsreveal_index了，参数如下:{params}")

            vin = params['vin']
            date = params.get('date')
            env = params.get('env')
            startTime = params.get('startTime')
            endTime = params.get('endTime')

            # -------
            if startTime:
                startTime = int(startTime)
                if startTime < 9992360223:
                    startTime *= 1000
            else:
                startTime = Timeutils.timeString2timeStamp(date, format='%Y-%m-%d', ms=True)

            if endTime:
                endTime = int(endTime)
                if endTime < 9992360223:
                    endTime *= 1000
            else:
                endTime = Timeutils.timeString2timeStamp(date, format='%Y-%m-%d', ms=True) + 86400*1000 - 1000

            overallList = ['event_ConnStatusList']
            signalList = [
                'ME7_IBS_SOC_STATE',
                'ME7_IBS_SOC',
                'ME7_IBS_SOH_SUL',
                'ME7_IBS_U_BATT',
                'ME7_VCU_LVSmartChrg_Status',
                'ME7_VCU_DC_VoltageReq',
                'ME7_IBS_Status_Voltage',
                'ME7_BCM_SystemPowerMode',
                'ME7_ESP_VehicleSpeed',
                'ME7_DCDC_IdcLvCurr'
            ]

        except:
            raise Exception("110900")

        # 判断不要跨天
        if Timeutils.timeStamp2timeString(startTime)[:10] != Timeutils.timeStamp2timeString(endTime)[:10]:
            raise Exception("110903")
        else:
            date = Timeutils.timeStamp2timeString(startTime)[:10]

        time0 = time.time()*1000
        logger.debug(f"hhhh开始。。。。。。{time0}")


        # 构建一个X轴
        time1 = time.time()*1000
        Xaxis = createXaxis(startTime, endTime, interval=40)
        logger.debug(f"hhhh构建一个X轴完毕。。from {Xaxis[0]} to {Xaxis[-1]}。{time.time()*1000-time1}")


        # 定义返回的resp
        resp = {}
        resp['Xaxis'] = Xaxis
        resp['dateList'] = [date]
        resp['YaxisList'] = []
        resp['YaxisOverall'] = {}
        resp['YaxisSignal'] = {}

        # 传入X轴和dateList，获取emq连接的event结果
        time1 = time.time()*1000
        if "event_ConnStatusList" in overallList:
            event_ConnStatusList = getConnStatus(vin, Xaxis, [date[:10]])
            logger.debug(f"hhhh获取emq连接的event结果 完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['event_ConnStatusList'] = event_ConnStatusList
            resp['YaxisList'].append({"event_ConnStatusList": {
                "type": "event",
                "other": "........."
            }})

        # 传入X轴和dateList，获取SDK初始化的聚合结果
        if "event_VehicleLoginList" in overallList:
            time1 = time.time()*1000
            event_VehicleLoginList = getVehicleLoginEvents(vin, Xaxis, [date[:10]])
            logger.debug(f"hhhh获取SDK初始化的聚合结果 完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['event_VehicleLoginList'] = event_VehicleLoginList
            resp['YaxisList'].append({"event_VehicleLoginList": {
                "type": "message",
                "other": "........."
            }})


        # 传入X轴和dateList，获取控车event的结果
        if "event_RemoteCmdList" in overallList:
            time1 = time.time()*1000
            event_RemoteCmdList = getRemoteCmdEvents(vin, Xaxis, [date[:10]])
            logger.debug(f"hhhh获取控车event的结果 完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['event_RemoteCmdList'] = event_RemoteCmdList
            resp['YaxisList'].append({"event_RemoteCmdList": {
                "type": "message",
                "other": "........."
            }})


        # 传入X轴和dateList，获取国标的报文条数结果
        if "message_tj32960Live" in overallList:
            time1 = time.time()*1000
            message_tj32960Live = getTJ32960(vin, Xaxis, 'message_national_live.txt', [date[:10]])
            logger.debug(f"hhhh获取国标的报文条数结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_tj32960Live'] = message_tj32960Live
            resp['YaxisList'].append({"message_tj32960Live": {
                "type": "message",
                "other": "........."
            }})


        # 传入X轴和dateList，获取国标的报文条数结果
        if "message_tj32960Resent" in overallList:
            time1 = time.time()*1000
            message_tj32960Resent = getTJ32960(vin, Xaxis, 'message_national_resent.txt', [date[:10]])
            logger.debug(f"hhhh获取国标的报文条数结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_tj32960Resent'] = message_tj32960Resent
            resp['YaxisList'].append({"message_tj32960Resent": {
                "type": "message",
                "other": "........."
            }})



        # 传入X轴和dateList，获取企标的聚合结果
        if "message_MSLive" in overallList:
            time1 = time.time()*1000
            message_MSLive = getMS(vin, Xaxis, 'message_enterprise_live.txt', [date[:10]])
            logger.debug(f"hhhh获取企标的聚合结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_MSLive'] = message_MSLive
            resp['YaxisList'].append({"message_MSLive": {
                "type": "message",
                "other": "........."
            }})


        # 传入X轴和dateList，获取企标的聚合结果
        if "message_MSResent" in overallList:
            time1 = time.time()*1000
            message_MSResent = getMS(vin, Xaxis, 'message_enterprise_resent.txt', [date[:10]])
            logger.debug(f"hhhh获取企标的聚合结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_MSResent'] = message_MSResent
            resp['YaxisList'].append({"message_MSResent": {
                "type": "message",
                "other": "........."
            }})


        # 传入X轴和dateList，获取企标的聚合结果
        if "message_MSWarning" in overallList:
            time1 = time.time()*1000
            message_MSWarning = getMS(vin, Xaxis, 'message_enterprise_warning.txt', [date[:10]])
            logger.debug(f"hhhh获取企标的聚合结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_MSWarning'] = message_MSWarning
            resp['YaxisList'].append({"message_MSWarning": {
                "type": "message",
                "other": "........."
            }})


        # 传入X轴和dateList，获取MISC的聚合结果
        if "message_MiscList" in overallList:
            time1 = time.time()*1000
            message_MiscList = getMisc(vin, Xaxis, [date[:10]])
            logger.debug(f"hhhh获取MISC的聚合结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_MiscList'] = message_MiscList
            resp['YaxisList'].append({"message_MiscList": {
                "type": "message",
                "other": "........."
            }})


        # 传入X轴和dateList，获取登入登出心跳的聚合结果
        if "message_HeartbeatList" in overallList:
            time1 = time.time()*1000
            message_HeartbeatList = getHeartbeat(vin, Xaxis, [date[:10]])
            logger.debug(f"hhhh获取登入登出心跳的聚合结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_HeartbeatList'] = message_HeartbeatList
            resp['YaxisList'].append({"message_HeartbeatList": {
                "type": "message",
                "other": "........."
            }})



        if signalList:
            realSignalList = []
            vehicleModel = ''
            for x in signalList:
                vehicleModel = x.split('_')[0]
                realSignalList.append(x[4:])

            if vehicleModel not in ['ME7', 'ME5']:
                raise Exception("110904")

            # 根据singal判断需要解析哪些canid
            canIDDict = service.msService.getCanIDListBySignalList(signalList=realSignalList,vehicleMode=vehicleModel)


            if not canIDDict:
                raise Exception("110900", '传入的signal,又一个或多个找不到对应的canid')


            # 得到日期List，用于拼接数据源的path
            dateList = service.public.createDateListByDuration(startTimestamp=startTime, endTimestamp=endTime)


            # 天际企标的报文文件名
            dataSourcesLive = 'message_enterprise_live.txt'
            # 天际企标补发报文文件名
            dataSourcesResent = 'message_enterprise_resent.txt'
            # 天际企标报警报文文件名
            dataSourcesWaining = 'message_enterprise_warning.txt'
            # 要从文件中读取的key
            readKeys = [['MCUTime'], ['TYPE_CMD'], ['contents', 'MSSecondPacket'], ['contents', 'MSPacketVer'], ['vehicleMode']]

            # 获取需要读取的文件列表
            fullPathList1 = service.public.getFullPathList(vin, dateList, dataSourcesLive)
            fullPathList2 = service.public.getFullPathList(vin, dateList, dataSourcesResent)
            fullPathList3 = service.public.getFullPathList(vin, dateList, dataSourcesWaining)

            # 获取企标的string结果，裁剪后转为dict结果
            oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)
            oriMessageLiveCropedDict = service.public.cropmessage2dict(oriMessageList, startTime, endTime)
            # logger.debug(f"企标实发报文，包含了这些秒包： {oriMessageLiveCropedDict.keys()}")

            # 获取企标的string结果，裁剪后转为dict结果
            oriMessageListResent = service.public.getOriMessageList(fullPathList2, readKeys)
            oriMessageResentCropedDict = service.public.cropmessage2dict(oriMessageListResent, startTime, endTime)
            # logger.debug(f"企标补发报文，包含了这些秒包：{oriMessageResentCropedDict.keys()}")

            # 获取企标的string结果，裁剪后转为dict结果
            oriMessageListWarning = service.public.getOriMessageList(fullPathList3, readKeys)
            # 告警报文，是实发和补发混杂，先排序在去crop
            oriMessageListWarning.sort()
            oriMessageWarningCropedDict = service.public.cropmessage2dict(oriMessageListWarning, startTime, endTime)
            # logger.debug(f"企标告警报文，包含了这些秒包：{oriMessageWarningCropedDict.keys()}")

            # 组合实发,补发,告警报文， 组合后是乱序的
            combinedDict = dict(oriMessageLiveCropedDict, **oriMessageResentCropedDict)
            combinedDict = dict(combinedDict, **oriMessageWarningCropedDict)

            # 对字典进行排序后转成list
            sortedMessages = sorted(combinedDict.items(),key=lambda x:x[0])

            logger.info(f"开始要解析企标了，到目前为止耗时: {time.time()*1000 - time0} ms")
            timeE = time.time()*1000


            # 开进程池并行处理
            Pools = Pool(8)
            asyncResult = []
            respContents = []

            if sortedMessages:
                print(f"异步解析这类数据：{sortedMessages[0]}")
                print("canIDDict:", canIDDict)

            for item in sortedMessages:
                asyncResult.append(Pools.apply_async(tjmsParseSignals2List,
                                                     (item[0],
                                                      item[1][0],
                                                      item[1][1],
                                                      item[1][2],
                                                      canIDDict
                                                      )))
            Pools.close()
            Pools.join()

            for res in asyncResult:
                _res = res.get()
                if _res:
                    respContents += _res

            logger.info(f"企标解析到信号完成，这个阶段耗时: {time.time()*1000 - timeE} ms")
            logger.info(f"企标解析到信号完成，到目前为止耗时: {time.time()*1000 - time0} ms")


            # 每个信号占1行，每行是所有的秒信号
            signalListFor1Line = transformer2Yaxis(canIDDict, respContents)
            logger.info(f"企标transformer2Yaxis完成，到目前为止耗时: {time.time()*1000 - time0} ms")

            for oneSignalAllSec in signalListFor1Line:
                _signalName = oneSignalAllSec[0]
                _signalAllValues = oneSignalAllSec[1]

                # 把每个信号的全部value，对应到统一的X轴上
                # logger.debug(f"assignSignal2TimeSlot调用前: {_signalAllValues}")
                YaxisSignalList = assignSignal2TimeSlot(Xaxis=Xaxis, dataList=_signalAllValues)
                # YaxisSignalList是若干个小dict，值没有[]
                # logger.debug(f"assignSignal2TimeSlot调用后: {YaxisSignalList}")

                resp['YaxisSignal'][_signalName] = YaxisSignalList
                choices = service.msService.getSignalInfo(signalName=_signalName,vehicleModel=vehicleModel)
                resp['YaxisList'].append({_signalName: {
                    "type": "signal",
                    "choices": choices['choices']
                }})


            logger.debug(f"企标全干完了，这一段耗时:。。。{time.time()*1000 - timeE}")

        logger.debug(f"hhhh组织resp 完毕。。 本次http调用共耗时：{time.time()*1000-time0} ms")

        # 没有值的时间点，也生成每个图形上对应x轴的空值
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


def transformer2Yaxis(canIDDict, contents):
    # 得到要输出signal的list
    signalList = []
    for k,v in canIDDict.items():
        for signal in v:
            signalList.append(signal)

    # 得到要输出的signal的List的Index，用于后面的contens到输出结果的映射
    _signalIndexTempList = [x for x in range(0,len(signalList))]
    signalIndex = dict(zip(signalList, _signalIndexTempList))

    # 初始化最终要返回的list
    signalYaxisList = []
    for _Y in signalList:
        signalYaxisList.append([_Y, {}])

    for _line in contents:
        _k = _line[1]
        _v = _line[2]
        signalYaxisList[signalIndex[_line[0]]][1][_k] = _v

    return signalYaxisList



def createXaxis(startTime, endTime, interval=10):

    workingStartTimeStamp = startTime
    workingEndTimeStamp = endTime

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
    b=[]
    for x in respMessageList:
        b.append(json.dumps(x))

    b.sort()

    respMessageList = []
    for x in b:
        respMessageList.append(json.loads(x))

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
    makeResp['YaxisList'] = resp['YaxisList']
    makeResp['YaxisSignal'] = {}
    makeResp['YaxisOverall'] = {}
    del(resp['Xaxis'])
    del(resp['dateList'])
    del(resp['YaxisList'])

    # 归类输出YaxisSignal
    for _item in resp['YaxisSignal']:

        _itemResp = []

        try:
            if isinstance(next(iter(resp['YaxisSignal'][_item].values())), int):
                type = 'int'
            elif isinstance(next(iter(resp['YaxisSignal'][_item].values())), list):
                type = 'list'
            elif isinstance(next(iter(resp['YaxisSignal'][_item].values())), dict):
                type = 'dict'
            elif isinstance(next(iter(resp['YaxisSignal'][_item].values())), float):
                type = 'float'
            else:
                type = 'NULL'
        except:
            type = 'NULL'


        # 遍历每个刻度
        for _x in makeResp['Xaxis']:

            # 取出_item这个分类下，每个刻度对应的实际值
            _value = resp['YaxisSignal'][_item].get(_x)

            if _value:
                _itemResp.append(_value)
            # 不区分value类型，统一返回None 2021/09/24 这样做是对的
            # elif type == 'int':
            #     _itemResp.append(0)
            # elif type == 'float':
            #     _itemResp.append(0.)
            # elif type == 'list':
            #     _itemResp.append([])
            # elif type == 'dict':
            #     _itemResp.append({})
            else:
                _itemResp.append(None)


        makeResp['YaxisSignal'][_item] = _itemResp


    # 归类输出YaxisOverall
    for _item in resp['YaxisOverall']:

        _itemResp = []

        try:
            if isinstance(next(iter(resp['YaxisOverall'][_item].values())), int):
                type = 'int'
            elif isinstance(next(iter(resp['YaxisSignal'][_item].values())), float):
                type = 'float'
            elif isinstance(next(iter(resp['YaxisOverall'][_item].values())), list):
                type = 'list'
            elif isinstance(next(iter(resp['YaxisOverall'][_item].values())), dict):
                type = 'dict'
            else:
                type = 'NULL'
        except:
            type = 'NULL'


        # 遍历每个刻度
        for _x in makeResp['Xaxis']:

            # 取出_item这个分类下，每个刻度对应的实际值
            _value = resp['YaxisOverall'][_item].get(_x)

            if _value:
                _itemResp.append(_value)
            elif type == 'int':
                _itemResp.append(0)
            elif type == 'float':
                _itemResp.append(0.)
            elif type == 'list':
                _itemResp.append([])
            elif type == 'dict':
                _itemResp.append({})
            else:
                _itemResp.append(None)

        makeResp['YaxisOverall'][_item] = _itemResp

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


def tjmsParseSignals2List(MCUTime, data, protocol, vehicleMode, canIDDict):

    signalsValues = service.msService.parse_tjms_signals_2_list(data, vehicleMode=vehicleMode, protocol=protocol, canIDDict=canIDDict)

    response = []

    # 补充上MCUTime
    for _signal in signalsValues:
        response.append((_signal[0], MCUTime, _signal[1]))

    return response



def assignSignal2TimeSlot(Xaxis, dataList, needSort=False):
    # 要求传入的dataList里，每行是一个Dict，并且Dict里要包含一个叫timestamp的key/value
    # print(f"assignSignal2TimeSlot: Xaxis={Xaxis}")
    # print(type(Xaxis))

    # 这里要增加一个copy的_Xaxis,并且添加一个刻度，用于后续计算时，标识终点。
    if len(Xaxis) < 2:
        logger.error(f"ERROR: assignSignal2TimeSlot 说干不了了，这里只有不到2个list的item：{Xaxis}，怎么虚拟出终点啊。。。")

    _duration = Timeutils.timeString2timeStamp(Xaxis[1]) - Timeutils.timeString2timeStamp(Xaxis[0])
    _Xaxis = Xaxis[:]
    _Xaxis.append(Timeutils.timeStamp2timeString(Timeutils.timeString2timeStamp(_Xaxis[-1]) + _duration))

    XaxisCursor = 0
    bufferCursor = 0
    workingStartTimeStamp = Timeutils.timeString2timeStamp(Xaxis[0], ms=True)
    workingStartTimeStr = _Xaxis[0]

    # 注意检查这样得到的list，是否从有序变成了无序
    dataList_keys = list(dataList.keys())
    dataList_values = list(dataList.values())
    _verifyDict = dict(zip(dataList_keys, dataList_values))
    if _verifyDict != dataList:
        logger.error("ERROR: 坏了，从dict转list后，变成了无序的list")
        print("ERROR: 坏了，从dict转list后，变成了无序的list")

    if needSort:
        dataList.sort()

    # 找到需要处理的第一条
    find = False
    while bufferCursor < len(dataList):

        if dataList_keys[bufferCursor] < workingStartTimeStr:
            # 跳过这条废数据
            bufferCursor += 1
        else:
            find = True
            break

    if find:
        logger.debug(f"找到了需要处理的第一条：content:{dataList_keys[bufferCursor]}")
        pass
    else:
        logger.debug(f'最后一个文件读完了，啥也没有')
        return {}

    Yaxis = {}
    # 开始根据_Xaxis的刻度，生成对应的respYdict
    while XaxisCursor < len(_Xaxis):
        # _Xaxis[XaxisCursor] 代表下一个刻度
        if _Xaxis[XaxisCursor] <= dataList_keys[bufferCursor]:
            XaxisCursor += 1
        else:
            if Yaxis.get(str(_Xaxis[XaxisCursor-1])):
                Yaxis[str(_Xaxis[XaxisCursor-1])] += dataList_values[bufferCursor]
            else:
                Yaxis[str(_Xaxis[XaxisCursor-1])] = dataList_values[bufferCursor]

            # 如果buffer还没到底，就cursor+1
            if bufferCursor < (len(dataList_keys) - 1):
                bufferCursor += 1
            else:
                break

    # logger.debug(f"assignSignal2TimeSlot执行中: Yaxis: {Yaxis}")

    # TODO 每个x格子，取目前的[]里的第一个有效值返回
    Yaxis = makeValueSlim(Yaxis)

    return Yaxis


def makeValueSlim(YaxisData):
    # print(f"makeValueSlim函数, YaxisData: {YaxisData}")
    for k,v in YaxisData.items():
        if v:
            # 现在的瘦身方法，是取每个刻度的list值的第一个。后续可以处理成第一个有效值或者均值或者K图
            _value = v[0]

            # 如果时浮点的值，保留两位小数
            if isinstance(_value, float):
                _value = round(_value, 2)

            YaxisData[k] = _value
        else:
            YaxisData[k] = None
    return YaxisData