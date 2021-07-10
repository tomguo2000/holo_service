import time
import datetime, math, os, json, psutil
from flask import Blueprint, request
from common.setlog2 import logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode
from multiprocessing import Pool
import service.public, service.msService
import sys
from service.public import parse_tjms_message



tjms = Blueprint("tjms", __name__)


@tjms.route('/signals/', methods=["GET"])
def signals():
    try:

        time0 = time.time() * 1000

        # 检查入参
        try:
            params = request.args.to_dict()
            vin = params['vin']
            startTime = int(params['startTime']) if int(params['startTime']) > 9999999999 else int(params['startTime'])*1000
            endTime = int(params['endTime']) if int(params['endTime']) > 9999999999 else int(params['endTime'])*1000
            signal = params.get('signal') if params.get('signal') else None
            vehicleMode = params.get('vehicleMode') if params.get('vehicleMode') else 'ME7'

        except:
            raise Exception ("110900",'params error')



        # 根据singal判断需要解析哪些canid，得到一个canIDList和singalList
        signalList = signal.split(',')
        canIDDict = service.msService.getCanIDListBySignalList(signalList=signalList,vehicleMode=vehicleMode)

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
        readKeys = [['MPUTime'], ['TYPE_CMD'], ['contents', 'MSSecondPacket'], ['contents', 'MSPacketVer']]

        # 获取需要读取的文件列表
        fullPathList1 = service.public.getFullPathList(vin, dateList, dataSourcesLive)
        fullPathList2 = service.public.getFullPathList(vin, dateList, dataSourcesResent)
        fullPathList3 = service.public.getFullPathList(vin, dateList, dataSourcesWaining)

        # 获取企标的string结果
        oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)
        oriMessageListResent = service.public.getOriMessageList(fullPathList2, readKeys)
        oriMessageListWarning = service.public.getOriMessageList(fullPathList3, readKeys)

        # 组合实发和补发报文
        oriAllMessage = oriMessageList + oriMessageListResent + oriMessageListWarning

        # 排序后按照时间段裁剪数据
        oriAllMessage.sort()
        cropedOriMessage = service.public.cropmessage(oriAllMessage, startTime, endTime)


        logger.info(f"开始要解析企标了，到目前未知耗时: {time.time()*1000 - time0} ms")
        time0 = time.time()*1000


        # 开进程池并行处理
        Pools = Pool(8)
        asyncResult = []
        respContents = []

        for item in cropedOriMessage:
            asyncResult.append(Pools.apply_async(tjmsParseSignals,
                                                 (item.split(',')[2],
                                                  item.split(',')[3],
                                                  item.split(',')[1],
                                                  item.split(',')[0],
                                                  canIDDict
                                                  )))
        Pools.close()
        Pools.join()

        for res in asyncResult:
            _res = res.get()
            if _res:
                respContents.append(_res)


        logger.info(f"开始要写最后的结果了，前一阶段耗时: {time.time()*1000 - time0} ms")

        with open ('result.csv', 'w') as f:
            f.write(str(respContents))


        resp = {
            "vin":vin,
            "startTime": startTime,
            "endTime": endTime,
            "canIDDict": canIDDict,
            "signalList": signalList,
            "vehicleMode": vehicleMode
        }
        return(resp)
    except Exception as ex:
        return {
                   "code": ex.args[0],
                   "message": ex.args[1],
                   "businessObj": None
               }, 200




@tjms.route('/parseSingleMSPacket', methods=["GET"])
def parseSingleMSPacket():
    try:

        # 检查入参
        try:
            params = request.args.to_dict()
            vehicleMode = params.get('vehicleMode')
            protocol = params.get('protocol')
            data = params.get('data')
            signal = params.get('signal')

        except:
            raise Exception ("110900")

        resp = parse_tjms_message(data=data, vehicleMode=vehicleMode, protocol=protocol, signal=signal)

        return resp

    except Exception as ex:
        return {
                   "code": ex.args[0],
                   "message": ReturnCode[ex.args[0]],
                   "businessObj": None
               }, 200


@tjms.route('/', methods=["GET"])
def details():
    try:

        time0 = time.time() * 1000

        # 检查入参
        try:
            params = request.args.to_dict()
            vin = params['vin']
            startTime = int(params['startTime']) if int(params['startTime']) > 9999999999 else int(params['startTime'])*1000
            endTime = int(params['endTime']) if int(params['endTime']) > 9999999999 else int(params['endTime'])*1000
            detail = params.get('detail') if params.get('detail') else None
            signal = params.get('signal') if params.get('signal') else None

        except:
            raise Exception ("110900")


        # 根据X轴，得到日期List，用于拼接数据源的path
        dateList = service.public.createDateListByDuration(startTimestamp=startTime, endTimestamp=endTime)


        # 天际企标的报文文件名
        dataSourcesLive = 'message_enterprise_live.txt'
        # 天际企标补发报文文件名
        dataSourcesResent = 'message_enterprise_resent.txt'
        # 天际企标报警报文文件名
        dataSourcesWaining = 'message_enterprise_warning.txt'
        # 要从文件中读取的key
        readKeys = [['MPUTime'], ['TYPE_CMD'], ['contents', 'MSSecondPacket'], ['contents', 'MSPacketVer']]

        # 获取需要读取的文件列表
        fullPathList1 = service.public.getFullPathList(vin, dateList, dataSourcesLive)
        fullPathList2 = service.public.getFullPathList(vin, dateList, dataSourcesResent)
        fullPathList3 = service.public.getFullPathList(vin, dateList, dataSourcesWaining)

        # 获取企标的string结果
        oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)
        oriMessageListResent = service.public.getOriMessageList(fullPathList2, readKeys)
        oriMessageListWarning = service.public.getOriMessageList(fullPathList3, readKeys)

        # 组合实发和补发报文
        oriAllMessage = oriMessageList + oriMessageListResent + oriMessageListWarning

        # 排序后按照时间段裁剪数据
        oriAllMessage.sort()
        cropedOriMessage = service.public.cropmessage(oriAllMessage, startTime, endTime)

        if not detail:
            return {
                       "code": 200,
                       "message": None,
                       "businessObj": {"amount": len(cropedOriMessage)}
                   }, 200


        logger.info(f"开始要解析企标了，到目前未知耗时: {time.time()*1000 - time0} ms")
        time0 = time.time()*1000

        # 串行处理，功能正确
        # for item in cropedOriMessage:
        #     _res = tjmsParse(item.split(',')[2], candb, item.split(',')[1], item.split(',')[0], signal)
        #     respContents.append(_res)

        # 开进程池并行处理
        Pools = Pool(8)
        asyncResult = []
        respContents = []

        for item in cropedOriMessage:
            asyncResult.append(Pools.apply_async(tjmsParse, (item.split(',')[2], item.split(',')[3], item.split(',')[1], item.split(',')[0], signal)))
        Pools.close()
        Pools.join()

        for res in asyncResult:
            _res = res.get()
            if _res:
                respContents.append(_res)


        # 并行处理方式2 效果不好
        # with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        #
        #     to_do = []
        #     for item in cropedOriMessage:
        #         future = executor.submit(tjmsParse, item.split(',')[2], candb, item.split(',')[1], item.split(',')[0], signal)
        #         to_do.append(future)
        #
        #     results = []
        #     for future in concurrent.futures.as_completed(to_do):
        #         res = future.result()
        #         results.append(res)


        logger.info(f"开始要写最后的结果了，前一阶段耗时: {time.time()*1000 - time0} ms")

        with open ('result.csv', 'w') as f:
            f.write(str(respContents))

        return {
                   "code": 200,
                   "message": None,
                   "businessObj": {"len":len(respContents),"size":sys.getsizeof(respContents)}
               }, 200
    except Exception as ex:
        return {
                   "code": ex.args[0],
                   "message": ReturnCode[ex.args[0]],
                   "businessObj": None
               }, 200


def tjmsParse(data, candb, type, MPUTime, signal=False):
    dd = service.public.parse_tjms_message(data, protocol=candb, signal=signal)

    resp = {'MPUTime': MPUTime, 'messageType': type, 'messageDetails': dd}
    return resp



def tjmsParseSignals(data, candb, type, MPUTime, canIDDict):

    dd = service.msService.parse_tjms_signals(data, vehicleMode='ME7', protocol=candb, canIDDict=canIDDict)

    resp = {'MPUTime': MPUTime, 'messageType': type, 'messageDetails': dd}
    return resp