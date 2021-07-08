import time
import datetime, math, os, json, psutil
from flask import Blueprint, request
from common.setlog2 import logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode
from multiprocessing import Pool
import service.public



tjms = Blueprint("tjms", __name__)



@tjms.route('/parsems', methods=["GET"])
def parsems():
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


        resp = service.public.parse_tjms_message(data=data, vehicleMode=vehicleMode, protocol=protocol, signal=signal)

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
        readKeys = [['MPUTime'], ['TYPE_CMD'], ['contents', 'MSSecondPacket']]

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

        # 解析企标string，得到list
        Pools = Pool(12)
        asyncResult = []
        respContents = []
        for item in cropedOriMessage:
            asyncResult.append(Pools.apply_async(tjmsParse, args=(item.split(',')[2], item.split(',')[1], item.split(',')[0], signal)))
        Pools.close()
        Pools.join()

        for res in asyncResult:
            _res = res.get()
            if _res:
                respContents.append(_res)

        return {
                   "code": 200,
                   "message": None,
                   "businessObj": respContents
               }, 200
    except Exception as ex:
        return {
                   "code": ex.args[0],
                   "message": ReturnCode[ex.args[0]],
                   "businessObj": None
               }, 200

def tjmsParse(data, type, MPUTime, signal=False):
    dd = service.public.parse_tjms_message(data, signal=signal)
    resp = {'MPUTime': MPUTime, 'messageType': type, 'messageDetails': dd}
    return resp
