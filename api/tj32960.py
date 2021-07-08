import time
import datetime, math, os, json, psutil
from flask import Blueprint, request
from common.setlog2 import logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode
from multiprocessing import Pool
import service.public
import service.gb32960.tj_gb32960 as tj


tj32960 = Blueprint("tj32960", __name__)


@tj32960.route('/', methods=["GET"])
def details():
    try:

        # 检查入参
        try:
            params = request.args.to_dict()
            vin = params['vin']
            startTime = int(params['startTime']) if int(params['startTime']) > 9999999999 else int(params['startTime'])*1000
            endTime = int(params['endTime']) if int(params['endTime']) > 9999999999 else int(params['endTime'])*1000
            details = params.get('details') if params.get('details') else None

        except:
            raise Exception ("110900")


        # 根据X轴，得到日期List，用于拼接数据源的path
        dateList = service.public.createDateListByDuration(startTimestamp=startTime, endTimestamp=endTime)


        # 天际国标的报文文件名
        dataSourcesLive = 'message_national_live.txt'
        # 天际国标补发报文文件名
        dataSourcesResent = 'message_national_resent.txt'
        # 要从文件中读取的key
        readKeys = [['MPUTime'], ['TYPE_CMD'], ['contents', 'message_ns']]

        # 获取需要读取的文件列表
        fullPathList1 = service.public.getFullPathList(vin, dateList, dataSourcesLive)
        fullPathList2 = service.public.getFullPathList(vin, dateList, dataSourcesResent)

        # 获取国标的string结果
        oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)
        oriMessageListResent = service.public.getOriMessageList(fullPathList2, readKeys)

        # 组合实发和补发报文
        oriAllMessage = oriMessageList + oriMessageListResent

        # 排序后按照时间段裁剪数据
        oriAllMessage.sort()
        cropedOriMessage = service.public.cropmessage(oriAllMessage, startTime, endTime)

        if not details:
            return {
                       "code": 200,
                       "message": None,
                       "businessObj": {"amount": len(cropedOriMessage)}
                   }, 200

        # 解析国标string，得到list
        Pools = Pool(12)
        asyncResult = []
        respContents = []
        for item in cropedOriMessage:
            asyncResult.append(Pools.apply_async(tjgb, args=(item.split(',')[2], item.split(',')[1])))
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

def tjgb(data, type):
    dd = tj.parse_tj_ns_message(data)
    # print (dd.dict,type)
    resp = {'messageType': type, 'messageDetails': dd.dict}
    return resp
