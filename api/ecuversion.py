import time
import datetime, math, os, json, psutil
from flask import Blueprint, request
from common.setlog2 import logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode
from multiprocessing import Pool
import service.public, service.staticService

ecuversion = Blueprint("ecuversion", __name__)


@ecuversion.route('/', methods=["GET"])
def ecuversion_stat():
    try:
        # 检查入参
        try:
            params = request.args.to_dict()
            vin = params.get('vin')
            ecus = params.get('ecus')
            date = params.get('date')   # 格式: 2021-06-26
            if not date:
                date = Timeutils.serverTimeNow()[:10]
        except:
            raise Exception ("110900")

        # 获取vin码的列表
        vinList = service.public.getVINList()

        # 获取fullpath，按照['.']来读取内容
        vinsContents = {}
        dataSources = 'event_ecus_info.txt'
        readKeys = ['.']

        for vin in vinList:
            vin_fullPathList = service.public.getFullPathList(vin, [date], dataSources)
            contents = service.public.getOriMessageList(vin_fullPathList, readKeys)
            refinedContents = []
            for c in contents:
                refinedContents.append(ujson.loads(c))
            vinsContents[vin] = refinedContents

        staticReport = service.staticService.static_ecu_ver(vinsContents)
        # 计算单车的结果

        # 计算全部车辆的计算结果


        return {
                   "code": 200,
                   "message": None,
                   "businessObj": staticReport
               }, 200
    except Exception as ex:
        return {
                   "code": ex.args[0],
                   "message": ReturnCode[ex.args[0]],
                   "businessObj": None
               }, 200

