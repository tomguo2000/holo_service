import time, datetime, sys, os, json
sys.path.append('/Users/guoliang/PycharmProjects/holo_service/')
os.chdir('../')
print(sys.path)

import service.public
import service.staticService
from flask import Blueprint, request
from common.setlog2 import set_logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode
from multiprocessing import Pool


def ecuversion_stat(date):
    try:
        dataSources = 'event_ecus_info.txt'
        readKeys = ['.']

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
                refinedContents.append(json.loads(c))
            vinsContents[vin] = refinedContents

        staticReport = service.staticService.static_ecu_ver(vinsContents)
        # 计算单车的结果

        # 计算全部车辆的计算结果


        return {
                   "code": 200,
                   "message": None,
                   "businessObj": staticReport
               }
    except Exception as ex:
        return {
                   "code": ex.args[0],
                   "message": ReturnCode[ex.args[0]],
                   "businessObj": None
               }, 200



if __name__ == '__main__':

    appname = 'holo_service'
    env = CONFIG['env']
    logger = set_logger(f"{appname}.{env}")
    os.environ['HOLO_APPNAME']=appname


    workingDataOffset = -7    # calculate the date from now

    workingDateArray = Timeutils.timeStamp2timeArray(timeStamp=time.time()) + datetime.timedelta(days=workingDataOffset)
    workingDateStr = Timeutils.timeArray2timeString(workingDateArray)[:10]
    print (ecuversion_stat(workingDateStr))
