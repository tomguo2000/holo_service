import time, datetime, sys, os, json, ujson
sys.path.append('/Users/guoliang/PycharmProjects/holo_service/')
sys.path.append('/home/baowen/holo_service/')

os.chdir('../')
print(sys.path)

import service.public
import service.staticService
from flask import Blueprint, request
from common.setlog2 import set_logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode
from multiprocessing import Pool


def ecuversion_errorrecords_flow(date):
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
                refinedContents.append(ujson.loads(c))
            vinsContents[vin] = refinedContents


        errorRecordFlow = service.staticService.errorRecordFlow(vinsContents)

        return errorRecordFlow

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


    # workingDataOffset = -51    # calculate the date from now

    startDate = '2021-09-01'
    days = 30
    workingDataOffset = 0

    ecuversion_statList = []
    while workingDataOffset < days:

        workingDateArray = Timeutils.timeString2timeArray(startDate, format='%Y-%m-%d')
        workingDateArray = workingDateArray + datetime.timedelta(days=workingDataOffset)
        workingDateStr = Timeutils.timeArray2timeString(workingDateArray)[:10]
        _temp = ecuversion_errorrecords_flow(workingDateStr)
        for _i in _temp:
            ecuversion_statList.append(_i)

        workingDataOffset += 1



    with open ("error_ecu_result.csv", 'w') as f:
        for line in ecuversion_statList:
            _c = {}
            _c['timeStr'] = line['timeStr']
            _c['vin'] = line['vin']
            _c['errorEcuName'] = line['errorEcuName']
            _c['fullEcuInfo'] = line['fullEcuInfo']
            # f.write(json.dumps(c))
            f.write(json.dumps(_c))
            f.write('\n')

