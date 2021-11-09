import time, datetime, sys, os, json, orjson
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


def ecuversion_allRecordFlow(date, vinList=None):
    try:
        dataSources = 'event_ecus_info.txt'
        readKeys = ['.']

        if not vinList:
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
                refinedContents.append(orjson.loads(c))
            vinsContents[vin] = refinedContents


        allRecordFlow = service.staticService.allRecordFlow(vinsContents)

        return allRecordFlow

    except Exception as ex:
        return {
                   "code": ex.args[0],
                   "message": ReturnCode[ex.args[0]],
                   "businessObj": None
               }, 200

def getLocalVinList():
    vinList = []
    with open('schedule/ota3.vin.list', 'r') as f:
        _temp = f.readlines()
        for _line in _temp:
            vinList.append(_line.strip())
    return vinList

if __name__ == '__main__':

    appname = 'holo_service'
    env = CONFIG['env']
    logger = set_logger(f"{appname}.{env}")
    os.environ['HOLO_APPNAME']=appname


    days = 7    # calculate the date from now
    startDate = Timeutils.timeString2timeArray(Timeutils.serverTimeNow()[:19]) - datetime.timedelta(days=days)
    startDate = Timeutils.timeArray2timeString(startDate)[:10]

    vinList = getLocalVinList()

    ecuversion_allResultList = []
    workingDataOffset = 0
    while workingDataOffset < days:
        workingDateArray = Timeutils.timeString2timeArray(startDate, format='%Y-%m-%d')
        workingDateArray = workingDateArray + datetime.timedelta(days=workingDataOffset)
        workingDateStr = Timeutils.timeArray2timeString(workingDateArray)[:10]
        _temp = ecuversion_allRecordFlow(workingDateStr, vinList=vinList)
        for _i in _temp:
            ecuversion_allResultList.append(_i)

        workingDataOffset += 1

    print(ecuversion_allResultList)

    resp = []

    for vin in vinList:
        vinRecords = 0
        vinWriteRecords = 0
        vinErrorRecords = 0
        vinCorrectRate = 0.
        for _record in ecuversion_allResultList:
            if vin == _record['vin']:
                # 有诊断记录
                vinRecords += 1
                if _record['errorEcuName']:
                    vinErrorRecords += 1
                else:
                    vinWriteRecords += 1
        if vinRecords:
            vinCorrectRate = vinWriteRecords / vinRecords

        _resp = vin + ',' + str(vinRecords) + ',' + str(vinWriteRecords) + ',' + str(vinErrorRecords) + ',' + '%6.2f%%'%(vinCorrectRate*100)
        resp.append(_resp)

    print(resp)

