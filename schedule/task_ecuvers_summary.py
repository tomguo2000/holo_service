import time, datetime, sys, os, json
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
from schedule.task_ecuvers_error_records_flow import ecuversion_errorrecords_flow

def filterME7records(data):
    ME7records = []
    for item in data:
        if 'MC' in item['vin']:
            pass
        else:
            ME7records.append(item)
    return ME7records

def filterErrorrecords(data):
    errorRecords = []
    for item in data:
        if item['errorEcuName']:
            errorRecords.append(item)
        else:
            pass
    return errorRecords


def reformEcuList(data):
    errorEcuList = []
    for item in data:
        for error_ecu_name in item['errorEcuName']:
            errorEcuList.append(error_ecu_name)
    return errorEcuList

def static(data):
    dict = {}
    for key in data:
        dict[key] = dict.get(key, 0) + 1
    return dict


def ecuversion_stat(data):
    totalRecordsAmount = len(data)
    totalME7Records = filterME7records(data)
    errorRecords = filterErrorrecords(totalME7Records)
    errorRecordsAmount = len(errorRecords)
    errorEcuList = reformEcuList(errorRecords)

    errorEcuStatic = static(errorEcuList)
    print(errorEcuStatic)
    print(sorted(errorEcuStatic.items(), key = lambda kv:(kv[1], kv[0])))




if __name__ == '__main__':

    appname = 'holo_service'
    env = CONFIG['env']
    logger = set_logger(f"{appname}.{env}")
    os.environ['HOLO_APPNAME']=appname


    # workingDataOffset = -51    # calculate the date from now

    startDate = '2021-09-01'
    days = 7
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


    ecuversion_stat(ecuversion_statList)