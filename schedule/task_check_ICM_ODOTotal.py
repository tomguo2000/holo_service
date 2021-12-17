import time, datetime, sys, os, json
sys.path.append('/Users/guoliang/PycharmProjects/holo_service/')
sys.path.append('/home/baowen/holo_service/')

os.chdir('../')
print(sys.path)

import service.public
import service.msService
import service.staticService
from flask import Blueprint, request
from common.setlog2 import set_logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode
from multiprocessing import Pool
from service.public import getPureContents

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
    # print(errorEcuStatic)
    ss = (sorted(errorEcuStatic.items(), key = lambda kv:(kv[1], kv[0]), reverse=True))

    return {
        "totalRecordsAmount": totalRecordsAmount,
        "errorRecordsAmount": errorRecordsAmount,
        # "errorEcuStatic": errorEcuStatic,
        "errorEcuStaticSorted": ss
    }


def get_ICM_ODOTotal(vin, date):
    # 天际企标的报文文件名
    dataSourcesLive = 'message_enterprise_live.txt'
    canIDDict = {
        'ICM_0x3A0': ['ICM_ODOTotal']
    }
    # canIDDict = {
    #     'BMS_0x100': ['BMS_PackU', 'BMS_PackI'],
    #     'BCM_0x310': ['BCM_FL_Door_Sts', 'BCM_FR_Door_Sts', 'BCM_RL_Door_Sts', 'BCM_RR_Door_Sts']
    # }
    # VBU_0x9A
    # HU_0x322

    fullPathList1 = service.public.getFullPathList(vin, [date], dataSourcesLive, env=env)
    return service.msService.getCanIDMessageFromFile(fullPathList1[0], canIDDict)


def strictly_increasing(dataList):
    return all(x <= y for x, y in zip(dataList, dataList[1:]))


def check_increasing(resultList):
    resp = {}
    resp['result'] = True
    x = 0
    for item in resultList:
        # item[0] 时间
        # item[1] value
        if x > item[1]:
            x = item[1]
            resp['result'] = False
            if resp.get('errorAt'):
                resp['errorAt'].append({item[0]: item[1]})
            else:
                resp['errorAt'] = [{item[0]:item[1]}]
        else:
            x = item[1]
    return resp


def singleCheckJob(seq, vin, startDateStr, endDateStr):
    print(f"{vin} is doing...{seq}...")

    resultList =[]
    workingDateStr = startDateStr

    while workingDateStr <= endDateStr:

        _temp = get_ICM_ODOTotal(vin=vin, date=workingDateStr)

        resultList = resultList + _temp

        workingDateArray = Timeutils.timeString2timeArray(workingDateStr, format='%Y-%m-%d')
        workingDateArray = workingDateArray + datetime.timedelta(days=1)
        workingDateStr = Timeutils.timeArray2timeString(workingDateArray)[:10]

    # time0 = time.time()*1000
    # resp = strictly_increasing(resultList)
    # print(f"strictly_increasing method check {vin}, cost me {time.time()*1000 - time0} ms")

    time0 = time.time()*1000
    resp = check_increasing(resultList)
    print(f"check_increasing method check {vin}, cost me {time.time()*1000 - time0} ms")
    return vin, resp['result'], resp.get('errorAt')


if __name__ == '__main__':

    appname = 'holo_service'
    env = CONFIG['env']
    logger = set_logger(f"{appname}.{env}")
    os.environ['HOLO_APPNAME']=appname

    workingDataOffset = -14    # calculate the date from now

    currentDateStr = Timeutils.serverTimeNow()[:10]      # '2021-12-16'
    _currentDateArray = Timeutils.timeString2timeArray(currentDateStr, format='%Y-%m-%d')
    _startDataArray = _currentDateArray + datetime.timedelta(days=workingDataOffset)
    startDateStr = Timeutils.timeArray2timeString(_startDataArray)[:10]      # '2021-12-02'

    with open('schedule/me5.list', 'r') as f:
        vinList = f.readlines()

    for seq, line in enumerate(vinList):
        vinList[seq] = line.strip()


    # 开进程池并行处理
    Pools = Pool(8)
    asyncResult = []

    for seq, vin in enumerate(vinList):
        asyncResult.append(Pools.apply_async(singleCheckJob,
                                             (seq,
                                              vin,
                                              startDateStr,
                                              currentDateStr
                                              )))
    Pools.close()
    Pools.join()

    for res in asyncResult:
        _res = res.get()
        if _res:
            print(_res)