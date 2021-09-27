import time
import datetime, math, os, json, psutil
from flask import Blueprint, request
from common.setlog2 import logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode, EnterpriseTransportProtolVer
from multiprocessing import Pool
import cantools
import numpy, binascii
from service.public import candbPool



def cropWarningAndTransformer2dict(oriAllMessage, startTime, endTime):
    startTimeStr = Timeutils.timeStamp2timeString(startTime)
    endTimeStr = Timeutils.timeStamp2timeString(endTime)

    messageList = []
    for line in oriAllMessage:
        messageList.append(json.loads(line))
    messageList.sort()

    bufferCursor = 0

    # 找到需要处理的第一条
    find = False
    while bufferCursor < len(oriAllMessage):

        if oriAllMessage[bufferCursor].get("MCUTime") < startTimeStr:
            # 跳过这条废数据
            bufferCursor += 1
        else:
            # print (f"First content {dataList[bufferCursor]} 找到了!")
            find = True
            break

    if find:
        logger.debug(f"找到了需要处理的第一条：content:{oriAllMessage[bufferCursor]}")
    else:
        logger.debug(f'最后一个文件读完了，啥也没有')
        return {}

    respMessage = {}

    while oriAllMessage[bufferCursor].get("MCUTime") < endTimeStr:
        _jsondata = oriAllMessage[bufferCursor]
        seq=(_jsondata['vehicleMode'], _jsondata['contents']['MSPacketVer'], _jsondata['contents']['MSSecondPacket'])
        respMessage[_jsondata['MCUTime']] = ','.join(seq)

        # 如果buffer还没到底，就cursor+1
        if bufferCursor < (len(oriAllMessage) - 1):
            bufferCursor += 1
        else:
            break
    return respMessage



def cropAndTransformer2dict(oriAllMessage, startTime, endTime):
    startTimeStr = Timeutils.timeStamp2timeString(startTime)
    endTimeStr = Timeutils.timeStamp2timeString(endTime)

    bufferCursor = 0

    # 找到需要处理的第一条
    find = False
    while bufferCursor < len(oriAllMessage):

        if oriAllMessage[bufferCursor].split('"MCUTime": "')[1][:19] < startTimeStr:
            # 跳过这条废数据
            bufferCursor += 1
        else:
            # print (f"First content {dataList[bufferCursor]} 找到了!")
            find = True
            break

    if find:
        logger.debug(f"找到了需要处理的第一条：content:{oriAllMessage[bufferCursor]}")
    else:
        logger.debug(f'最后一个文件读完了，啥也没有')
        return {}

    respMessage = {}

    while oriAllMessage[bufferCursor].split('"MCUTime": "')[1][:19] < endTimeStr:
        _jsondata = json.loads(oriAllMessage[bufferCursor])
        seq=(_jsondata['vehicleMode'], _jsondata['contents']['MSPacketVer'], _jsondata['contents']['MSSecondPacket'])
        respMessage[_jsondata['MCUTime']] = ','.join(seq)

        # 如果buffer还没到底，就cursor+1
        if bufferCursor < (len(oriAllMessage) - 1):
            bufferCursor += 1
        else:
            break
    return respMessage



def genMessagesSignals(canDB):
    respDict = {}
    messages = canDB.messages
    for _msg in messages:
        signals = _msg.signals
        respDict[_msg.name] = {_sig.name for _sig in signals}
    return respDict


def getSignalInfo(signalName, vehicleModel):
    if vehicleModel == 'ME7':
        canDB = candbPool[vehicleModel]['0e']
    else:
        canDB = candbPool[vehicleModel]['00']

    fullCanDBDict = genMessagesSignals(canDB)

    resp = {}
    for x in fullCanDBDict:
        if signalName in fullCanDBDict[x]:
            signalInfo = canDB.get_message_by_name(x).get_signal_by_name(signalName)
            resp['canID'] = x
            resp['comment'] = signalInfo.comment
            resp['choices'] = signalInfo.choices
            break

    return resp

def getCanIDListBySignalList(signalList, vehicleMode):
    if vehicleMode == 'ME7':
        canDB = candbPool[vehicleMode]['0e']
    else:
        canDB = candbPool[vehicleMode]['00']
    fullCanDBDict = genMessagesSignals(canDB)

    resp = {}
    errorSign = False
    for sig in signalList:
        for x in fullCanDBDict:
            if sig in fullCanDBDict[x]:
                if resp.get(x):
                    resp[x].append(sig)
                else:
                    resp[x] = [sig]
                break
        else:
            errorSign = True

    # resp = {
    #     'BMS_0x100': ['BMS_PackU', 'BMS_PackI'],
    #     'BCM_0x310': ['BCM_FL_Door_Sts', 'BCM_FR_Door_Sts', 'BCM_RL_Door_Sts', 'BCM_RR_Door_Sts']
    # }

    if errorSign:
        return None
    else:
        return resp



def parse_tjms_signals(data, vehicleMode, protocol, canIDDict):

    time1 = time.time() * 1000

    canDb = candbPool[vehicleMode][protocol]
    CanIDList = EnterpriseTransportProtolVer[vehicleMode][protocol]

    CanIDListLen = len(CanIDList)

    mainCursor = 0

    canIDAmount = {}
    canMessageOffset = {}

    for y in range(CanIDListLen):
        canIDAmount[y] = int(data[mainCursor:mainCursor+2], 16)
        canMessageOffset[y] = mainCursor+2
        mainCursor = mainCursor + 2 + canIDAmount[y]*16

    if len(data) != mainCursor:
        logger.warning(f"企标秒包的解析错误，按照canid的顺序取走后有剩余字节")

    resp = {}

    for _canID in canIDDict:
        _index = CanIDList.index(_canID)

        signalMessageList = []
        for y in range(0, int(canIDAmount[_index])):
            _canMessage = data[canMessageOffset[_index] + y*16 : canMessageOffset[_index] + y*16 + 16]

            _fullSignalMessage = canDb.decode_message(_canID,binascii.unhexlify(_canMessage),0,0)

            _resp = []
            for _respSignalName in canIDDict[_canID]:
                _resp.append({_respSignalName: _fullSignalMessage[_respSignalName]})



            signalMessageList.append(_resp)
            resp[CanIDList[_index]] = signalMessageList

    return resp


def parse_tjms_signals_2_list(data, vehicleMode, protocol, canIDDict):

    time1 = time.time() * 1000
    canDb = candbPool[vehicleMode][protocol]

    CanIDList = EnterpriseTransportProtolVer[vehicleMode][protocol]

    CanIDListLen = len(CanIDList)

    mainCursor = 0

    canIDAmount = {}
    canMessageOffset = {}

    for y in range(CanIDListLen):
        canIDAmount[y] = int(data[mainCursor:mainCursor+2], 16)
        canMessageOffset[y] = mainCursor+2
        mainCursor = mainCursor + 2 + canIDAmount[y]*16

    if len(data) != mainCursor:
        logger.warning(f"企标秒包的解析错误，按照canid的顺序取走后有剩余字节")


    # 开始按照调用请求的canIDDict解析对应的signal值

    resp = []
    for _canID in canIDDict:        # 取到需要解析的canID
        _index = CanIDList.index(_canID)


        # 这个canID在秒包内共有多少个小包
        canIDSecAmount = int(canIDAmount[_index])

        # 取出来这个canID在这个秒包的全部数据
        canIDSecAllData = data[canMessageOffset[_index] : canMessageOffset[_index] + int(canIDAmount[_index])*16]


        # 需要解析的signalList和解析结果
        signalNameList = canIDDict[_canID]


        # for需要处理的某一个signal
        for signalName in signalNameList:
            signalValueList = []

            for microSecCount in range(0, canIDSecAmount):

                # 取出来第一个8字节
                _canMessage = canIDSecAllData[microSecCount*16:microSecCount*16 + 16]

                # 这个8字节的全部message
                # _fullSignalMessage = canDb.decode_message(_canID, binascii.unhexlify(_canMessage), 0, 0)
                _fullSignalMessage = canDb.decode_message(_canID, binascii.unhexlify(_canMessage), 0, True)

                signalValueList.append(_fullSignalMessage[signalName])

            resp.append((signalName, signalValueList))

    return resp


# if __name__ == "__main__":
#     data = '107d002710010000027d002710010000037d102710010000047d102710010000057d002710010000067d002710010000077d002710010000087d102710010000097cf827100100000a7d1027100100000b7d1027100100000c7d1027100100000d7d0027100100000e7d1027100100000f7d002710010000007d0027100100000100000100ff00ffffffff01010000ff00ff00000100000191000000020000080100000400ff8000010000018627101900e0000801cf000000ffc2ab080172100000320008080000000001240202aa0260007e01e90027103fff800401fe001e0078000004010000000b0000000001640000000000000201ff0000ff000000020100000000000000020100000000000000040164004e7e7d00004e00000001580800000007d00e01000000000000000001000000000000000001a20000058150000301000050000000000001808000000000aa000101000000c85200000100f0fffcfc00000000000100004810000012000197c97e573d2f00000140ff4c555a925e040182000064400000000100ffffffff00020001fff0000990000000017d0002fe08720000010001ff000000000501000100800000493b01c900000000400003000100171580006403000100400000fd3f606a01c2b8c2c057595857010000000000aa00000100000000000000000100000000000000000100000000008000000100000000000000000001000000093212000000000001000000000000000001000000000000000000000113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70000000000000000000000000000000000000000010202000000000000010000624110001e1e011c1024247ca07c0000000000000000'
#     vehicleMode = 'ME7'
#     protocol = '0e'
#     canIDDict = {'GW_IBS_0x335': ['IBS_SOC', 'IBS_SOH_SUL'], 'BMS_0x100': ['BMS_PackU']}
#     parse_tjms_signals_2_list(data, vehicleMode, protocol, canIDDict)
