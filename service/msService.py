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
import re
import orjson
import collections
from dbcfile.additionalCanDB import additionalCanDB


def getCanIDMessageFromFile(fullFilename, canIDDict):

    resp = {}
    resp['ICM_ODOTotal'] = []

    try:
        with open(fullFilename, 'r') as f:
            lines = f.readlines()

        # 防止tbox发报文卡滞，导致的MPU时间相同的两条上传包，MCU时间颠倒。这里引入根据MCU时间排序
        lines.sort(key=lambda x: x.split('MCUTime')[1])

        for line in lines:
            _json = orjson.loads(line)
            _canMessage = parse_tjms_signals_2_list(_json['contents']['MSSecondPacket'],
                                                    _json['vehicleMode'],
                                                    _json['contents']['MSPacketVer'],
                                                    canIDDict,
                                                    firstOnly=True,
                                                    signalsInvalidValueDict={'ICM_ODOTotal': 16777215}
                                                    )
            if _canMessage:
                resp['ICM_ODOTotal'].append((_json['contents']['MCUTime'], _canMessage[0][1][0]))
    except:
        pass

    return (resp['ICM_ODOTotal'])


def cropWarningAndTransformer2dict(oriAllMessage, startTime, endTime):
    startTimeStr = Timeutils.timeStamp2timeString(startTime)
    endTimeStr = Timeutils.timeStamp2timeString(endTime)

    messageList = []
    for line in oriAllMessage:
        _jsondata = orjson.loads(line)
        del(_jsondata['serverTime'])
        del(_jsondata['MPUTime'])
        del(_jsondata['TYPE_CMD'])
        del(_jsondata['MCUTimeDelay'])
        messageList.append(json.dumps(_jsondata))
    messageList.sort()

    messageListSorted = []
    for line in messageList:
        messageListSorted.append(orjson.loads(line))

    bufferCursor = 0

    # 找到需要处理的第一条
    find = False
    while bufferCursor < len(messageListSorted):

        if messageListSorted[bufferCursor].get("MCUTime") <= startTimeStr:
            # 跳过这条废数据
            bufferCursor += 1
        else:
            find = True
            break

    if find:
        logger.debug(f"找到了需要处理的第一条：content:{messageListSorted[bufferCursor]}")
    else:
        logger.debug(f'最后一个文件读完了，啥也没有')
        return {}

    respMessage = {}

    while messageListSorted[bufferCursor].get("MCUTime") < endTimeStr:
        _jsondata = messageListSorted[bufferCursor]

        # 6月1日至7月8日的企标报文中，不含有'vehicleMode'这个节点，所以默认补一个ME7，这里不严谨，后续要去掉或者根据其他条件修改
        _vehicleMode = _jsondata.get('vehicleMode') if _jsondata.get('vehicleMode') else 'ME7'

        seq=(_vehicleMode, _jsondata['contents']['MSPacketVer'], _jsondata['contents']['MSSecondPacket'])
        # respMessage[_jsondata['MCUTime']] = ','.join(seq)
        respMessage[_jsondata['MCUTime']] = seq

        # 如果buffer还没到底，就cursor+1
        if bufferCursor < (len(messageListSorted) - 1):
            bufferCursor += 1
        else:
            break
    return respMessage



def cropAndTransformer2dict(oriAllMessage, startTime, endTime, needSort=None):
    # 企标的补发报文，不是按照MCU时间顺序补发的，所以需要指定排序
    if needSort:
        oriAllMessage.sort(key=lambda x: x.split('MCUTime')[1])

    startTimeStr = Timeutils.timeStamp2timeString(startTime)
    endTimeStr = Timeutils.timeStamp2timeString(endTime)

    logger.debug(f"cropAndTransformer2dict: there have {len(oriAllMessage)} lines to do")

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

    # respMessage = {}
    respMessage = collections.OrderedDict()

    while oriAllMessage[bufferCursor].split('"MCUTime": "')[1][:19] <= endTimeStr:

        _jsondata = orjson.loads(oriAllMessage[bufferCursor])

        # 6月1日至7月8日的企标报文中，不含有'vehicleMode'这个节点，所以默认补一个ME7，这里不严谨，后续要去掉或者根据其他条件修改
        _vehicleMode = _jsondata.get('vehicleMode') if _jsondata.get('vehicleMode') else 'ME7'

        seq=(_vehicleMode, _jsondata['contents']['MSPacketVer'], _jsondata['contents']['MSSecondPacket'])
        
        # respMessage[_jsondata['MCUTime']] = ','.join(seq)
        respMessage[_jsondata['MCUTime']] = seq
        '''
        _MCUTime = oriAllMessage[bufferCursor].split('"MCUTime": "')[1][:19]
        if oriAllMessage[bufferCursor].find('vehicleMode') == -1:
            _vehicleMode = 'ME7'
        else:
            _vehicleMode = oriAllMessage[bufferCursor].split('"vehicleMode": "')[1].split('"')[0]
        _MSPacketVer = oriAllMessage[bufferCursor].split('"MSPacketVer": "')[1][:2]
        _MSSecondPacket = oriAllMessage[bufferCursor].split('"MSSecondPacket": "')[1].split('"')[0]

        seq = (_vehicleMode, _MSPacketVer, _MSSecondPacket)

        respMessage[_MCUTime] = seq
        '''

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

    # 增加misc报文为代表的补充message和signal 2021/11/30
    additionalDict = additionalCanDB.get_message_signals()
    for key in additionalDict.keys():
        respDict[key] = additionalDict[key]
    return respDict


def getSignalInfo(signalName, vehicleModel, fullCanDBDict=None):

    if vehicleModel == 'ME7':
        canDB = candbPool[vehicleModel]['0a']
    else:
        canDB = candbPool[vehicleModel]['01']

    # 如果传入了fullCanDBDict，就不需要在生成一遍，为了提速和兼容 2021/11/03
    if not fullCanDBDict:
        fullCanDBDict = genMessagesSignals(canDB)

    resp = {}
    for x in fullCanDBDict:
        if signalName in fullCanDBDict[x]:
            try:
                signalInfo = canDB.get_message_by_name(x).get_signal_by_name(signalName)
                messageCycleTime = canDB.get_message_by_name(x).cycle_time
            except:
                print(f"要从补充的canDB中找这个的信息 {signalName}")
                signalInfo = additionalCanDB.get_signal(signalName)
                messageCycleTime = additionalCanDB.get_cycle_time(signalName)


            resp['canID'] = x
            resp['comment'] = signalInfo.comment
            resp['choices'] = signalInfo.choices
            resp['maximum'] = signalInfo.maximum
            resp['minimum'] = signalInfo.minimum
            resp['cycle_time'] = messageCycleTime

            graphType = 'smooth'

            if signalInfo.choices:
                for k,v in signalInfo.choices.items():
                    if v.lower() == 'invalid':
                        resp['invalid'] = k
                    if v.lower() == 'init':
                        resp['init'] = k
                    if v.lower() == 'reserved':
                        resp['reserved'] = k
                    if v.lower() == 'failure':
                        resp['failure'] = k

                    if v.lower() not in ['invalid', 'init', 'reserved', 'failure']:
                        graphType = 'poly'

            resp['graphType'] = graphType
            break

    return resp



def findSignalInfo(signalName, vehicleModel):
    if vehicleModel == 'ME7':
        canDB = candbPool[vehicleModel]['0a']
    else:
        canDB = candbPool[vehicleModel]['01']

    fullCanDBDict = genMessagesSignals(canDB)

    return fuzzy_finder(signalName, fullCanDBDict)



def fuzzy_finder(key, data):
    """
    模糊查找器
    :param key: 关键字
    :param data: 数据
    :return: list
    """

    # 结果列表
    suggestions = []
    # 非贪婪匹配，转换 'djm' 为 'd.*?j.*?m'
    # pattern = '.*?'.join(key)
    pattern = '.*%s.*'%(key)
    # print("pattern",pattern)
    # 编译正则表达式
    regex = re.compile(pattern, re.IGNORECASE)
    for canID, signalList in data.items():
        # print("item",item['name'])
        # 检查当前项是否与regex匹配。
        # 先搜索canID部分
        match = regex.search(str(canID))
        if match:
            _content = {
                "canID": canID,
                "signalName": None
            }
            suggestions.append(_content)
            # 添加这个canID下的全部信号
            for _signal in data.get(canID):
                _content = {
                    "canID": "|--------",
                    "signalName": _signal
                }
                suggestions.append(_content)

        for _c in signalList:
            match = regex.search(str(_c))
            if match:
                # 如果匹配，就添加到列表中
                _content = {
                    "canID": canID,
                    "signalName": _c
                }
                suggestions.append(_content)

    # 返回前10条匹配结果，改成返回全部匹配结果，由前端展示前10条
    # return suggestions[:10]
    return suggestions



def getCanIDListBySignalList(signalList, vehicleMode, msUploadProtol):
    '''
    从redis获取full然后jsonload很慢，还是直接解析来的快
    :param signalList:
    :param vehicleMode:
    :param msUploadProtol:
    :return:
    '''
    if vehicleMode == 'ME7':
        if msUploadProtol:
            canDB = candbPool[vehicleMode][msUploadProtol]
        else:
            canDB = candbPool[vehicleMode]['0e']
    else:
        if msUploadProtol:
            canDB = candbPool[vehicleMode][msUploadProtol]
        else:
            canDB = candbPool[vehicleMode]['01']

    fullCanDBDict = genMessagesSignals(canDB)

    canIDDict = {}
    signalInfoDict = {}
    errorSign = False
    for sig in signalList:
        for x in fullCanDBDict:
            if sig in fullCanDBDict[x]:
                if canIDDict.get(x):
                    canIDDict[x].append(sig)
                else:
                    canIDDict[x] = [sig]
                break
        else:
            errorSign = True

        signalInfoDict[sig] = getSignalInfo(signalName=sig, vehicleModel=vehicleMode, fullCanDBDict=fullCanDBDict)

    # canIDDict = {
    #     'BMS_0x100': ['BMS_PackU', 'BMS_PackI'],
    #     'BCM_0x310': ['BCM_FL_Door_Sts', 'BCM_FR_Door_Sts', 'BCM_RL_Door_Sts', 'BCM_RR_Door_Sts']
    # }


    if errorSign:
        return None, None
    else:
        return canIDDict, signalInfoDict



def parse_tjms_signals(data, vehicleMode, protocol, canIDDict):


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


def parse_tjms_signals_2_list(data, vehicleMode, protocol, canIDDict, firstOnly=False, signalsInvalidValueDict={}):

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
    # print(canIDDict)
    # {'GW_IBS_0x335': ['IBS_SOH_SUL']}
    for _canID in canIDDict:        # 取到需要解析的canID
        _index = CanIDList.index(_canID)

        # 这个canID在秒包内共有多少个小包
        canIDSecAmount = int(canIDAmount[_index])

        # 取出来这个canID在这个秒包的全部数据
        canIDSecAllData = data[canMessageOffset[_index]:canMessageOffset[_index] + int(canIDAmount[_index])*16]

        # 需要解析的signalList和解析结果
        signalNameList = canIDDict[_canID]

        # 根据firstOnly，来决定canIDSecAmount从哪里开始。
        # 如果只需要解析关键报文里的一个8字节，取最后的那个8字节
        if firstOnly:
            if canIDSecAmount > 0:
                where2Start = canIDSecAmount -1
            else:
                where2Start = 0
        else:
            where2Start = 0


        for microSecCount in range(where2Start, canIDSecAmount):

            # 取出来某个8字节
            _canMessage = canIDSecAllData[microSecCount*16:microSecCount*16 + 16]

            # 这个8字节的全部message
            _fullSignalMessage = canDb.decode_message(_canID, binascii.unhexlify(_canMessage), 0, True)
            # Like this:
            # {'IBS_SOC': 255, 'IBS_SOH_SUL': 255, 'IBS_SOFV_StopEnable': 7.1875, 'IBS_SOFV_Restart': 7.625, 'IBS_SOH_LAM': 127.5, 'IBS_SOH_COR': 9.875, 'IBS_SOFV_Restart_STATE': 1, 'IBS_SOFV_StopEnable_STATE': 1, 'IBS_SOH_SUL_STATE': 3, 'IBS_SOC_STATE': 3, 'IBS_SOH_COR_STATE': 2, 'IBS_SOH_LAM_STATE': 3}

            # for需要处理的某一个signal
            for signalName in signalNameList:
                signalInvalidValue = signalsInvalidValueDict.get(signalName)
                signalValueList = []
                # print(_fullSignalMessage[signalName])
                # print(f"signalInvalidValue:{signalInvalidValue}")
                if signalInvalidValue == _fullSignalMessage[signalName]:
                    # print("find a invalid value, skip it!")
                    pass
                else:
                    signalValueList.append(_fullSignalMessage[signalName])
                    resp.append((signalName, signalValueList))


    return resp


# if __name__ == "__main__":
#     data = '107d002710010000027d002710010000037d102710010000047d102710010000057d002710010000067d002710010000077d002710010000087d102710010000097cf827100100000a7d1027100100000b7d1027100100000c7d1027100100000d7d0027100100000e7d1027100100000f7d002710010000007d0027100100000100000100ff00ffffffff01010000ff00ff00000100000191000000020000080100000400ff8000010000018627101900e0000801cf000000ffc2ab080172100000320008080000000001240202aa0260007e01e90027103fff800401fe001e0078000004010000000b0000000001640000000000000201ff0000ff000000020100000000000000020100000000000000040164004e7e7d00004e00000001580800000007d00e01000000000000000001000000000000000001a20000058150000301000050000000000001808000000000aa000101000000c85200000100f0fffcfc00000000000100004810000012000197c97e573d2f00000140ff4c555a925e040182000064400000000100ffffffff00020001fff0000990000000017d0002fe08720000010001ff000000000501000100800000493b01c900000000400003000100171580006403000100400000fd3f606a01c2b8c2c057595857010000000000aa00000100000000000000000100000000000000000100000000008000000100000000000000000001000000093212000000000001000000000000000001000000000000000000000113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70000000000000000000000000000000000000000010202000000000000010000624110001e1e011c1024247ca07c0000000000000000'
#     vehicleMode = 'ME7'
#     protocol = '0e'
#     canIDDict = {'GW_IBS_0x335': ['IBS_SOC', 'IBS_SOH_SUL'], 'BMS_0x100': ['BMS_PackU']}
#     parse_tjms_signals_2_list(data, vehicleMode, protocol, canIDDict)
