import time
import datetime, math, os, json, psutil
from flask import Blueprint, request
from common.setlog2 import logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode, EnterpriseTransportProtolVer
from multiprocessing import Pool
import cantools
import numpy, binascii

# candb = cantools.db.load_file('dbcfile/ME7_TboxCAN_CMatrix_V307.210409_400km_SOP+6_TBOX.DBC',cache_dir='./cache')
candb_0e = cantools.db.load_file('dbcfile/ME7_TboxCAN_CMatrix_V307.210409_400km_SOP+6_TBOX.DBC')
candbPool = {
    'ME7': {
        '0c': candb_0e,
        '0d': candb_0e,
        '0e': candb_0e,
        '0f': candb_0e
    }
}

def genMessagesSignals(canDB):
    respDict = {}
    messages = canDB.messages
    for _msg in messages:
        signals = _msg.signals
        respDict[_msg.name] = {_sig.name for _sig in signals}
    return respDict



def getCanIDListBySignalList(signalList, vehicleMode):
    canDB = candbPool[vehicleMode]['0e']
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

