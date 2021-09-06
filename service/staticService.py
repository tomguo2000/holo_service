import json

def errorRecordFlow(contents):
    waterFlowRecords = []
    for vin in contents:
        for item in contents[vin]:
            # 以下是vin发起的一次完整的诊断记录
            _vin = item['vin']
            _timeStr = item['uploadTime']
            detail = item['details']['ecus']
            if len(detail) < 4:
                # 不足4个ecu，判定是HU发起的诊断，忽略记录
                pass
            else:
                # 这是tbox发起的诊断，需要进行统计
                _errorEcuName = []
                for ecu in detail:

                    if not ecu.get('ecuPn') or not ecu.get('ecuHv') or not ecu.get('ecuSv'):
                        # 这是一次诊断失败，有空值
                        # print('Fail', ecu['ecuN'], ecu['ecuPn'],ecu['ecuHv'],ecu['ecuSv'])
                        _errorEcuName.append(ecu['ecuN'])

                    else:
                        # 这是一次成功诊断，完美
                        # print(ecu['ecuN'], ecu['ecuPn'],ecu['ecuHv'],ecu['ecuSv'])
                        pass

                # 出错的诊断，加入waterFlowRecords
                # if _errorEcuName:
                waterFlowRecords.append(
                    {
                        "timeStr": _timeStr,
                        "vin": _vin,
                        "errorEcuName": _errorEcuName,
                        "fullEcuInfo": detail
                    }
                )


    b=[]
    for x in waterFlowRecords:
        b.append(json.dumps(x))

    # b.sort(reverse=True)
    b.sort()

    waterFlowRecords = []
    for x in b:
        waterFlowRecords.append(json.loads(x))

    return waterFlowRecords



def static_ecu_ver(contents):
    waterFlowRecords = []
    for vin in contents:
        for item in contents[vin]:
            # 以下是vin发起的一次完整的诊断记录
            _vin = item['vin']
            _timeStr = item['uploadTime']
            detail = item['details']['ecus']
            if len(detail) < 4:
                # 不足4个ecu，判定是HU发起的诊断，忽略记录
                pass
            else:
                # 这是tbox发起的诊断，需要进行统计
                _errorEcuName = []
                for ecu in detail:

                    if not ecu.get('ecuPn') or not ecu.get('ecuHv') or not ecu.get('ecuSv'):
                        # 这是一次诊断失败，有空值
                        # print('Fail', ecu['ecuN'], ecu['ecuPn'],ecu['ecuHv'],ecu['ecuSv'])
                        _errorEcuName.append(ecu['ecuN'])

                    else:
                        # 这是一次成功诊断，完美
                        # print(ecu['ecuN'], ecu['ecuPn'],ecu['ecuHv'],ecu['ecuSv'])
                        pass

                _errorEcuAmount = len(_errorEcuName)

                waterFlowRecords.append(
                    {
                        "vin": _vin,
                        "timeStr": _timeStr,
                        "errorEcuAmount": _errorEcuAmount,
                        "errorEcuName": _errorEcuName
                    }
                )

    # waterFlowRecords.sort()

    print(waterFlowRecords)

    total = len(waterFlowRecords)
    totalVin = len(contents.keys())
    rightRec = 0
    errorRec = 0
    errorEcuNameCount = {}
    errorVINRecords = {}
    for item in waterFlowRecords:
        if item['errorEcuAmount'] == 0:
            rightRec += 1
        else:
            errorRec += 1

            if errorVINRecords.get(item['vin']):
                errorVINRecords[item['vin']].append(item['timeStr'])
            else:
                errorVINRecords[item['vin']] = [item['timeStr']]

            for _errorEcuName in item['errorEcuName']:
                errorEcuNameCount[_errorEcuName] = errorEcuNameCount.get(_errorEcuName, 0) + 1

    resp = {
        "totalVins": totalVin,
        # "vinsList": [key for key in contents],
        "totalRecords": total,
        "rightRecords": rightRec,
        # "errorVINRecords": errorVINRecords,
        "errorRecords": errorRec,
        "errorEcuNameCount": errorEcuNameCount
    }
    return resp
