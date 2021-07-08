import time, datetime, math
from common.timeUtils import Timeutils


class XaxisService(object):
    def __init__(self, startTime=None, endTime=None, interval=30):
        self.startTime = startTime if not startTime else int(time.time()*1000 - 7*24*3600*1000)
        self.endTime = endTime if not endTime else int(time.time()*1000)
        self.interval = interval

    @classmethod
    def settingXaxis(self):
        # 调整startTimeStamp向上到workingStartTimeStamp
        # 秒清0，分钟是否被3整除，如不能就分钟减1再试
        tempTimeArray = Timeutils.timeStamp2timeArray(startTimeStamp)

        delta = datetime.timedelta(seconds=tempTimeArray.second, microseconds=tempTimeArray.microsecond)
        workingStartTime = tempTimeArray - delta
        while workingStartTime.minute % interval != 0:
            workingStartTime = workingStartTime - datetime.timedelta(minutes=1)
        workingStartTimeStamp = Timeutils.timeArray2timeStamp(workingStartTime, ms=True)

        tempTimeArray = Timeutils.timeStamp2timeArray(endTimeStamp)
        delta = datetime.timedelta(seconds=60 - tempTimeArray.second, microseconds=-tempTimeArray.microsecond)
        workingEndTime = tempTimeArray + delta
        while workingEndTime.minute % interval != 0:
            workingEndTime = workingEndTime + datetime.timedelta(minutes=1)
        workingEndTimeStamp = Timeutils.timeArray2timeStamp(workingEndTime, ms=True)

        # xAxisTotal: X轴的点位数量
        xAxisTotal = math.ceil((workingEndTimeStamp - workingStartTimeStamp) / (interval * 60 * 1000))

        # respXaxis: 用于绘图的X轴的坐标list
        respXaxis = []
        timeCursor = workingStartTimeStamp
        while xAxisTotal:
            respXaxis.append(timeCursor)
            timeCursor += interval * 60 * 1000
            xAxisTotal -= 1

        # # pathTotal: 涉及到的文件的数量
        # pathTotal = workingEndTime.toordinal() - workingStartTime.toordinal() +1
        # fullPathList = []
        # pathCursor = workingStartTime
        # while pathTotal:
        #     _path = str(pathCursor.year) + '-' + str(pathCursor.month).zfill(2) + '-' + str(pathCursor.day).zfill(2)
        #     fullPathList.append(os.path.join(CONFIG['Storage_Prefix'], VIN, _path))
        #     pathCursor = pathCursor + datetime.timedelta(days=1)
        #     pathTotal -= 1

        # 返回workingStartTime, workingEndTime, respXaxis[]
        return {
            'VIN': VIN,
            'workingStartTimeStamp': workingStartTimeStamp,
            'workingEndTimeStamp': workingEndTimeStamp,
            'respXaxis': respXaxis,
            # 'fullPathList': fullPathList,
            'totalXaxis': len(respXaxis)
            # 'totalFullPath': len(fullPathList)
        }