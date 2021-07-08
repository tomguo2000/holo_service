'''
    自定义时间转换工具类
    V1.1.2
    增加了@classmethod在全部函数前，支持使用方直接 Timeutils.serverTimeNow()
    增加了todayStartTimeStamp方法，支持获取今天的起始时间戳
    增加了serverTimeNow的方法，支持毫秒的字符输出
    By Guoliang
    05/31/2021
'''
import datetime
import time


class Timeutils(object):
    def __init__(self):
        pass

    @classmethod
    def todayStartTimeStamp(self, ms=None):
        "input: ms=True     output: 1622390400000"
        _ts = time.time()
        _tsString = time.strftime("%Y-%m-%d", time.localtime(_ts))
        _todayStartTS = int(time.mktime(time.strptime(_tsString, "%Y-%m-%d")))
        if ms:
            _todayStartTS = _todayStartTS * 1000
        return _todayStartTS

    @classmethod
    def serverTimeNow(self, ms=3):
        "input: ms=3     output: '2021-05-13_08:27:51.123'"
        timeArray = datetime.datetime.now()
        resp = "%04d" % timeArray.year
        resp += "-%02d" % timeArray.month
        resp += "-%02d" % timeArray.day
        resp += "_"
        resp += "%02d" % timeArray.hour
        resp += ":%02d" % timeArray.minute
        resp += ":%02d" % timeArray.second
        if ms:
            # 由于原始精度，是6位长度的microsecond,所以这里用6-
            microsecond = timeArray.microsecond/(10**(6-ms))
            resp += f".%0{ms}d" % microsecond
        return resp

    @classmethod
    def timeStamp2timeArray(self, timeStamp):
        "input:1620865671 or 1620865671999. output: datetime.datetime(obj)"
        # input: 1620865671          output: datetime.datetime(2021, 5, 13, 8, 27, 51)
        # input: 1620865671000       output: datetime.datetime(2021, 5, 13, 8, 27, 51)
        # input: 1620865671999       output: datetime.datetime(2021, 5, 13, 8, 27, 51, 999000)
        # input: 1620865671.888      output: datetime.datetime(2021, 5, 13, 8, 27, 51, 888000)

        if int(timeStamp) > 9999999999:
            return datetime.datetime.fromtimestamp(timeStamp / 1000)
        else:
            return datetime.datetime.fromtimestamp(timeStamp)

    @classmethod
    def timeStamp2timeString(self, timeStamp, format="%Y-%m-%d_%H:%M:%S"):
        "input: 1620865671          output: '2021-05-13_08:27:51"
        # input: 1620865671          output: '2021-05-13_08:27:51'
        # input: 1620865671999       output: '2021-05-13_08:27:51'
        # input: 1620865671.888      output: '2021-05-13_08:27:51'
        # input: 'a'                 output: Exception

        if timeStamp > 9999999999:
            timeStamp = timeStamp / 1000

        timeArray = time.localtime(timeStamp)
        return time.strftime(format, timeArray)

    @classmethod
    def timeArray2timeStamp(self, timeArray, ms=False):
        if ms:
            return int((time.mktime(timeArray.timetuple())) * 1000)
        else:
            return int(time.mktime(timeArray.timetuple()))

    @classmethod
    def timeArray2timeString(self, timeArray, format="%Y-%m-%d_%H:%M:%S"):
        "datetime.datetime ---> string"
        return timeArray.strftime(format)

    @classmethod
    def timeString2timeArray(self, timeString, format="%Y-%m-%d_%H:%M:%S"):
        "string ---> datetime.datetime"
        return datetime.datetime.strptime(timeString, format)

    @classmethod
    def timeString2timeStamp(self, timeString, format="%Y-%m-%d_%H:%M:%S", ms=False):
        if ms:
            return int(time.mktime(time.strptime(timeString, format)) * 1000)
        else:
            return int(time.mktime(time.strptime(timeString, format)))

    @classmethod
    def timeNS2timeString(self, national_std):
        "150a0f0a0B2F ---> 2021-10-15_10:11:47"
        # length must be 12
        if len(national_std) != 12: raise Exception("format ERROR: must be 12 chars")

        x = (int(national_std[0:2], 16))
        if x > 99:
            raise Exception("Year format ERROR")
        else:
            x = str(x)
        x = '0' + x if len(x) < 2 else x

        y = (int(national_std[2:4], 16))
        if y > 12:
            raise Exception("Month format ERROR")
        else:
            y = str(y)
        y = '0' + y if len(y) < 2 else y

        z = (int(national_std[4:6], 16))
        if z > 31:
            raise Exception("Day format ERROR")
        else:
            z = str(z)
        z = '0' + z if len(z) < 2 else z

        a = (int(national_std[6:8], 16))
        if a > 60:
            raise Exception("Hour format ERROR")
        else:
            a = str(a)
        a = '0' + a if len(a) < 2 else a

        b = (int(national_std[8:10], 16))
        if b > 60:
            raise Exception("Min format ERROR")
        else:
            b = str(b)
        b = '0' + b if len(b) < 2 else b

        c = (int(national_std[10:12], 16))
        if c > 60:
            raise Exception("Second format ERROR")
        else:
            c = str(c)
        c = '0' + c if len(c) < 2 else c

        return '20' + x + '-' + y + '-' + z + '_' + a + ':' + b + ':' + c

    @classmethod
    def timeNS2timeStamp(self, national_std, ms=False):
        "15050f0a0B2F ---> 1621044707"
        timestring = self.timeNS2timeString(national_std)
        return self.timeString2timeStamp(timestring, ms=ms)
        pass


class Timedelta(datetime.timedelta):
    " x = Timedelta(days=3, hours=-1)"
    # Timedelta(
    #   days  = n,
    #   hours = n,
    #   minutes = n,
    #   seconds = n,
    #   microseconds = n
    # )
    pass
