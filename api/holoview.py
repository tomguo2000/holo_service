import time
import datetime, math, os, json, psutil
from flask import Blueprint, request, render_template, make_response
from common.setlog2 import logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode,EnterpriseTransportProtolVer
from multiprocessing import Pool
import service.public, service.msService
import gc
import base64
import orjson
import collections
from dbcfile.additionalCanDB import additionalCanDB
import bisect
import random


holoview = Blueprint("holoview", __name__)


@holoview.route('/checkSignal', methods=["GET"])
def holoview_checkSignal():
    try:
        # 检查入参
        try:
            params = request.args.to_dict()
            print("holoview_checkSignal:", params)
            vehicleModel = params['vehicleModel']
            signalName = params['signalName']

            if vehicleModel not in ['ME7', 'ME5']:
                raise

        except:
            raise Exception ("110900", "亲，选个车型先啊。。。")

        signalInfo = service.msService.getSignalInfo(signalName=signalName, vehicleModel=vehicleModel)

        if not signalInfo:
            return {
                       "code": 400,
                       "message": "没有找到了这个信号的信息，小天认为你喝酒了",
                       "businessObj": None
                   }, 200
        else:
            return {
                       "code": 200,
                       "message": "找到了这个信号的信息，小天认为你做的很好",
                       "businessObj": signalInfo
                   }, 200
    except Exception as ex:
        return {
                   "code": ex.args[0],
                   "message": ex.args[1] if len(ex.args) > 1 else ReturnCode[ex.args[0]],
                   "businessObj": None
               }, 200



@holoview.route('/findSignal', methods=["GET"])
def holoview_findSignal():
    try:
        # 检查入参
        try:
            params = request.args.to_dict()
            vehicleModel = params['vehicleModel']
            signalName = params['signalName']

            if vehicleModel not in ['ME7', 'ME5']:
                raise

        except:
            raise Exception ("110900", "亲，选个车型先啊。。。")

        signalInfo = service.msService.findSignalInfo(signalName=signalName, vehicleModel=vehicleModel)

        if not signalInfo:
            respMessage = [
                "没有找到了这个信号的信息，小天认为你喝酒了",
                "啥也搜不到，去给小姐姐买杯咖啡提提神吧",
                "实在搜不出你要的东西，小天尽力了，发会呆。。。",
                "没有没有没有，你搜的啥啊，就是没有没有没有",
            ]
            return {
                       "code": 400,
                       "message": respMessage[int(random.random()*len(respMessage))],
                       "businessObj": None
                   }, 200
        else:
            return {
                       "code": 200,
                       "message": "找到了一些，小天认为你做的很好",
                       "businessObj": signalInfo
                   }, 200
    except Exception as ex:
        return {
                   "code": ex.args[0],
                   "message": ex.args[1] if len(ex.args) > 1 else ReturnCode[ex.args[0]],
                   "businessObj": None
               }, 200


@holoview.route('/overall', methods=["GET"])
def holoview_getOverall():
    overall = {
        "event_ConnStatusList": "Tbox到平台连接事件",
        "event_VehicleLoginList": "车辆登录Vehicle服务事件",
        "event_RemoteCmdList": "远程控车指令事件",
        "message_tj32960Login": "国标登录报文",
        "message_tj32960Live": "国标实发报文",
        "message_tj32960Resent": "国标补发报文",
        "message_MSLive": "企标实发报文",
        "message_MSResent": "企标补发报文",
        "message_MSWarning": "企标告警报文",
        "message_MiscList": "Misc报文",
        "message_HeartbeatList": "心跳报文"
    }
    return {
               "code": 200,
               "message": "获取整体指标成功",
               "businessObj": overall
           }, 200


@holoview.route('/help', methods=["GET"])
def holoview_help():
    with open('static/imgs/xiaotian_1.png', 'rb') as f:
        img_base64data = base64.b64encode(f.read())
        imgData_1 = img_base64data.decode()
        imgData_1 = "data:image/png;base64," + imgData_1

    with open('static/imgs/xiaotian_2.png', 'rb') as f:
        img_base64data = base64.b64encode(f.read())
        imgData_2 = img_base64data.decode()
        imgData_2 = "data:image/png;base64," + imgData_2

    with open('static/imgs/xiaotian_3.png', 'rb') as f:
        img_base64data = base64.b64encode(f.read())
        imgData_3 = img_base64data.decode()
        imgData_3 = "data:image/png;base64," + imgData_3

    return render_template('holoview_help.html', img1data=imgData_1, img2data=imgData_2, img3data=imgData_3)


@holoview.route('/', methods=["GET"])
def holoview_index():
    try:
        # 检查入参
        try:
            params = request.args.to_dict()
            logger.info(f"有人调用holoview了，参数如下:{params}")

            vin = params['vin']
            vehicleModel = params.get('vehicleModel')
            date = params.get('date')
            startTime = params.get('startTime')
            endTime = params.get('endTime')
            env = params.get('env')
            overall = params.get('overall')
            signal = params.get('signal')
            Xscale = params.get('Xscale')
            skipInvalidValue = params.get('skipInvalidValue') if params.get('skipInvalidValue') else False

            logger.debug(f"初始访问的请求参数: {params}")

            '''
            参数说明
            vin                 必选
            date                日期，可选，无默认值
            startTime           起始时间戳
            endTime             结束时间戳
            overall             整体指标，可选。后台默认值是event_ConnStatusList
            signal              信号指标，可选。后台默认值是IBS有关的10个信号
            Xscale              时间刻度级数，可选。取值1-17，目前开放5-11

            skipInvalidValue    信号指标是否跳过非法值，可选。后台默认是False
            如果同时传了日期和时间戳，已时间戳为准
            '''

            if not startTime:
                startTime = Timeutils.timeString2timeStamp(date, format="%Y-%m-%d", ms=True)
            if not endTime:
                endTime = startTime + (86400-2) * 1000

            if int(startTime) > 9999999999:     # 传入的是ms，取个整
                startTime = int(int(startTime) / 1000) * 1000
                # startTime = int(startTime)
            else:
                startTime = int(startTime) * 1000

            if int(endTime) > 9999999999:       # 传入的是ms，向后取个整
                endTime = int(int(endTime) / 1000 + 1) * 1000
                # endTime = int(endTime)
            else:
                endTime = int(endTime) * 1000

            # 根据startTime和endTime和Xscale，决定Xscale和Xinterval
            openedXscale = {
                # '1': 3600,      # 60m
                # '2': 1800,      # 30m
                # '3': 600,       # 10m
                # '4': 300,       # 5m
                # '5': 120,       # 2m
                '6': 60,        # 1m
                '7': 30,        # 30s
                '8': 20,        # 20s
                '9': 10,        # 10s
                '10': 5,         # 5s
                '11': 2,        # 2s
                '12': 1,        # 1s
                '13': 0.5,      # 500ms
                '14': 0.2,      # 200ms
                '15': 0.1,      # 100ms
                '16': 0.05,     # 50ms
                '17': 0.02,     # 20ms
                '18': 0.01,     # 10ms
            }

            if not Xscale:  # 如果没传，则判断是框选的时间段
                _interval = (endTime - startTime)/1000/720   # 设1440格，每格约多少秒
                bestXscale = [x for x in openedXscale.keys()][-1]           # 先取一个最小值
                for k,v in openedXscale.items():
                    if v > _interval:
                        continue
                    else:
                        bestXscale = k
                        break

                Xscale = bestXscale

            # 根据Xscale决定X轴时间刻度的间隔（秒）
            Xinterval = openedXscale.get(Xscale)

            # 默认添加event_ConnStatusList到overallList里面，用于UI的连接/断开的背景色
            overallList = overall.split(',') if overall else []
            if 'event_ConnStatusList' not in overallList:
                overallList.insert(0, 'event_ConnStatusList')

            signalList = signal.split(',') if signal else []

            # firstOnly 用来表示是否只处理 某canID在秒包里的第一个8字节信息？
            # 为True适用于较大时间刻度时, False适用于小时间刻度。
            if Xinterval >= 1:
                firstOnly = True
            else:
                firstOnly = False

        except:
            raise Exception ("110900")

        # 再兼容一会ibs_reveal,如果没有vehicleMode，从signalList里裁剪出一个 2021/10/24
        if not vehicleModel:
            realSignalList = []
            for x in signalList:
                vehicleModel = x.split('_')[0]
                realSignalList.append(x[4:])
            signalList = realSignalList[:]


        # 如果没选择配置文件，提示不干
        if len(signalList) + len(overallList) <= 1:
            raise Exception("110900", "帅气的小哥哥，选了vin码，还要加载个配置文件再来问我")


        # # 如果查询的数据是30天前，提示不干活
        # if startTime < Timeutils.todayStartTimeStamp(ms=True) - 30*86400*1000:
        #     raise Exception("110900", "让小天给你查30天以前的东西，你得加钱才能办")

        # 控制查询的时间段，不要跨太多天
        if (endTime - startTime) / (86400*1000) > 10:
            raise Exception("110900", "让小天查10天跨度的东西，累死了，不干！")

        # 如果查询的日期早于2021/06/01，提示不知道
        if startTime < Timeutils.timeString2timeStamp('2021-06-01_00:00:00', ms=True):
            raise Exception("110900", "让小天查2021年6月1日前的数据？小天那时候还不记事呢！")


        # # 判断不要跨天，否则不干活
        # if Timeutils.timeStamp2timeString(startTime)[:10] != Timeutils.timeStamp2timeString(endTime)[:10]:
        #     raise Exception ("110903")
        # else:
        #     date = Timeutils.timeStamp2timeString(startTime)[:10]

        # 现在的做法是允许跨天，调用方自己控制

        # 得到日期List，用于拼接数据源的path
        dateList = service.public.createDateListByDuration(startTimestamp=startTime, endTimestamp=endTime)

        # 检查对应的目录，是否包含gz文件，如果是，提示数据已经被封印，运气爆棚，不用咒语就解封完毕。。。再来一次吧
        if not service.public.checkPathArchived(vin, dateList, env):
            raise Exception("110900", "大侠，你查的日期太久了，数据已经被封印! 不过你今天运气好，不用咒语就已经解封，几秒钟再查一次就有了")

        time0 = time.time()*1000
        logger.debug(f"0：可以了，咱现在从头开始。。。。。。{time0}")

        # 构建一个X轴
        Xaxis = createXaxis(startTime, endTime, interval=Xinterval)
        logger.debug(f"1：构建一个X轴完毕。。。{time.time()*1000-time0}")

        # Xaxis的点数超过10000，就抛异常
        XaxisTotalPlots = len(Xaxis)
        if XaxisTotalPlots > 10000:
            raise Exception("110900", "让小天给你返10000个点的信息，不是你把我累死就是你被浏览器拖死，不行！")

        # 定义返回的resp
        resp = {}
        resp['Xaxis'] = Xaxis
        resp['XaxisTotalPlots'] = len(Xaxis)
        resp['dateList'] = dateList

        # 2021/10/17新增，给前端描述缩放比例的字段
        resp['Xscale'] = {}
        resp['Xscale']['currentXscaleKey'] = Xscale
        resp['Xscale']['XscaleKeys'] = [_x for _x in openedXscale.keys()]
        resp['Xscale']['XscaleDict'] = openedXscale

        resp['YaxisList'] = []
        resp['YaxisOverall'] = {}
        resp['YaxisSignal'] = {}
        resp['YaxisSignal_RealValue'] = {}



        # 传入X轴和dateList，获取emq连接的event结果
        if "event_ConnStatusList" in overallList:
            time1 = time.time()*1000
            event_ConnStatusList = getConnStatus(vin, Xaxis, dateList, env=env)
            logger.debug(f"hhhh获取emq连接的event结果 完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['event_ConnStatusList'] = event_ConnStatusList
            resp['YaxisList'].append({"event_ConnStatusList": {
                "type": "event",
                "choices": {
                    "1": "client.connected",
                    "0": "client.disconnected"
                },
                "maximum": 1,
                "minimum": 0,
                "graphType": 'poly',
                "comment": '判断Tbox和MQTT的连接状态',
                "cycle_time": '原始频率 事件型'
            }})
            logger.debug(f"2-1：event_ConnStatusList的event结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取SDK初始化的聚合结果
        if "event_VehicleLoginList" in overallList:
            time1 = time.time()*1000
            event_VehicleLoginList = getVehicleLoginEvents(vin, Xaxis, dateList, env=env)
            logger.debug(f"hhhh获取SDK初始化的聚合结果 完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['event_VehicleLoginList'] = event_VehicleLoginList
            resp['YaxisList'].append({"event_VehicleLoginList": {
                "type": "message",
                "choices": {
                    "1": "登录车辆服务",
                    "0": "无"
                },
                "maximum": 1,
                "minimum": 0,
                "graphType": 'poly',
                "comment": 'Tbox登录TSP车辆服务的动作',
                "cycle_time": '原始频率 事件型'
            }})
            logger.debug(f"2-2：event_VehicleLoginList的event结果完毕。。。{time.time()*1000-time0}")

        # 传入X轴和dateList，获取控车event的结果
        if "event_RemoteCmdList" in overallList:
            time1 = time.time()*1000
            event_RemoteCmdList = getRemoteCmdEvents(vin, Xaxis, dateList, env=env)
            logger.debug(f"hhhh获取控车event的结果 完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['event_RemoteCmdList'] = event_RemoteCmdList
            resp['YaxisList'].append({"event_RemoteCmdList": {
                "type": "message",
                "choices": {
                    "1": "登录车辆服务",
                    "0": "无"
                },
                "maximum": 10,
                "minimum": 0,
                "graphType": 'smooth',
                "comment": '4G控车的动作',
                "cycle_time": '原始频率 事件型'
            }})
            logger.debug(f"2-3：event_RemoteCmdList 的event结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取国标的报文条数结果
        if "message_tj32960Live" in overallList:
            time1 = time.time()*1000
            message_tj32960Live = getTJ32960(vin, Xaxis, 'message_national_live.txt', dateList, env=env)
            logger.debug(f"hhhh获取国标的报文条数结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_tj32960Live'] = message_tj32960Live
            resp['YaxisList'].append({"message_tj32960Live": {
                "type": "message",
                "choices": {},
                "maximum": 10,
                "minimum": 0,
                "graphType": 'smooth',
                "comment": '国标实发报文条数',
                "cycle_time": '原始频率 10秒'
            }})
            logger.debug(f"2-4：message_tj32960Live 的event结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取国标补发的报文条数结果
        if "message_tj32960Resent" in overallList:
            time1 = time.time()*1000
            message_tj32960Resent = getTJ32960(vin, Xaxis, 'message_national_resent.txt', dateList, env=env)
            logger.debug(f"hhhh获取国标的报文条数结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_tj32960Resent'] = message_tj32960Resent
            resp['YaxisList'].append({"message_tj32960Resent": {
                "type": "message",
                "choices": {},
                "maximum": 100,
                "minimum": 0,
                "graphType": 'smooth',
                "comment": '国标补发报文条数',
                "cycle_time": '原始频率 事件型'
            }})
            logger.debug(f"2-4：message_tj32960Resent 的event结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取企标实发的结果
        if "message_MSLive" in overallList:
            logger.debug(f"2-5-0：get message_MSLive 开始。。{time.time()*1000-time0}")
            # 1. 得到oriMessageLiveCropedDict，后面signal继续用这个数据
            dataSourcesLive = 'message_enterprise_live.txt'
            fullPathList1 = service.public.getFullPathList(vin, dateList, dataSourcesLive, env=env)
            oriMessageList = service.public.getPureContents(fullPathList1)

            logger.debug(f"2-5-1：oriMessageList完成。。{time.time()*1000-time0}")

            oriMessageLiveCropedDict = service.msService.cropAndTransformer2dict(oriMessageList, startTime, endTime, needSort=True) \
                if oriMessageList else {}

            logger.debug(f"2-5-2：oriMessageLiveCropedDict 完成。。{time.time()*1000-time0}")

            # MCUTimeList = [x.split('"MCUTime": "')[1].split('", "TYPE_CMD')[0] for x in oriMessageList]
            MPUTimeList = [x.split('"MPUTime": "')[1].split('", "MCUTime')[0] for x in oriMessageList]

            logger.debug(f"2-5-3：MPUTimeList 完成。。{time.time()*1000-time0}")

            del(oriMessageList)

            # 2. 分发到Y轴
            YMSdict = assignAmount2TimeSlot(Xaxis, MPUTimeList, needSort=False)

            logger.debug(f"2-5-3：YMSdict 完成。。{time.time()*1000-time0}")

            # 3. 上装配到resp输出
            # message_MSLive = getMS(vin, Xaxis, 'message_enterprise_live.txt', dateList, env=env)
            resp['YaxisOverall']['message_MSLive'] = YMSdict
            resp['YaxisList'].append({"message_MSLive": {
                "type": "message",
                "choices": {},
                "maximum": 100,
                "minimum": 0,
                "graphType": 'smooth',
                "comment": '企标实发报文条数',
                "cycle_time": '原始频率 10秒'
            }})
            logger.debug(f"2-5：get message_MSLive 的结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取企标补发的结果
        if "message_MSResent" in overallList:

            # 天际企标补发报文文件名
            dataSourcesResent = 'message_enterprise_resent.txt'
            fullPathList2 = service.public.getFullPathList(vin, dateList, dataSourcesResent, env=env)
            oriMessageListResent = service.public.getPureContents(fullPathList2)
            oriMessageResentCropedDict = service.msService.cropAndTransformer2dict(oriMessageListResent, startTime, endTime, needSort=True) \
                if oriMessageListResent else {}
            MPUTimeList = [x.split('"MPUTime": "')[1].split('", "MCUTime')[0] for x in oriMessageListResent]
            MPUTimeList.sort()
            YMSdict = assignAmount2TimeSlot(Xaxis, MPUTimeList, needSort=False)

            resp['YaxisOverall']['message_MSResent'] = YMSdict
            resp['YaxisList'].append({"message_MSResent": {
                "type": "message",
                "choices": {},
                "maximum": 100,
                "minimum": 0,
                "graphType": 'smooth',
                "comment": '企标补发报文条数',
                "cycle_time": '原始频率 事件型'
            }})
            logger.debug(f"2-6：get message_MSResent 的结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取企标告警的结果
        if "message_MSWarning" in overallList:

            # 天际企标报警报文文件名
            dataSourcesWaining = 'message_enterprise_warning.txt'
            fullPathList3 = service.public.getFullPathList(vin, dateList, dataSourcesWaining, env=env)
            oriMessageListWarning = service.public.getPureContents(fullPathList3)
            oriMessageWarningCropedDict = service.msService.cropAndTransformer2dict(oriMessageListWarning, startTime, endTime, needSort=True) \
                if oriMessageListWarning else {}

            MPUTimeList = [x.split('"MPUTime": "')[1].split('", "MCUTime')[0] for x in oriMessageListWarning]
            MPUTimeList.sort()
            YMSdict = assignAmount2TimeSlot(Xaxis, MPUTimeList, needSort=False)

            resp['YaxisOverall']['message_MSWarning'] = YMSdict
            resp['YaxisList'].append({"message_MSWarning": {
                "type": "message",
                "choices": {},
                "maximum": 100,
                "minimum": 0,
                "graphType": 'smooth',
                "comment": '企标告警报文条数',
                "cycle_time": '原始频率 事件型'
            }})
            logger.debug(f"2-7：get message_MSWarning 的结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取MISC的聚合结果
        if "message_MiscList" in overallList:
            time1 = time.time()*1000
            message_MiscList = getMisc(vin, Xaxis, dateList, env=env)
            logger.debug(f"hhhh获取MISC的聚合结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_MiscList'] = message_MiscList
            resp['YaxisList'].append({"message_MiscList": {
                "type": "message",
                "choices": {},
                "maximum": 10,
                "minimum": 0,
                "graphType": 'smooth',
                "comment": 'MISC报文条数',
                "cycle_time": '原始频率 10秒'
            }})
            logger.debug(f"2-8：message_MiscList 的结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取登入登出心跳的聚合结果
        if "message_HeartbeatList" in overallList:
            time1 = time.time()*1000
            message_HeartbeatList = getHeartbeat(vin, Xaxis, dateList, env=env)
            logger.debug(f"hhhh获取登入登出心跳的聚合结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_HeartbeatList'] = message_HeartbeatList
            resp['YaxisList'].append({"message_HeartbeatList": {
                "type": "message",
                "choices": {},
                "maximum": 10,
                "minimum": 0,
                "graphType": 'smooth',
                "comment": 'Tbox心跳报文',
                "cycle_time": "原始频率 180秒"
            }})
            logger.debug(f"2-9：message_HeartbeatList 的结果完毕。。。{time.time()*1000-time0}")


        # 传进来的signalList必须包含ME7_ 或者 ME5_这样的前缀 废弃这个要求
        # 传进来的signalList是干净的信号全称    2021/10/24
        if signalList:

            # signalList有值，必须vehicleModel也要有有效值
            if vehicleModel not in ['ME7', 'ME5']:
                raise Exception("110904")


            # 如果前面overall的时候没有处理企标实发报文，在这里补上
            if 'oriMessageLiveCropedDict' not in locals().keys():
                # 天际企标的报文文件名
                dataSourcesLive = 'message_enterprise_live.txt'
                fullPathList1 = service.public.getFullPathList(vin, dateList, dataSourcesLive, env=env)
                oriMessageList = service.public.getPureContents(fullPathList1)
                oriMessageLiveCropedDict = service.msService.cropAndTransformer2dict(oriMessageList, startTime, endTime, needSort=True) \
                    if oriMessageList else {}
                del(oriMessageList)
                logger.debug(f"5-1：获取企标实发报文的string结果，裁剪后转为dict结果完毕。。。{time.time()*1000-time0}")


            if 'oriMessageResentCropedDict' not in locals().keys():
                # 天际企标补发报文文件名
                dataSourcesResent = 'message_enterprise_resent.txt'
                fullPathList2 = service.public.getFullPathList(vin, dateList, dataSourcesResent, env=env)
                oriMessageListResent = service.public.getPureContents(fullPathList2)
                oriMessageResentCropedDict = service.msService.cropAndTransformer2dict(oriMessageListResent, startTime, endTime, needSort=True) \
                    if oriMessageListResent else {}
                del(oriMessageListResent)
                logger.debug(f"5-2：获取企标补发报文的string结果，裁剪后转为dict结果完毕。。。{time.time()*1000-time0}")


            if 'oriMessageWarningCropedDict' not in locals().keys():
                # 天际企标报警报文文件名
                dataSourcesWaining = 'message_enterprise_warning.txt'
                fullPathList3 = service.public.getFullPathList(vin, dateList, dataSourcesWaining, env=env)
                oriMessageListWarning = service.public.getPureContents(fullPathList3)
                oriMessageWarningCropedDict = service.msService.cropAndTransformer2dict(oriMessageListWarning, startTime, endTime, needSort=True) \
                    if oriMessageListWarning else {}
                del(oriMessageListWarning)
                logger.debug(f"5-3：获取企标告警报文的string结果，裁剪后转为dict结果完毕。。。{time.time()*1000-time0}")


            # 组合实发,补发,告警报文， 组合后是乱序的
            combinedDict = dict(oriMessageLiveCropedDict, **oriMessageResentCropedDict)
            combinedDict = dict(combinedDict, **oriMessageWarningCropedDict)

            # 对字典进行排序后转成list
            sortedMessages = sorted(combinedDict.items(), key=lambda x:x[0])
            del(combinedDict)
            # gc.collect()
            logger.debug(f"6: 实发补发告警组合完毕，开始要解析到信号了，到目前为止耗时: {time.time()*1000 - time0} ms")


            # 判断下传入的车型，和报文里读到的车型是否匹配，如不匹配，就别做下去了
            msUploadProtol = None
            if sortedMessages:
                # print(sortedMessages[0][1])
                msUploadProtol = sortedMessages[0][1][1]
                if vehicleModel != sortedMessages[0][1][0]:
                    raise Exception("110905", f'小天说了，你告诉我车型是{vehicleModel},可实际上报文是{sortedMessages[0][1][0]},你自己想想清楚先')

            # 输入一段连续的报文list，根据X轴的实际情况，进行抽稀。
            abstractionMessages = abstract(sortedMessages, Xaxis)
            del(sortedMessages)
            gc.collect()
            logger.debug(f"6+1：根据X轴的实际情况，进行抽稀完成。。。{time.time()*1000-time0}")

            # 把canIDDict的生成放在这里，因为确认了用什么样的车型和组包协议
            # 根据singal判断需要解析哪些canid，假设查询时，signal前缀截取的vehicleModel是正确的，后面要跟报文中的实际车型对比
            canIDDict, signalInfoDict = service.msService.getCanIDListBySignalList(signalList=signalList,
                                                                                   vehicleMode=vehicleModel,
                                                                                   msUploadProtol=msUploadProtol)

            # 获取到每个信号的invalid值
            signalsInvalidValueDict = {}
            if skipInvalidValue:
                for k, v in signalInfoDict.items():
                    if v.get('invalid'):
                        signalsInvalidValueDict[k] = v.get('invalid')


            logger.debug(f"6+2：根据singal判断需要解析哪些canid 的结果完毕。。。{time.time()*1000-time0}")

            if not canIDDict:
                raise Exception("110900", '传入的signal,又一个或多个找不到对应的canid')

            # 判断这些can协议中支持的signal以及cannID，是否包含在tbox上报协议中。如果有不上传的信号，放在missedSignalList中
            missedSignalList = []
            missedCanIDList = []
            additionalCanIDDict = {}
            # print(f"检查canIDDict.keys()里要有misc这个补充canID: {canIDDict.keys()}")

            for _canID in canIDDict.keys():
                # 2021/11/30 补充了额外的canDB，这里筛选一下信号，是否可以在补充的canDB中找到
                if _canID in additionalCanDB.get_message_signals():
                    additionalCanIDDict[_canID] = canIDDict[_canID]

            if msUploadProtol:
                for k in canIDDict.keys():
                    if k in EnterpriseTransportProtolVer[vehicleModel][msUploadProtol]:
                        pass
                    else:
                        missedCanIDList.append(k)
                        for _s in canIDDict[k]:
                            missedSignalList.append(_s)


            if missedSignalList:
                for _canID in missedCanIDList:
                    del(canIDDict[_canID])

            # print(abstractionMessages)
            # Like this:
            # {'2021-07-02_03:47:10': (('ME7', '0a', '328e8026da334...000'),)}
            # or 当X刻度小于秒时
            # {'2021-07-02_04:46:16.000': (('ME7', '0a', '328f482...00'),)}

            # 开进程池并行处理
            Pools = Pool(8)
            asyncResult = []
            respContents = []

            if abstractionMessages:
                # print("canIDDict:", canIDDict)
                pass

            for k,v in abstractionMessages.items():
                asyncResult.append(Pools.apply_async(tjmsParseSignals2List,
                                                     (k,
                                                      v[0][2],
                                                      v[0][1],
                                                      v[0][0],
                                                      canIDDict,
                                                      firstOnly,
                                                      signalsInvalidValueDict
                                                      )))
            Pools.close()
            Pools.join()

            for res in asyncResult:
                _res = res.get()
                if _res:
                    respContents += _res

            logger.debug(f"7: 多进程异步解析到信号完成，到目前为止耗时: {time.time()*1000 - time0} ms")

            # 高于1Hz的报文，从前一秒倒推，修改对应的报文时间
            if not firstOnly:
                logger.debug(f"8.0, 需要拆解秒以下的信号")
                respContents = modifyMicroSecTime(canIDDict, respContents, Xinterval, signalInfoDict)
                logger.debug(f"8.1, 秒以下的信号拆解完成")

            # 每个信号占1行，每行是所有的秒信号
            signalListFor1Line = transformer2Yaxis(canIDDict, respContents, Xinterval=Xinterval, signalInfos=signalInfoDict, firstOnly=firstOnly)
            logger.debug(f"8: 把解析信号分组完成，到目前为止耗时: {time.time()*1000 - time0} ms")

            print(signalListFor1Line)
            # 2021/11/30在这里补充上additional的报文解析
            # print(f"为什么没有进入additionalCanIDDict，检查一下: {additionalCanIDDict}")
            if additionalCanIDDict:
                for key in additionalCanIDDict.keys():
                    if key == 'misc':
                        # 补充misc的解析结果
                        additionalMisc_parse(vin, env, Xaxis, dateList, additionalCanIDDict['misc'], signalListFor1Line)
                    else:
                        logger.error(f"这是要我干啥啊，不懂了。additionalCanIDDict: {additionalCanIDDict}")


            # 达到一定scale的，需要标记实值，给echars放大后，标记一个代表实值的原点
            if Xscale >= '16':
                for oneSignalRealValue in signalListFor1Line:
                    _signalName = oneSignalRealValue[0]
                    _signalAllValues = oneSignalRealValue[1]
                    _signal_RealValue = []
                    for k, v in _signalAllValues.items():
                        _plot = {}
                        _plot['coord'] = [k, v]
                        _signal_RealValue.append(_plot)

                    resp['YaxisSignal_RealValue'][_signalName] = _signal_RealValue


            # signalListFor1Line_ver2 = signalListFor1Line[:]
            if not firstOnly:
                smartFillUp(signalListFor1Line, resp['Xaxis'])



            # 极限填充，把上一步的signalListFor1Line。
            # print(f"极限填充前的signalListFor1Line: {signalListFor1Line}")
            # 1、判断距离，距离小于2秒要填充。 暂定义2秒，以后可以放到config中
            # 2、填充到最小刻度--10ms。
            # 3、取前一个值填充，是否改成线性差值，TBD
            # 4、由于signalListFor1Line的值的部分是字典，可以直接把填充的值set进去，填充后会变成无序的字典
            # if not firstOnly:
            #     extremelyFillUp(signalListFor1Line)
            # print(f"极限填充后的signalListFor1Line: {signalListFor1Line}")

            for oneSignalAllSec in signalListFor1Line:
                _signalName = oneSignalAllSec[0]
                _signalAllValues = oneSignalAllSec[1]

                resp['YaxisSignal'][_signalName] = _signalAllValues
                # _signalInfo = service.msService.getSignalInfo(signalName=_signalName,vehicleModel=vehicleModel)
                _signalInfo = signalInfoDict[_signalName]

                resp['YaxisList'].append({_signalName: {
                    "type": "signal",
                    "choices": _signalInfo['choices'],
                    "maximum": _signalInfo['maximum'],
                    "minimum": _signalInfo['minimum'],
                    "graphType": _signalInfo['graphType'],
                    "comment": _signalInfo['comment'],
                    "cycle_time": '信号原始频率 ' + str(_signalInfo['cycle_time'])+' ms',
                }})

            # 补充上tbox没有传到平台的那些canid信息
            for oneSignalMissed in missedSignalList:
                if resp['YaxisSignal'].get(oneSignalMissed):    # 有的missedSignal，会在additionalCanDB中得到处理
                    pass
                else:
                    resp['YaxisSignal'][oneSignalMissed] = {}

                _signalInfo = signalInfoDict[oneSignalMissed]
                resp['YaxisList'].append({oneSignalMissed: {
                    "type": "signal",
                    "choices": _signalInfo['choices'],
                    "maximum": _signalInfo['maximum'],
                    "minimum": _signalInfo['minimum'],
                    "graphType": _signalInfo['graphType'],
                    "comment": _signalInfo['comment'],
                    "cycle_time": '信号原始频率 ' + str(_signalInfo['cycle_time'])+' ms',
                    "warning": '这个信号Tbox没有传给平台！'
                }})


            logger.debug(f"9: 把每个信号的全部value，对应到统一的X轴上完成，到目前为止耗时: {time.time()*1000 - time0} ms")

        # 按照Xaxis的刻度，把没有值的刻度填充无效值
        resp = makeResponse(resp)
        logger.debug(f"10: 按照Xaxis的刻度，把没有值的刻度填充无效值完成，到目前为止耗时: {time.time()*1000 - time0} ms")

        return {
                   "code": 200,
                   "message": None,
                   "businessObj": resp
               }, 200
    except Exception as ex:
        return {
                   "code": ex.args[0],
                   # "message": ex.args[1] if len(ex.args) > 1 else ReturnCode[ex.args[0]],
                   "message": str(ex),
                   "businessObj": None
               }, 200

def additionalMisc_parse(vin, env, Xaxis, dateList, additionalSignalList, signalListFor1Line):
    # 读时间段的报文到usefulMessage
    dataSources = 'message_misc.txt'
    # 获取需要读取的文件列表
    fullPathList = service.public.getFullPathList(vin, dateList, dataSources, env=env)
    _pureContents = service.public.getPureContents(fullPathList)

    startTimeStr = Xaxis[0]
    endTimeStr = Xaxis[-1]

    # 找到需要处理的第一条
    bufferCursor = 0
    find = False
    while bufferCursor < len(_pureContents):
        if _pureContents[bufferCursor].split('"MCUTime": "')[1][:19] < startTimeStr[0:19]:
            # 跳过这条废数据
            bufferCursor += 1
        else:
            find = True
            break

    if not find:
        return {}

    usefulMessage = collections.OrderedDict()

    while _pureContents[bufferCursor].split('"MCUTime": "')[1][:19] <= endTimeStr:

        _jsondata = orjson.loads(_pureContents[bufferCursor])

        _flatJson = {}

        _modemMode = _jsondata['contents']['modemMode']
        if _modemMode == '4G':
            _modemMode = '01'

        _flatJson['modemMode'] = int(_modemMode, 16)
        _flatJson['modemSignal'] = int(_jsondata['contents']['modemSignal'], 16)
        _flatJson['locationStatus'] = int(_jsondata['contents']['miscData']['locationStatus'], 16)
        _flatJson['longitude'] = int(_jsondata['contents']['miscData']['longitude'], 16) / 1000000
        _flatJson['latitude'] = int(_jsondata['contents']['miscData']['latitude'], 16) / 1000000
        _flatJson['UTC'] = _jsondata['contents']['miscData']['UTC']
        _flatJson['UTCms'] = int(_jsondata['contents']['miscData']['UTCms'], 16)
        _flatJson['groundSpeed'] = int(_jsondata['contents']['miscData']['groundSpeed'], 16) / 10
        _flatJson['speedDirection'] = int(_jsondata['contents']['miscData']['speedDirection'], 16)

        seq = [_flatJson.get(x) for x in additionalSignalList]

        usefulMessage[_jsondata['MCUTime']] = seq

        # 如果buffer还没到底，就cursor+1
        if bufferCursor < (len(_pureContents) - 1):
            bufferCursor += 1
        else:
            break

    # logger.debug(f"additionalMisc_parse: 读时间段的报文到usefulMessage: {usefulMessage}")
    # 选出X轴上要显示的content
    if len(Xaxis) >= 2:
        _interval = Timeutils.timeString2timeStamp(Xaxis[1], ms=True) - Timeutils.timeString2timeStamp(Xaxis[0], ms=True)
    else:
        _interval = 0
    ordered_origin_key = list(usefulMessage.keys())
    ordered_origin_key_MAX = len(ordered_origin_key)
    outputContent = {}

    # 秒及以上的，这个算法非常好，但毫秒Xaxis，就会出现过多的实值
    if _interval >= 1000:   # 秒及以上
        for _x in Xaxis:
            if usefulMessage.get(_x):
                outputContent[_x] = usefulMessage.get(_x)
            else:
                _guess = bisect.bisect_left(ordered_origin_key, _x)

                if _guess != 0 and ordered_origin_key[_guess - 1] == _x[:19]:        # 如果_x带ms，则可能和-1位置的是同一时刻
                    outputContent[_x] = usefulMessage.get(ordered_origin_key[_guess - 1])
                elif _guess < ordered_origin_key_MAX and Timeutils.timeString2timeStamp(ordered_origin_key[_guess], ms=True) - Timeutils.timeString2timeStamp(_x, ms=True) < _interval:
                    outputContent[_x] = usefulMessage.get(ordered_origin_key[_guess])
                else:
                    pass
    else:       # 毫秒时
        for _x in Xaxis:
            if _x[-3:] == '000':    # 代表这是个整秒的刻度
                if usefulMessage.get(_x[0:19]):
                    outputContent[_x] = usefulMessage.get(_x[0:19])

    # logger.debug(f"additionalMisc_parse: 选出X轴上要显示的content:{outputContent}")
    # 根据additionalSignalList，添加到最后的输出结果signalListFor1Line
    for seq, signalName in enumerate(additionalSignalList):
        _signalDict = {}
        for k,v in outputContent.items():
            _signalDict[k] = v[seq]

        # 这个是总输出
        signalListFor1Line.append([signalName, _signalDict])

    logger.debug(f"additionalMisc_parse: 添加到最后的输出结果signalListFor1Line完成")
    return True


def smartFillUp(signalListFor1Line, Xaxis):

    TIMEGAP = 2 # 两个信号之间的时间距离，超过这个值，判断是发生了中断。单位：秒
    time00 = time.time() * 1000

    for siganlRealValue in signalListFor1Line:
        # 以下是对一个信号的全部有效报文做处理
        if siganlRealValue[1]:           # 不是一个没有值的信号，开始搞
            ordered_origin_key = list(siganlRealValue[1].keys())
            ordered_origin_key_total = len(ordered_origin_key)
            _virtualValueDict = {}
            for _x in Xaxis:
                # 没有从实值里get到，需要填充
                if siganlRealValue[1].get(_x) is None:
                    _guessLeft = bisect.bisect_left(ordered_origin_key, _x)

                    # 送进去的_x在队尾，说明不需要填充了，把right置为None
                    if _guessLeft == ordered_origin_key_total:
                        _guessRight = None
                        _guessLeft -= 1

                    else:
                        # 送进去的_x不在队尾，也不在排头，则把right赋值
                        if _guessLeft > 0:
                            _guessRight = _guessLeft
                            _guessLeft -= 1
                        else:
                            # 送进去的_x排最前面，把right赋值为0。这里这个right不会用到。
                            _guessRight = _guessLeft
                        # print(_x, ordered_origin_key[_guess])

                    if _x < ordered_origin_key[_guessLeft]:
                        # 实际填充时，小于队列第一个，不需要对left和right比对，直接跳过。
                        pass
                    else:
                        # 如果right没有值，说明已经在实值队列的后面，不填充。
                        if not _guessRight:
                            pass

                        else:
                            # _x在实值队列的中部，需要判断到left的距离，和right距离left的距离，决定是否填充

                            # 判断最近的_guess是否大于阈值，大于就不填充，小于则填充
                            _distanceLeft = float(_x[17:]) - float(ordered_origin_key[_guessLeft][17:])

                            _distanceRightLeft = float(ordered_origin_key[_guessRight][17:]) - float(ordered_origin_key[_guessLeft][17:])
                            # print(_x, ordered_origin_key[_guess], _distance)
                            if _distanceLeft < 0:
                                _distanceLeft += 60

                            if _distanceRightLeft < 0:
                                _distanceRightLeft += 60

                            # 如果_x距离left不到TIMEGAP，且right距离left也不到TIMEGAP时，进行填充。否则就判定是个间断，不填充了。
                            if _distanceLeft < TIMEGAP and _distanceRightLeft < TIMEGAP:
                                _virtualValueDict[_x] = siganlRealValue[1].get(ordered_origin_key[_guessLeft])

            # 合并这两个字典
            siganlRealValue[1] = dict(siganlRealValue[1], **_virtualValueDict)

    logger.debug(f"smartFillUp done, spent {time.time()*1000-time00} ms")

# 已经被smartFillUp代替
def extremelyFillUp(signalListFor1Line):
    TIMEGAP = 2000 # 两个信号之间的时间距离，超过这个值，判断是发生了中断。

    time00 = time.time() * 1000
    for siganlRealValue in signalListFor1Line:

        # 以下是对一个信号的全部有效报文做处理
        if siganlRealValue[1]:           # 不是一个没有值的信号
            _tempKeyList = list(siganlRealValue[1].keys())
            _tempValueList = list(siganlRealValue[1].values())
            _fillStartTS = Timeutils.timeString2timeStamp(_tempKeyList[0], ms=True)     # 令第一条记录为填充的起点
            _filledDict = {}                                        # 定义填充的结果
            for _i in range(len(_tempKeyList)):
                # print(_tempKeyList[_i])
                # print(_tempValueList[_i])
                _fillEndTS = Timeutils.timeString2timeStamp(_tempKeyList[_i], ms=True)
                if _fillEndTS - _fillStartTS > TIMEGAP:  # 判断是个断点
                    _fillStartTS = _fillEndTS     # 这是一个新的填充的起始点
                else:
                    # 需要按照极限进行填充 --- 间隔 10ms
                    # print(f"需要按照极限进行填充: from {Timeutils.timeStamp2timeStringMS(_fillStartTS)}, to {Timeutils.timeStamp2timeStringMS(_fillEndTS)}, value: {_tempValueList[_i]}. "
                    #       f"OR {_fillStartTS} to {_fillEndTS}")

                    for _x in range(_fillStartTS + 10 , _fillEndTS + 10, 10):
                        _filledDict[Timeutils.timeStamp2timeStringMS(_x)] = _tempValueList[_i]

                    _fillStartTS = _fillEndTS

            # print("原始的dict:", siganlRealValue[1])
            # print((siganlRealValue[0], _filledDict))

            # 把filledDict 范围上已经覆盖原始Dict，可以直接使用
            # siganlRealValue[1] = _filledDict
            # siganlRealValue[1]包含起点的kv对，但filledDict不含，所以还是要做字典合并 2021/10/24
            # 好处是这时候用字典，已经不需要再排序了，从x的任意刻度，直接在这个字典中取输出值即可
            siganlRealValue[1] = dict(siganlRealValue[1], **_filledDict)

    logger.debug(f"extremelyFillUp done, spent {time.time()*1000-time00} ms")


def abstract(sortedMessages, Xaxis):
    '''
    把传入的有序的dict，按照x轴的秒，进行抽稀
    :param sortedMessages:
    :param Xaxis:
    :return:
    '''
    # print(sortedMessages[0])

    # 设2个指针. XaxisCursor:X时间轴的指针， bufferCursor:要处理的sortedMessages的指针。
    XaxisCursor = 0
    bufferCursor = 0

    abstractionMessages = {}

    # 下面的算法，会导致Xaxis的最后一个坐标，即使有对应时间点的sortedMessages，也不能被选中。
    # 增加一个dummy的Xaxis，由于是浅复制，结束前要删除这个dummy
    Xaxis.append('dummy')

    # 传进来的参数有值再说，否则直接返回空dict
    if sortedMessages:
        #
        while XaxisCursor < len(Xaxis):
            # 寻找第一个有值的刻度
            # sortedMessages[bufferCursor] like this：
            # ('2021-07-02_04:46:15', ('ME7', '0a', '1a8f482710010000028f502710010000038f502710010000048f482710010000058f502710010000068f482710010000078f482710010000088f482710010000098f4827100100000a8f4827100100000b8f5027100100000c8f4827100100000d8f5027100100000e8f4827100100000f8f482710010000008f482710010000018f482710010000028f482710010000038f502710010000048f482710010000058f482710010000068f502710010000078f482710010000088f482710010000098f4827100100000a8f5027100100000b070000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100ff00ffffffff01010000ff00ff0000010000019b000000020000020100000400ff80000b019727107fff0000010001e827101964e0000201d7100010ffc8b3020178100000320008020000000001240202aa0200008101eb0027103fff800601f0001e00780c0006010000000b0000000001640000000000f565013a84f02082b0048401000000000000000401000000000000000601640064919000006401e27fc3fe800000030100200fff2851280501525253694803460001890800000008101001000000000000000001000000000000000001a20000058e50000301000054000000000001008000000000aa000100010018948e78ac0100f0fffcfc00000001000000000000000000011080009802001f00019eb57dae4c08000001ffff7781ff9e5f0b018200005c4000000001080660af0103060001fff0001f2000000001590006ec06700000010001ff0000000006010001008000004e3401dc00000000530005000100255680006403000100400010fd3f606b01acb0b9b25a5a5a67010000000000aa000001000000000000000001000000000000000001000000000000000001000000000000000001030000000000000001000080093212000000000001000000000000000001000000000000000000000113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e701ffffffffffffffff01ffffffffffffffff01ffffffffffffffff01ffffffffffffffff00010000000000000000010000000000000000010000000000000000010000000000000000010000000000646c0000010202000000000000'))
            if Xaxis[XaxisCursor] <= sortedMessages[bufferCursor][0]+'.000000':
                XaxisCursor += 1
            else:
                # 如果这个刻度有值了，就忽略
                if abstractionMessages.get(str(Xaxis[XaxisCursor-1])):
                    pass
                # 如果没有值，就写入
                else:
                    abstractionMessages[str(Xaxis[XaxisCursor-1])] = sortedMessages[bufferCursor][1:]

                # 如果buffer还没到底，就cursor+1
                if bufferCursor < (len(sortedMessages) - 1):
                    bufferCursor += 1
                else:
                    break

    else:
        pass

    # 删除前面的dummy的Xaxis，由于是浅复制，结束前要删除这个dummy
    if Xaxis[-1] == 'dummy':
        Xaxis.pop(-1)

    return abstractionMessages

def modifyMicroSecTime(canIDDict, contents, Xinterval, signalInfos={}):
    # 给这个杀千刀的ESP_VehicleSpeed打补丁，非要在tbox降低采样频率，干！
    if canIDDict.get('ESP_0x121'):
        for _item in canIDDict.get('ESP_0x121'):
            signalInfos[_item]['tbox_cycle_time'] = 100

    # 前面步骤完成了对同一秒下面多个信号的排序，并且不破坏秒内信号的值的顺序，赞 2021/10/24
    from itertools import groupby
    tm_group = groupby(contents,key=lambda x : (x[0], x[1]))        # 按信号分组

    resp_seprateMicroSecPack = []
    for key,group in tm_group:
        oldTimeMiscoSec = key[1]
        newTimeStamp = Timeutils.timeString2timeStamp(oldTimeMiscoSec, ms=True) - 1000
        # print(f"把{key[0]} 这个信号的在{key[1]} 秒收到的内容处理一下.新的时间是：{Timeutils.timeStamp2timeString(newTimeStamp)}")
        doingList = list(group)
        _intervalMS = signalInfos.get(key[0]).get('cycle_time')

        if canIDDict.get('ESP_0x121') and key[0] in canIDDict.get('ESP_0x121'):
            # print("len(doingList)")
            if (len(doingList)) <= 11:
                _intervalMS = signalInfos.get(key[0]).get('tbox_cycle_time')

        if len(doingList) == 1:
            startOffset = 1000
        else:
            # 后对齐，如:32秒应该发10帧，结果发了9帧，则从:31.200--:32.000会有值
            startOffset = _intervalMS + (1000 - len(doingList) * _intervalMS)

        # print(f"doingList={doingList}")
        for _item in doingList:
            # print(f"新的时间是：{Timeutils.timeStamp2timeStringMS(newTimeStamp+startOffset)}, 间隔{_intervalMS} 说你呢: {_item}")
            # print(_item[0], Timeutils.timeStamp2timeStringMS(newTimeStamp+startOffset), _item[2])
            resp_seprateMicroSecPack.append((_item[0], Timeutils.timeStamp2timeStringMS(newTimeStamp+startOffset), _item[2]))
            startOffset += _intervalMS

    # print(f"resp_seprateMicroSecPack={resp_seprateMicroSecPack}")
    return resp_seprateMicroSecPack




def transformer2Yaxis(canIDDict, contents, Xinterval, signalInfos={}, firstOnly=True):
    # canIDDict like this:
    # {'BMS_0x100': ['BMS_PackI']}
    # contents like this:
    # [('BMS_PackI', '2021-10-17_07:51:28.000', [0.0]),('BMS_PackI', '2021-10-17_07:51:28.000', [0.0])]
    # 得到要输出signal的list
    # 处理方法要根据信号的cycle_time，把contents里的内容，不需要考虑Xscale，按照cycle_time还原。


    signalList = []
    for k,v in canIDDict.items():
        for signal in v:
            signalList.append(signal)

    # 得到要输出的signal的List的Index，用于后面的contens到输出结果的映射
    _signalIndexTempList = [x for x in range(0,len(signalList))]
    signalIndex = dict(zip(signalList, _signalIndexTempList))

    # 初始化最终要返回的list
    signalYaxisList = []
    for _Y in signalList:
        signalYaxisList.append([_Y, {}])

    for _line in contents:
        _k = _line[1]
        _v = _line[2]
        # TODO 验证这样的做法是否合适？从 signalYaxisList[signalIndex[_line[0]]][1][_k] = _v 到现在这样
        if _v:
            _value = _v[0]
            # 如果时浮点的值，保留两位小数
            if isinstance(_value, float):
                _value = round(_value, 2)
            signalYaxisList[signalIndex[_line[0]]][1][_k] = _value
        else:
            signalYaxisList[signalIndex[_line[0]]][1][_k] = _v

    # print(f"signalYaxisList={signalYaxisList}")
    return signalYaxisList



def createXaxis(startTime, endTime, interval=10):

    workingStartTimeStamp = startTime
    workingEndTimeStamp = endTime + interval

    # xAxisTotal: X轴的点位数量
    xAxisTotal = math.ceil((workingEndTimeStamp - workingStartTimeStamp) / (interval*1000))

    # respXaxis: 用于绘图的X轴的坐标list
    respXaxis = []
    timeCursor = workingStartTimeStamp
    while xAxisTotal:
        if interval >= 1:
            respXaxis.append(Timeutils.timeStamp2timeString(timeCursor))
        else:
            respXaxis.append(Timeutils.timeStamp2timeStringMS(timeCursor))
        timeCursor += interval * 1000
        xAxisTotal -= 1

    return respXaxis


def getTJ32960(vin, Xaxis, messagetype, dateList, env):
    # 天际国标的报文文件名
    dataSources = messagetype

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources, env=env)

    # readKeys = [['MPUTime'], ['TYPE_CMD']]
    readKeys = [['MPUTime']]
    # 获取内容
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    # 假设实发和补发没有重复的部分。不需要进行去重的动作

    Y32960 = assignAmount2TimeSlot(Xaxis, oriMessageList, needSort=False)
    return Y32960


def getMS(vin, Xaxis, messagetype, dateList, env):
    time0 = time.time()*1000

    # 天际企标的报文文件名
    dataSourcesLive = messagetype


    # 获取需要读取的文件列表
    time1 = time.time()*1000
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSourcesLive, env=env)


    # 读取必要的message
    time1 = time.time()*1000
    readKeys = [['MPUTime']]
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)


    # 把企标分发到Y轴
    time1 = time.time()*1000
    YMSdict = assignAmount2TimeSlot(Xaxis, oriMessageList, needSort=False)

    return YMSdict


def getMisc(vin, Xaxis, dateList, env):
    # 天际的报文文件名
    dataSources = 'message_misc.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources, env=env)

    readKeys = [['MPUTime']]
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    YMisc = assignAmount2TimeSlot(Xaxis, oriMessageList)

    return YMisc


def getHeartbeat(vin, Xaxis, dateList, env):
    # 天际的报文文件名
    dataSources = 'message_hearbeat.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources, env=env)

    readKeys = [['MPUTime']]
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    YHeartbeat = assignAmount2TimeSlot(Xaxis, oriMessageList)

    return YHeartbeat


def getVehicleLoginEvents(vin, Xaxis, dateList, env):
    # 天际的报文文件名
    dataSources = 'event_vehicle.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources, env=env)

    # readKeys = [['timestamp'], ['event']]
    readKeys = ['.']
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    # readKeys用['.']的时候，通畅需要对返回的List做二次加工
    respMessageList = []
    for _item in oriMessageList:
        _temp = orjson.loads(_item)

        if _temp['timestamp'] < 9999999999:
            _temp['timestamp'] = _temp['timestamp'] * 1000

        respMessageList.append({'timestamp': _temp['timestamp'],
                                'timestr': Timeutils.timeStamp2timeString(_temp['timestamp']),
                                'event': _temp['event']})

    YHeartbeat = assignArray2TimeSlot(Xaxis, respMessageList)

    return YHeartbeat


def getRemoteCmdEvents(vin, Xaxis, dateList, env):
    # 天际的报文文件名
    dataSources = 'event_remote_cmd.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources, env=env)

    # readKeys = [['timestamp'], ['event']]
    readKeys = ['.']
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    # readKeys用['.']的时候，通畅需要对返回的List做二次加工
    respMessageList = []
    for _item in oriMessageList:

        try:

            _temp = orjson.loads(_item)

            # 判断是平台发出，还是tbox回执
            if _temp.get('status') == '258':
                # 这是平台发出的控车指令，新增记录
                if _temp['timestamp'] < 9999999999:
                    _temp['timestamp'] = _temp['timestamp'] * 1000

                respMessageList.append({'timestamp': _temp['timestamp'],    # 1626159853760
                                        'timestr': Timeutils.timeStamp2timeString(_temp['timestamp']),
                                        'cmd': _temp['cmd'],            # "Diagnose_Read_DTC"
                                        'cmdId': _temp['cmdId'],        # "1626159852952hMrO5FawR5v"
                                        'sn': _temp.get('sn'),          # "4988"
                                        'vin': _temp['vin'],            # "LTWA35K14LS000540"
                                        'QA': _temp['status']           # "258"
                                        })
            elif _temp.get('ACK'):
                # 这个tbox返回的回执，修改记录
                _vin = _temp.get('VIN')         # "LTWA35K14LS000540"
                _sn = _temp.get('SN')           # "137c"
                _sn = str(int(_sn, 16))         # 转10进制的str
                _ACK = _temp.get('ACK')         # "01"

                for _update in respMessageList:
                    if _update['vin'] == _vin and _update['sn'] == _sn:
                        _update['QA'] = _ACK

            else:
                logger.warning(f"不予处理的event内容，既不是258已发出，也不是tbox的ACK:{_temp}")
                continue
        except:
            logger.warning(f"dirty message 读不懂:{_temp}")

    # 2021-08-27 发生过remotecmd的文件里，时间乱序，所以在这里排序一下
    b=[]
    for x in respMessageList:
        b.append(json.dumps(x))

    b.sort()

    respMessageList = []
    for x in b:
        respMessageList.append(orjson.loads(x))

    YHeartbeat = assignArray2TimeSlot(Xaxis, respMessageList)

    return YHeartbeat


def getConnStatus(vin, Xaxis, dateList, env):
    previousConnStatus = getPreviousConnStatus(vin, dateList[0], env=env)

    dataSource = 'event_emq_conn.txt'
    fullPathList = service.public.getFullPathList(vin, dateList, dataSource, env=env)

    respMessageList = []
    respMessageList.append({'timestamp': Timeutils.timeString2timeStamp(Xaxis[0], ms=True),
                            'timestr': Xaxis[0],
                            'event': previousConnStatus['event']})


    readKeys = ['.']
    oriMessageList = service.public.getOriMessageList(fullPathList, readKeys)

    # readKeys用['.']的时候，通畅需要对返回的List做二次加工
    for _item in oriMessageList:
        _temp = orjson.loads(_item)

        if _temp['timestamp'] < 9999999999:
            _temp['timestamp'] = _temp['timestamp'] * 1000

        respMessageList.append({'timestamp': _temp['timestamp'],
                                'timestr': Timeutils.timeStamp2timeString(_temp['timestamp']),
                                'event': _temp['event']})

    YConnList = assignArray2TimeSlot(Xaxis, respMessageList)

    return YConnList


def getPreviousConnStatus(vin, date, env):
    dataSource = 'event_emq_conn.txt'
    startDateStr = Timeutils.timeArray2timeString(Timeutils.timeString2timeArray(date, format="%Y-%m-%d")
                                                  + datetime.timedelta(days=-1), format="%Y-%m-%d")
    while True:
        fullPathList = service.public.getFullPathList(vin=vin,
                                                      dateList=[startDateStr],
                                                      dataSources=dataSource,
                                                      env=env)
        if os.path.exists(fullPathList[0]) or startDateStr < '2021-06-01':
            break
        else:
            startDateStr = Timeutils.timeArray2timeString(Timeutils.timeString2timeArray(startDateStr, format="%Y-%m-%d")
                                                          + datetime.timedelta(days=-1), format="%Y-%m-%d")

    _fullPath = fullPathList[0]
    if os.path.split(os.path.split(_fullPath)[0])[1] == '2021-05-31':
        previousConnStatus = {'event': 'unknown'}
    else:
        connStatusList = service.public.getOriMessageList([_fullPath], ['.'])
        connEvent = orjson.loads(connStatusList[-1])
        previousConnStatus = {'event': connEvent.get('event')}

    return previousConnStatus


# 按照Xaxis的刻度，把没有值的刻度填充无效值
def makeResponse(resp):
    makeResp = {}
    makeResp['Xaxis'] = resp['Xaxis']
    makeResp['XaxisTotalPlots'] = resp['XaxisTotalPlots']
    makeResp['dateList'] = resp['dateList']
    makeResp['Xscale'] = resp['Xscale']
    makeResp['YaxisList'] = resp['YaxisList']
    makeResp['YaxisSignal_RealValue'] = resp['YaxisSignal_RealValue']
    makeResp['YaxisSignal'] = {}
    makeResp['YaxisOverall'] = {}
    del(resp['Xaxis'])
    del(resp['dateList'])
    del(resp['YaxisList'])
    del(resp['YaxisSignal_RealValue'])

    # 归类输出YaxisSignal
    for _item in resp['YaxisSignal']:

        _itemResp = []

        try:
            if isinstance(next(iter(resp['YaxisSignal'][_item].values())), int):
                type = 'int'
            elif isinstance(next(iter(resp['YaxisSignal'][_item].values())), list):
                type = 'list'
            elif isinstance(next(iter(resp['YaxisSignal'][_item].values())), dict):
                type = 'dict'
            elif isinstance(next(iter(resp['YaxisSignal'][_item].values())), float):
                type = 'float'
            else:
                type = 'NULL'
        except:
            type = 'NULL'


        # 遍历每个刻度
        for _x in makeResp['Xaxis']:

            # 取出_item这个分类下，每个刻度对应的实际值
            _value = resp['YaxisSignal'][_item].get(_x)

            if _value is not None:
                _itemResp.append(_value)

            else:
                _itemResp.append(None)


        makeResp['YaxisSignal'][_item] = _itemResp


    # 归类输出YaxisOverall
    for _item in resp['YaxisOverall']:

        _itemResp = []

        try:
            if isinstance(next(iter(resp['YaxisOverall'][_item].values())), int):
                type = 'int'
            elif isinstance(next(iter(resp['YaxisSignal'][_item].values())), float):
                type = 'float'
            elif isinstance(next(iter(resp['YaxisOverall'][_item].values())), list):
                type = 'list'
            elif isinstance(next(iter(resp['YaxisOverall'][_item].values())), dict):
                type = 'dict'
            else:
                type = 'NULL'
        except:
            type = 'NULL'


        # 遍历每个刻度
        for _x in makeResp['Xaxis']:

            # 取出_item这个分类下，每个刻度对应的实际值
            _value = resp['YaxisOverall'][_item].get(_x)

            if _value:
                _itemResp.append(_value)
            elif type == 'int':
                _itemResp.append(0)
            elif type == 'float':
                _itemResp.append(0.)
            elif type == 'list':
                _itemResp.append([])
            elif type == 'dict':
                _itemResp.append({})
            else:
                _itemResp.append(None)

        makeResp['YaxisOverall'][_item] = _itemResp

    return makeResp



def assignAmount2TimeSlot(Xaxis, dataList, needSort=False):

    # Xaxis = []
    # for _x in Xaxis1:
    #     Xaxis.append(Timeutils.timeStamp2timeString(_x))


    # 设2个指针：Xaxis指针， buffer指针。 赋值到Yaxis的字典里。
    XaxisCursor = 0
    bufferCursor = 0
    workingStartTimeString = Xaxis[0]
    # step = Xaxis[1] - Xaxis[0]
    # workingEndTimeStamp = Xaxis[-1] + step

    # 看看排序花了多久
    time1 = time.time()*1000

    if needSort:
        dataList.sort()
        logger.debug(f"对{len(dataList)}条数据（例如：{dataList[0]}）的排序，共花了{time.time()*1000 - time1} ms")
    # 找到需要处理的第一条
    find = False
    while bufferCursor < len(dataList):
        # if Timeutils.timeString2timeStamp(dataList[bufferCursor].split(',')[0], ms=True) < workingStartTimeStamp:
        # if int(dataList[bufferCursor].split(',')[0]) < workingStartTimeStamp:
        if dataList[bufferCursor].split(',')[0] < workingStartTimeString:
            # 跳过这条废数据
            bufferCursor += 1
        else:
            # print (f"First content 找到了!")
            find = True
            break

    if find:
        # logger.debug(f"找到了需要处理的第一条：content:{dataList[bufferCursor]}")
        pass
    else:
        # logger.debug(f'最后一个文件读完了，啥也没有')
        return {}

    Yaxis = {}
    # 开始根据respXaxis的刻度，生成对应的respYdict
    while XaxisCursor < len(Xaxis):
        # 这是用转成时间戳方式，太慢，废掉
        # if Xaxis[XaxisCursor] < Timeutils.timeString2timeStamp(dataList[bufferCursor].split(',')[0], ms=True):
        # 改成直接字符串对比的方式，要求来源要有序
        if Xaxis[XaxisCursor] <= dataList[bufferCursor].split(',')[0]:
            XaxisCursor += 1
        else:
            # 为了解决企标实时数量，在缩放后，对应到了应该位置的前一个，把-1去掉，观察下。
            '''
            if Yaxis.get(str(Xaxis[XaxisCursor-1])):
                Yaxis[str(Xaxis[XaxisCursor-1])] += 1
            else:
                Yaxis[str(Xaxis[XaxisCursor-1])] = 1
            '''
            if Yaxis.get(str(Xaxis[XaxisCursor])):
                Yaxis[str(Xaxis[XaxisCursor])] += 1
            else:
                Yaxis[str(Xaxis[XaxisCursor])] = 1

            # 如果buffer还没到底，就cursor+1
            if bufferCursor < (len(dataList) - 1):
                bufferCursor += 1
            else:
                break
    return Yaxis


def assignArray2TimeSlot(Xaxis, dataList, needSort=False):
    # 要求传入的dataList里，每行是一个Dict，并且Dict里要包含一个叫timestamp的key/value
    XaxisCursor = 0
    bufferCursor = 0
    workingStartTimeStamp = Timeutils.timeString2timeStamp(Xaxis[0], ms=True)
    # step = Xaxis[1] - Xaxis[0]
    # workingEndTimeStamp = Xaxis[-1] + step

    if needSort:
        dataList.sort()

    # 找到需要处理的第一条
    find = False
    while bufferCursor < len(dataList):

        if int(dataList[bufferCursor].get('timestamp')) < workingStartTimeStamp:
            # 跳过这条废数据
            bufferCursor += 1
        else:
            # print (f"First content {dataList[bufferCursor]} 找到了!")
            find = True
            break

    if find:
        logger.debug(f"找到了需要处理的第一条：content:{dataList[bufferCursor]}")
    else:
        logger.debug(f'最后一个文件读完了，啥也没有')
        return {}

    Yaxis = {}
    # 开始根据respXaxis的刻度，生成对应的respYdict
    while XaxisCursor < len(Xaxis):

        # if Xaxis[XaxisCursor] < dataList[bufferCursor].split(',')[0]:
        if Timeutils.timeString2timeStamp(Xaxis[XaxisCursor], ms=True) <= dataList[bufferCursor].get('timestamp'):
            XaxisCursor += 1
        else:
            if Yaxis.get(str(Xaxis[XaxisCursor-1])):
                Yaxis[str(Xaxis[XaxisCursor-1])].append(dataList[bufferCursor])
            else:
                Yaxis[str(Xaxis[XaxisCursor-1])] = [dataList[bufferCursor]]

            # 如果buffer还没到底，就cursor+1
            if bufferCursor < (len(dataList) - 1):
                bufferCursor += 1
            else:
                break
    return Yaxis


def tjmsParseSignals2List(MCUTime, data, protocol, vehicleMode, canIDDict, firstOnly=False, signalsInvalidValueDict={}):

    signalsValues = service.msService.parse_tjms_signals_2_list(data,
                                                                vehicleMode=vehicleMode,
                                                                protocol=protocol,
                                                                canIDDict=canIDDict,
                                                                firstOnly=firstOnly,
                                                                signalsInvalidValueDict=signalsInvalidValueDict)

    response = []

    # 补充上MCUTime
    for _signal in signalsValues:
        response.append((_signal[0], MCUTime, _signal[1]))

    # 指定排序的范围，只对信号名称进行排序，第2和3个字段不参与排序
    # ('ESP_VehicleSpeed', '2021-10-21_13:42:41.000', [14.9625])
    #    排序                     不排序                     不排序
    response.sort(key=lambda x:(x[0], 0, 0))
    return response



def assignSignal2TimeSlot(Xaxis, dataList, needSort=False):
    # 要求传入的dataList里，每行是一个Dict，并且Dict里要包含一个叫timestamp的key/value
    # print(dataList)
    # print(type(dataList))

    XaxisCursor = 0
    bufferCursor = 0
    workingStartTimeStamp = Timeutils.timeString2timeStamp(Xaxis[0], ms=True)
    workingStartTimeStr = Xaxis[0]

    # TODO 注意检查这样得到的list，是否从有序变成了无序
    dataList_keys = list(dataList.keys())
    dataList_values = list(dataList.values())

    _verifyDict = dict(zip(dataList_keys, dataList_values))
    if _verifyDict != dataList:
        logger.error("ERROR: 坏了，从dict转list后，变成了无序的list")
        print("ERROR: 坏了，从dict转list后，变成了无序的list")

    if needSort:
        dataList.sort()

    # 找到需要处理的第一条
    find = False
    while bufferCursor < len(dataList):

        # if int(dataList[bufferCursor].get('timestamp')) < workingStartTimeStamp:
        if dataList_keys[bufferCursor] < workingStartTimeStr:
            # 跳过这条废数据
            bufferCursor += 1
        else:
            # print (f"First content {dataList[bufferCursor]} 找到了!")
            find = True
            break

    if find:
        logger.debug(f"找到了需要处理的第一条：content:{dataList_keys[bufferCursor]}")
    else:
        logger.debug(f'最后一个文件读完了，啥也没有')
        return {}

    Yaxis = {}
    # 开始根据respXaxis的刻度，生成对应的respYdict
    while XaxisCursor < len(Xaxis):

        if Xaxis[XaxisCursor] <= dataList_keys[bufferCursor]:
            XaxisCursor += 1
        else:
            if Yaxis.get(str(Xaxis[XaxisCursor-1])):
                # Yaxis[str(Xaxis[XaxisCursor-1])].append(dataList_values[bufferCursor])
                Yaxis[str(Xaxis[XaxisCursor-1])] += dataList_values[bufferCursor]
            else:
                Yaxis[str(Xaxis[XaxisCursor-1])] = dataList_values[bufferCursor]

            # 如果buffer还没到底，就cursor+1
            if bufferCursor < (len(dataList_keys) - 1):
                bufferCursor += 1
            else:
                break
    return Yaxis
