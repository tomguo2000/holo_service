import time
import datetime, math, os, json, psutil
from flask import Blueprint, request
from common.setlog2 import logger
from common.timeUtils import Timeutils
from common.config import CONFIG, ReturnCode
from multiprocessing import Pool
import service.public, service.msService
import gc



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
            raise Exception ("110900")

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
                   "message": ReturnCode[ex.args[0]],
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


@holoview.route('/', methods=["GET"])
def holoview_index():
    try:
        # 检查入参
        try:
            params = request.args.to_dict()
            logger.info(f"有人调用holoview了，参数如下:{params}")

            vin = params['vin']
            date = params.get('date')
            startTime = params.get('startTime')
            endTime = params.get('endTime')
            env = params.get('env')
            overall = params.get('overall')
            signal = params.get('signal')
            Xinterval = int(params.get('Xinterval')) if params.get('Xinterval') else 30
            skipInvalidValue = params.get('skipInvalidValue') if params.get('skipInvalidValue') else False

            '''
            参数说明
            vin                 必选
            date                日期，可选，无默认值
            startTime           起始时间戳
            endTime             结束时间戳
            overall             整体指标，可选。后台默认值是event_ConnStatusList
            signal              信号指标，可选。后台默认值是IBS有关的10个信号
            Xinterval           时间间隔（秒），可选。后台默认值是30秒
            skipInvalidValue    信号指标是否跳过非法值，可选。后台默认是False
            如果同时传了日期和时间戳，已时间戳为准
            '''

            if not startTime:
                startTime = Timeutils.timeString2timeStamp(date, format="%Y-%m-%d", ms=True)
            if not endTime:
                endTime = startTime + (86400-1) * 1000

            startTime = int(startTime) if int(startTime) > 9999999999 else int(startTime) * 1000
            endTime = int(endTime) if int(endTime) > 9999999999 else int(endTime) * 1000
            overallList = overall.split(',') if overall else []
            signalList = signal.split(',') if signal else []

            # firstOnly 用来表示是否只处理 某canID在企标秒包里的第一个8字节信息？
            # 为True适用于较大时间刻度时, False适用于小时间刻度。
            firstOnly = True

        except:
            raise Exception ("110900")

        if not overallList:
            overallList = ['event_ConnStatusList']

        if not signalList:
            signalList = [
                'ME7_IBS_SOC_STATE',
                'ME7_IBS_SOC',
                'ME7_IBS_SOH_SUL',
                'ME7_IBS_U_BATT',
                'ME7_VCU_LVSmartChrg_Status',
                'ME7_VCU_DC_VoltageReq',
                'ME7_IBS_Status_Voltage',
                'ME7_BCM_SystemPowerMode',
                'ME7_ESP_VehicleSpeed',
                'ME7_DCDC_IdcLvCurr'
            ]

        # # 判断不要跨天，否则不干活
        # if Timeutils.timeStamp2timeString(startTime)[:10] != Timeutils.timeStamp2timeString(endTime)[:10]:
        #     raise Exception ("110903")
        # else:
        #     date = Timeutils.timeStamp2timeString(startTime)[:10]

        # 现在的做法是允许跨天，调用方自己控制

        # 得到日期List，用于拼接数据源的path
        dateList = service.public.createDateListByDuration(startTimestamp=startTime, endTimestamp=endTime)

        time0 = time.time()*1000
        logger.debug(f"0：可以了，咱现在从头开始。。。。。。{time0}")

        # 构建一个X轴
        Xaxis = createXaxis(startTime, endTime, interval=Xinterval)
        logger.debug(f"1：构建一个X轴完毕。。。{time.time()*1000-time0}")

        # 定义返回的resp
        resp = {}
        resp['Xaxis'] = Xaxis
        resp['dateList'] = dateList
        resp['YaxisList'] = []
        resp['YaxisOverall'] = {}
        resp['YaxisSignal'] = {}


        # 传入X轴和dateList，获取emq连接的event结果
        if "event_ConnStatusList" in overallList:
            time1 = time.time()*1000
            event_ConnStatusList = getConnStatus(vin, Xaxis, dateList)
            logger.debug(f"hhhh获取emq连接的event结果 完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['event_ConnStatusList'] = event_ConnStatusList
            resp['YaxisList'].append({"event_ConnStatusList": {
                "type": "event",
                "other": "........."
            }})
            logger.debug(f"2-1：event_ConnStatusList的event结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取SDK初始化的聚合结果
        if "event_VehicleLoginList" in overallList:
            time1 = time.time()*1000
            event_VehicleLoginList = getVehicleLoginEvents(vin, Xaxis, dateList)
            logger.debug(f"hhhh获取SDK初始化的聚合结果 完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['event_VehicleLoginList'] = event_VehicleLoginList
            resp['YaxisList'].append({"event_VehicleLoginList": {
                "type": "message",
                "other": "........."
            }})
            logger.debug(f"2-2：event_VehicleLoginList的event结果完毕。。。{time.time()*1000-time0}")

        # 传入X轴和dateList，获取控车event的结果
        if "event_RemoteCmdList" in overallList:
            time1 = time.time()*1000
            event_RemoteCmdList = getRemoteCmdEvents(vin, Xaxis, dateList)
            logger.debug(f"hhhh获取控车event的结果 完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['event_RemoteCmdList'] = event_RemoteCmdList
            resp['YaxisList'].append({"event_RemoteCmdList": {
                "type": "message",
                "other": "........."
            }})
            logger.debug(f"2-3：event_RemoteCmdList 的event结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取国标的报文条数结果
        if "message_tj32960Live" in overallList:
            time1 = time.time()*1000
            message_tj32960Live = getTJ32960(vin, Xaxis, 'message_national_live.txt', dateList)
            logger.debug(f"hhhh获取国标的报文条数结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_tj32960Live'] = message_tj32960Live
            resp['YaxisList'].append({"message_tj32960Live": {
                "type": "message",
                "other": "........."
            }})
            logger.debug(f"2-4：message_tj32960Live 的event结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取国标补发的报文条数结果
        if "message_tj32960Resent" in overallList:
            time1 = time.time()*1000
            message_tj32960Resent = getTJ32960(vin, Xaxis, 'message_national_resent.txt', dateList)
            logger.debug(f"hhhh获取国标的报文条数结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_tj32960Resent'] = message_tj32960Resent
            resp['YaxisList'].append({"message_tj32960Resent": {
                "type": "message",
                "other": "........."
            }})
            logger.debug(f"2-4：message_tj32960Resent 的event结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取企标的聚合结果
        if "message_MSLive" in overallList:
            time1 = time.time()*1000
            message_MSLive = getMS(vin, Xaxis, 'message_enterprise_live.txt', dateList)
            logger.debug(f"hhhh获取企标的聚合结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_MSLive'] = message_MSLive
            resp['YaxisList'].append({"message_MSLive": {
                "type": "message",
                "other": "........."
            }})
            logger.debug(f"2-5：message_MSLive 的结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取企标的聚合结果
        if "message_MSResent" in overallList:
            time1 = time.time()*1000
            message_MSResent = getMS(vin, Xaxis, 'message_enterprise_resent.txt', dateList)
            logger.debug(f"hhhh获取企标的聚合结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_MSResent'] = message_MSResent
            resp['YaxisList'].append({"message_MSResent": {
                "type": "message",
                "other": "........."
            }})
            logger.debug(f"2-6：message_MSResent 的结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取企标的聚合结果
        if "message_MSWarning" in overallList:
            time1 = time.time()*1000
            message_MSWarning = getMS(vin, Xaxis, 'message_enterprise_warning.txt', dateList)
            logger.debug(f"hhhh获取企标的聚合结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_MSWarning'] = message_MSWarning
            resp['YaxisList'].append({"message_MSWarning": {
                "type": "message",
                "other": "........."
            }})
            logger.debug(f"2-7：message_MSWarning 的结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取MISC的聚合结果
        if "message_MiscList" in overallList:
            time1 = time.time()*1000
            message_MiscList = getMisc(vin, Xaxis, dateList)
            logger.debug(f"hhhh获取MISC的聚合结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_MiscList'] = message_MiscList
            resp['YaxisList'].append({"message_MiscList": {
                "type": "message",
                "other": "........."
            }})
            logger.debug(f"2-8：message_MiscList 的结果完毕。。。{time.time()*1000-time0}")


        # 传入X轴和dateList，获取登入登出心跳的聚合结果
        if "message_HeartbeatList" in overallList:
            time1 = time.time()*1000
            message_HeartbeatList = getHeartbeat(vin, Xaxis, dateList)
            logger.debug(f"hhhh获取登入登出心跳的聚合结果完毕。。。{time.time()*1000-time1}")
            resp['YaxisOverall']['message_HeartbeatList'] = message_HeartbeatList
            resp['YaxisList'].append({"message_HeartbeatList": {
                "type": "message",
                "other": "........."
            }})
            logger.debug(f"2-9：message_HeartbeatList 的结果完毕。。。{time.time()*1000-time0}")


        # 传进来的signalList必须包含ME7_ 或者 ME5_这样的前缀
        if signalList:
            realSignalList = []
            vehicleModel = ''
            for x in signalList:
                vehicleModel = x.split('_')[0]
                realSignalList.append(x[4:])

            if vehicleModel not in ['ME7', 'ME5']:
                raise Exception("110904")

            # 天际企标的报文文件名
            dataSourcesLive = 'message_enterprise_live.txt'
            # 天际企标补发报文文件名
            dataSourcesResent = 'message_enterprise_resent.txt'
            # 天际企标报警报文文件名
            dataSourcesWaining = 'message_enterprise_warning.txt'

            # 获取需要读取的完整文件路径的列表
            fullPathList1 = service.public.getFullPathList(vin, dateList, dataSourcesLive)
            fullPathList2 = service.public.getFullPathList(vin, dateList, dataSourcesResent)
            fullPathList3 = service.public.getFullPathList(vin, dateList, dataSourcesWaining)

            logger.debug(f"4：获取到了需要读取的文件列表:{fullPathList1},{fullPathList2},{fullPathList3}。。。{time.time()*1000-time0}")

            # 获取三类报文的原始内容
            oriMessageList = service.public.getPureContents(fullPathList1)
            oriMessageListResent = service.public.getPureContents(fullPathList2)
            oriMessageListWarning = service.public.getPureContents(fullPathList3)
            logger.debug(f"5-0：获取三类报文的原始内容完毕。。。{time.time()*1000-time0}")

            # 按照秒格式，把三类报文的原始内容，根据时间段crop，然后转成字典格式
            oriMessageLiveCropedDict = service.msService.cropAndTransformer2dict(oriMessageList, startTime, endTime) \
                if oriMessageList else {}
            logger.debug(f"5-1：获取企标实发报文的string结果，裁剪后转为dict结果完毕。。。{time.time()*1000-time0}")
            del(oriMessageList)
            gc.collect()

            oriMessageResentCropedDict = service.msService.cropAndTransformer2dict(oriMessageListResent, startTime, endTime) \
                if oriMessageListResent else {}
            logger.debug(f"5-2：获取企标补发报文的string结果，裁剪后转为dict结果完毕。。。{time.time()*1000-time0}")
            del(oriMessageListResent)
            gc.collect()

            # 告警报文，是实发和补发混杂，要特殊处理下，先排序在去crop
            oriMessageWarningCropedDict = service.msService.cropWarningAndTransformer2dict(oriMessageListWarning, startTime, endTime) \
                if oriMessageListWarning else {}
            logger.debug(f"5-3：获取企标告警报文的string结果，裁剪后转为dict结果完毕。。。{time.time()*1000-time0}")
            del(oriMessageListWarning)
            gc.collect()


            # 组合实发,补发,告警报文， 组合后是乱序的
            combinedDict = dict(oriMessageLiveCropedDict, **oriMessageResentCropedDict)
            combinedDict = dict(combinedDict, **oriMessageWarningCropedDict)

            # 对字典进行排序后转成list
            sortedMessages = sorted(combinedDict.items(), key=lambda x:x[0])
            logger.debug(f"6: 实发补发告警组合完毕，开始要解析到信号了，到目前为止耗时: {time.time()*1000 - time0} ms")
            del(combinedDict)
            gc.collect()

            # 判断下传入的车型，和报文里读到的车型是否匹配，如不匹配，就别做下去了
            if sortedMessages:
                print(sortedMessages[0][1])
                msUploadProtol = sortedMessages[0][1][1]
                if vehicleModel != sortedMessages[0][1][0]:
                    raise Exception("110905", f'小天说了，你告诉我车型是{vehicleModel},可实际上报文是{sortedMessages[0][1][0]},你自己想想清楚先')
            msUploadProtol = None

            # 输入一段连续的报文list，根据X轴的实际情况，选取一组真正需要解析的报文。
            abstractionMessages = abstract(sortedMessages, Xaxis)

            del(sortedMessages)
            gc.collect()

            # 把canIDDict的生成放在这里，因为确认了用什么样的车型和组包协议
            # 根据singal判断需要解析哪些canid，假设查询时，signal前缀截取的vehicleModel是正确的，后面要跟报文中的实际车型对比
            canIDDict = service.msService.getCanIDListBySignalList(signalList=realSignalList,
                                                                   vehicleMode=vehicleModel,
                                                                   msUploadProtol=msUploadProtol)

            # 获取到每个信号的invalid值
            if skipInvalidValue:
                signalsInvalidValueDict = service.msService.getSignalsInvalidValues(signalList=realSignalList,
                                                                                    vehicleMode=vehicleModel)
            else:
                signalsInvalidValueDict = {}

            logger.debug(f"6+1：根据singal判断需要解析哪些canid 的结果完毕:{canIDDict}。。。{time.time()*1000-time0}")

            if not canIDDict:
                raise Exception("110900", '传入的signal,又一个或多个找不到对应的canid')

            # print(abstractionMessages)
            # Like this:
            # {'2021-07-02_03:47:10': (('ME7', '0a', '328e8026da3344106e8e7026da3344106f8e7026da334410608e8026da334410618e7026da334410628e7026da334410638e8026da334410648e7026da334410658e7026da334410668e8026da334410678e7026da334410688e7026da334410698e8026da3344106a8e8026da3344106b8e7026da3344106c8e7026da3344106d8e8026da3344106e8e7026da3344106f8e7026da334410608e8026da334410618e8026da334410628e7026da334410638e8026da334410648e7026da334410658e7026da334410668e8026da334410678e8026da334410688e7026da334410698e7026da3344106a8e7026da3344106b8e7026da3344106c8e8826da3344106d8e8026da3344106e8e7026da3344106f8e8026da334410608e7026da334410618e7026da334410628e7026da334410638e7026da334410648e7026da334410658e8026da334410668e7026da334410678e8026da334410688e8026da334410698e7026da3344106a8e7026da3344106b8e8026da3344106c8e7026da3344106d8e7026da3344106e8e7026da3344106f000a04004d4c00003c0004004d4c00003c0004004d4c00003c0004004d4c00003c0004004d4c00003c0004004d4c00003c0004004d4c00003c0004004d4c00003c0004004d4c00003c0004004d4c00003c00020f9216905249495b0f9216905249495c020008930b8f00000b0008930b8f00000c0000013f0a008a0200002601f13e1707fa31cf50019627107fff0000000001ec27101964e0000601ef1000108388b306017d11000032000806000000000001e00027103fff800d01f7001e007800000d010500000b000000000164923904a100f56c013a8422208223040c01fa3e8fa02a0a82ac01000000000000000d016400ae919100000001ad7fc3fe8000004c01002e3bff5352530501545454534803460001fb1003000208a1ca000001a20000058f8c004301000054000000004000000100f0fffcfc000000010000000000000000000001ad5080414c090000015253768e5a9e5608000001fff0001f2000005601590006ec0670000d010001ff00000000060001db0000000853000a000100255680006403000110600000fd3f686a0000010000000000000000010000000000000000010000000000000000010000000000000000010300000000000000010000801832120000013b07e07821d02a690100000000004b554c010011bb55400000000108000000000000000198750000012f0800015711bb68eb20363d01045340380010f919011079107f107f108001107f10761080108001107d107d107e107f01107e107c1080107f011080107f107e107e01108010821083108101108110821080107e01107f107d107f107f01104310801082107f01107f107d107f107f011082107e10801081011080107b107f107f01107f107d1074107d011080107c107c108101107f107f1083108001107f1081107f107e01107c107d107e107f01107b107e1080108101107e107c107f107f0110801080108110800110811081106b108101107f1080107f1081011081107e107c107d01107e107e107e107d01107e1080107e107f01107e107e107a107f0110801081107e1080014949494949494949014949484849494849014949494949484848014949494948494949014948484800000000000000000000010202000000000000'),), '2021-07-02_06:00:30': (('ME7', '0a', '1a8f302710010000028f302710010000038f402710010000048f302710010000058f302710010000068f402710010000078f302710010000088f302710010000098f3027100100000a8f4027100100000b8f4027100100000c8f3027100100000d8f4027100100000e8f4027100100000f8f402710010000008f402710010000018f402710010000028f402710010000038f402710010000048f402710010000058f402710010000068f302710010000078f302710010000088f402710010000098f4027100100000a8f4027100100000b070000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100ff00ffffffff01010000ff00ff0000010000019b000000020000020100000400ff80000b019727107fff0000010001e827101964e0000201d7100010ffc8b3020178100000320008020000000001240202aa0200007e01eb0027103fff800601f0001e00780c0006010000000b0000000001640000000000f565011684e02082a80474010000000000000004010000000000000006016400648e8e00006401e27fc3fe800000030100200fff284c2805014d4d4d694803460001760800000007e01001000000000000000001000000000000000001a20000058b50000301000054000000000001008000000000aa000100010018948e78ac0100f0fffcfc00000001000000000000000000011080009802001f00019a997de14a09000001ffff737aff9e5f0b018200005c4000000001080660af0103060001fff0001f2000000001590006ec06700000010001ff0000000006010001008000144c3701c2000000004d0005000100255680006403000100400010fd3f606b01acb0b9b25a5a5a67010000000000aa000001000000000000000001000000000000000001000000000000000001000000000000000001030000000000000001000080093212000000000001000000000000000001000000000000000000000113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e701ffffffffffffffff01ffffffffffffffff01ffffffffffffffff01ffffffffffffffff00010000000000000000010000000000000000010000000000000000010000000000000000010000000000646c0000010202000000000000'),), '2021-07-02_08:13:50': (('ME7', '0a', '328a90279019440c228a90279019440c238a90279019440c248a90279019440c258a90279019440c268a90279019440c278a90279019440c288a90279019440c298a90279019440c2a8a90279019440c2b8a90279019440c2c8a90278d19440c2d8a90278d19440c2e8a90278d19440c2f8a90278d19440c208a90278d19440c218a90278b19440c228a98278b19440c238a90278619440c248a90278619440c258a98278119440c268a98278119440c278a90277e19440c288a80277e19440c298a98277e19440c2a8a90277e19440c2b8a98277e19440c2c8a98277c19440c2d8a98277c19440c2e8a90277c19440c2f8a98277c19440c208a98277c19440c218a90277c19440c228a90277c19440c238a98277c19440c248a98277c19440c258a98277c19440c268a98277c19440c278a98277c19440c288a98277c19440c298a98277e19440c2a8a98278119440c2b8a90277e19440c2c8a98277e19440c2d8a98278119440c2e8a90277e19440c2f8a98277e19440c208a98277e19440c218a98277e19440c228a90277e19440c230a004000006d000000004000006d000000004000006d000000004000006d000000004000006d000000004000006d000000004000006d000000004000006d000000004000006d000000004000006d0000000a0400484700003c000400484700003c000400484700003c000400484700003c000400484700003c000400484700003c000400484700003c000400484700003c000400484700003c000400484700003c00020690168e5345453a0690168e5345453b02002091218e00000a002091218e00000b01367ffc9ffe842602010a0015030000000e01120b008a0200000a01e43aa6d4fa1153530188274d945b04050d013204fd0089006103015d274c086400001a018b8300107c88b70a013911000071080b0a01f17ffc000000000001860000000000000601f10100284100000901fa0000008000000901230a03aa0220708a011808274d4a2e800401fe001e0078000004010500000b0000000001640000000100f643013a8084207ec8037301fa3e8fa05f17c5f30100000000000000040166008c8e8e0000000181a922e58000004c01032dd40852644f0501505050758803460001051010800908b1bc01000000060000000001004000000000000001a20000058e880043010000540000000040010040641f0000a1000100010042948e78ac0100f0fffcfc000000010000000000000000010000465800100000019080009802001d0101acc482b249090000015253758c5a9e5608018200005c4000000001080660af01031607018641002190000000015980078006da0000010250c500960064050100c10080009c504401f80000003c4e0000000100257d853c64c300011000c010fd3f6c6b01b3b9b5b963625f61010000000000aa0000010000000000000000010000000000000000010000000000000000010000000000000000010100000000000000010000001c321200000000000108000000000000000198350000012f080000010015f03f0010f91c011008100c100d100d01100c1006100e100d01100b100b100b100d01100b100a100d100d01100d100c100c100b01100c100c100d100c01100c100c100a100a01100a100a100a100a010fd6100a100b100901100a1009100b100a01100c1009100b100b01100a1008100a100b01100a10091002100a01100a10071009100b01100b100b100d100a01100b100b100b100a011008100a1009100a0110061009100a100b0110091008100b100a01100b100a100c100a01100a100a0ff9100b01100a100a100a100b01100b10091008100801100b10091009100801100a100b1009100a01100a100a1006100a01100a100c1009100a014848484848484848014848484748484748014848484848474747014848484847484848014847484800000000000000000000010202000000000000'),), '2021-07-02_17:07:10': (('ME7', '0a', '2688b827100100000288b827100100000388b827100100000488b827100100000588b827100100000688c027100100000788b827100100000888b827100100000988c027100100000a88c027100100000b88c027100100000c88b827100100000d88b827100100000e88c027100100000f88b827100100000088c027100100000188b827100100000288c027100100000388b827100100000488c027100100000588b827100100000688b827100100000788c027100100000888c027100100000988b827100100000a88c027100100000b88c027100100000c88c027100100000d88b827100100000e88c027100100000f88c027100100000088c027100100000188b827100100000288b827100100000388c027100100000488c027100100000588b827100100000688b827100100000709000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000200ff00ffffffff011383168252414132020000ff00ff00000100ffffffff000002000001970000000200000e0100000400ff800007000001e427101964e0000e01ca000010ffc8b20e01771000003100080e0000000001240202aa0200007e01e50027103fff800801f2001e0078000008010000000b0000000001640000000000f6e7013a7ed4207d38033701000000000000000701000000000000000801640076828200007600000001760800000007e01001000000040000000001000000000000000001a20000058150000301000054000000000001008000000000aa000100010000948e78ac0100f0fffcfc0000000000011080049802001a000199b57dc54208000001ffff7178ff9e5f0b018200005c4000000001080660af0103150001fff000233000000001590007da07420000010001ff0000000005010001008000004b4501ce00000000430007000100259780006403000100400010fd3f606b01aab0acae5a59585a010000000000aa000001000000000000000001000000000000000001000000000000000001000000000000000000010000000932120000000000010000000000000000010000000000000000000100000000000000000113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e70113e713e713e713e701ffffffffffffffff01ffffffffffffffff01ffffffffffffffff01ffffffffffffffff01ffffffff00000000010000000000000000010000000000000000010000000000000000010000000000000000010000000000646c0000010202000000000000'),), '2021-07-02_19:20:30': (('ME7', '0a', '3287c8271d19440c2287d0271d19440c2387c8271d19440c2487d0271d19440c2587d0271d19440c2687c8271d19440c2787d0271d19440c2887d0271d19440c2987d0271d19440c2a87c8271d19440c2b87d0271d19440c2c87c8271d19440c2d87c8271d19440c2e87d0271d19440c2f87d0271d19440c2087d0271d19440c2187d0271d19440c2287d0271d19440c2387d0271d19440c2487c8271d19440c2587c8271d19440c2687d0271d19440c2787d0271d19440c2887d0271d19440c2987c8271d19440c2a87d0271d19440c2b87d0271d19440c2c87d0271d19440c2d87c8271d19440c2e87d0271d19440c2f87c8271d19440c2087d0271d19440c2187c8271d19440c2287c8271d19440c2387d0271d19440c2487d0271d19440c2587d0271d19440c2687d0271d19440c2787c8271d19440c2887d0271d19440c2987d0271d19440c2a87c8271d19440c2b87d0271d19440c2c87d0271d19440c2d87d0271d19440c2e87d0271d19440c2f87d0271d19440c2087d0271d19440c2187c8271d19440c2287d0271d19440c230a004000006b000000004000006b000000004000006b000000004000006b000000004000006b000000004000006b000000004000006b000000004000006b000000004000006b000000004000006b0000000a0400454400003c000400454400003c000400454400003c000400454400003c000400454400003c000400454400003c000400454400003c000400454400003c000400454400003c000400454400003c0002138416835142433f138416835142433002002084228300000f0020842183000000018284a1a00183f908010b0815020000000601120b008a0200000a01c9737671fa10fa5301af27227fff04050a0194000000c913630d01112710186400001a01178300106d08ba0a010511403431000b0a0164849d000000000f01800000000000000001df0100004100000f01fc0000008000000f01230a27aa0220708a01d80827233fff800e01f4001e007800000e010500000b0000000001640000000100f72c013a7de8207c58031c01fa3e8fa14c5314cc01000000000000000e0164000085850000000177a982b18000004e0100cd93ff4d4e4b05014b4b4c4f8803460001781011e00a08a1b301000000060000000001004000000000000001a20000058484004301000054000000004001004064640000a5000100010042948e78ac0100f0fffcfc000000010000000000000000010000424d00100000019080009802001d0101ac8d81844408000001505271885a9f5608018200005c4000000001080660af010716070180500023c000000001598007fc07880000010010a60006000a050100c1008000004c5501f00000003c4a000c00010025a08000640300011000c010fd3f6c6b01a9afabad59585658010000000000aa0000010000000000000000010000000000000000010000000000000000010000000000000000010100000000000000010000001a321200000000000108000000000000000198350000012f080000010015f03f0010f91c010fb60fba0fbb0fbb010fbb0fb50fbb0fbb010fb90fb90fb90fba010fba0fb80fbb0fbb010fbb0fba0fb90fba010fbc0fbc0fbc0fbc010fbb0fbc0fba0fba010fba0fb90fbb0fba010f8b0fbb0fbb0fba010fbb0fb90fba0fba010fbc0fba0fbb0fbb010fbb0fb70fba0fba010fba0fb90fb30fb9010fba0fb80fb90fbb010fba0fba0fbd0fba010fbb0fbb0fbb0fba010fb90fb90fba0fba010fb70fb90fbb0fbb010fb90fb90fba0fba010fbb0fbb0fbb0fba010fbb0fbb0fab0fbb010fbb0fbb0fbb0fbb010fbb0fba0fb90fb9010fba0fb90fb90fb8010fba0fba0fb90fba010fba0fba0fb60fba010fba0fbb0fb90fba014242424242424242014242424242424142014242424242424141014242424242424242014242424200000000000000000000010202000000000000'),)}

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

            # 每个信号占1行，每行是所有的秒信号
            signalListFor1Line = transformer2Yaxis(canIDDict, respContents)
            logger.debug(f"8: 把解析信号group完成，到目前为止耗时: {time.time()*1000 - time0} ms")

            for oneSignalAllSec in signalListFor1Line:
                _signalName = oneSignalAllSec[0]
                _signalAllValues = oneSignalAllSec[1]

                resp['YaxisSignal'][_signalName] = _signalAllValues
                _signalInfo = service.msService.getSignalInfo(signalName=_signalName,vehicleModel=vehicleModel)
                resp['YaxisList'].append({_signalName: {
                    "type": "signal",
                    "choices": _signalInfo['choices'],
                    "maximum": _signalInfo['maximum'],
                    "minimum": _signalInfo['minimum']
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
                   "message": ex.args[1] if len(ex.args) > 1 else ReturnCode[ex.args[0]],
                   "businessObj": None
               }, 200


def abstract(sortedMessages, Xaxis):
    # print(sortedMessages[0])

    # 设2个指针：Xaxis指针， buffer指针。 赋值到Yaxis的字典里。
    XaxisCursor = 0
    bufferCursor = 0

    abstractionMessages = {}

    # 传进来的参数有值再说，否则直接返回空dict
    if sortedMessages:
        # 开始根据respXaxis的刻度，生成对应的respYdict
        while XaxisCursor < len(Xaxis):
            # 寻找第一个有值的刻度
            if Xaxis[XaxisCursor] <= sortedMessages[bufferCursor][0]:
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
    return abstractionMessages

def transformer2Yaxis(canIDDict, contents):
    # 得到要输出signal的list
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

    return signalYaxisList



def createXaxis(startTime, endTime, interval=10):

    workingStartTimeStamp = startTime
    workingEndTimeStamp = endTime

    # xAxisTotal: X轴的点位数量
    xAxisTotal = math.ceil((workingEndTimeStamp - workingStartTimeStamp) / (interval*1000))

    # respXaxis: 用于绘图的X轴的坐标list
    respXaxis = []
    timeCursor = workingStartTimeStamp
    while xAxisTotal:
        respXaxis.append(Timeutils.timeStamp2timeString(timeCursor))
        timeCursor += interval * 1000
        xAxisTotal -= 1

    return respXaxis


def getTJ32960(vin, Xaxis, messagetype, dateList):
    # 天际国标的报文文件名
    dataSources = messagetype

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources)

    # readKeys = [['MPUTime'], ['TYPE_CMD']]
    readKeys = [['MPUTime']]
    # 获取内容
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    # 假设实发和补发没有重复的部分。不需要进行去重的动作

    Y32960 = assignAmount2TimeSlot(Xaxis, oriMessageList, needSort=False)
    return Y32960


def getMS(vin, Xaxis, messagetype, dateList):
    time0 = time.time()*1000

    # 天际企标的报文文件名
    dataSourcesLive = messagetype


    # 获取需要读取的文件列表
    time1 = time.time()*1000
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSourcesLive)


    # 读取必要的message
    time1 = time.time()*1000
    readKeys = [['MPUTime']]
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)


    # 把企标分发到Y轴
    time1 = time.time()*1000
    YMSdict = assignAmount2TimeSlot(Xaxis, oriMessageList, needSort=False)

    return YMSdict


def getMisc(vin, Xaxis, dateList):
    # 天际的报文文件名
    dataSources = 'message_misc.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources)

    readKeys = [['MPUTime']]
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    YMisc = assignAmount2TimeSlot(Xaxis, oriMessageList)

    return YMisc


def getHeartbeat(vin, Xaxis, dateList):
    # 天际的报文文件名
    dataSources = 'message_hearbeat.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources)

    readKeys = [['MPUTime']]
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    YHeartbeat = assignAmount2TimeSlot(Xaxis, oriMessageList)

    return YHeartbeat


def getVehicleLoginEvents(vin, Xaxis, dateList):
    # 天际的报文文件名
    dataSources = 'event_vehicle.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources)

    # readKeys = [['timestamp'], ['event']]
    readKeys = ['.']
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    # readKeys用['.']的时候，通畅需要对返回的List做二次加工
    respMessageList = []
    for _item in oriMessageList:
        _temp = json.loads(_item)

        if _temp['timestamp'] < 9999999999:
            _temp['timestamp'] = _temp['timestamp'] * 1000

        respMessageList.append({'timestamp': _temp['timestamp'],
                                'timestr': Timeutils.timeStamp2timeString(_temp['timestamp']),
                                'event': _temp['event']})

    YHeartbeat = assignArray2TimeSlot(Xaxis, respMessageList)

    return YHeartbeat


def getRemoteCmdEvents(vin, Xaxis, dateList):
    # 天际的报文文件名
    dataSources = 'event_remote_cmd.txt'

    # 获取需要读取的文件列表
    fullPathList1 = service.public.getFullPathList(vin, dateList, dataSources)

    # readKeys = [['timestamp'], ['event']]
    readKeys = ['.']
    oriMessageList = service.public.getOriMessageList(fullPathList1, readKeys)

    # readKeys用['.']的时候，通畅需要对返回的List做二次加工
    respMessageList = []
    for _item in oriMessageList:

        try:

            _temp = json.loads(_item)

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
        respMessageList.append(json.loads(x))

    YHeartbeat = assignArray2TimeSlot(Xaxis, respMessageList)

    return YHeartbeat


def getConnStatus(vin, Xaxis, dateList):
    previousConnStatus = getPreviousConnStatus(vin, dateList[0])

    dataSource = 'event_emq_conn.txt'
    fullPathList = service.public.getFullPathList(vin, dateList, dataSource)

    respMessageList = []
    respMessageList.append({'timestamp': Timeutils.timeString2timeStamp(Xaxis[0], ms=True),
                            'timestr': Xaxis[0],
                            'event': previousConnStatus['event']})


    readKeys = ['.']
    oriMessageList = service.public.getOriMessageList(fullPathList, readKeys)

    # readKeys用['.']的时候，通畅需要对返回的List做二次加工
    for _item in oriMessageList:
        _temp = json.loads(_item)

        if _temp['timestamp'] < 9999999999:
            _temp['timestamp'] = _temp['timestamp'] * 1000

        respMessageList.append({'timestamp': _temp['timestamp'],
                                'timestr': Timeutils.timeStamp2timeString(_temp['timestamp']),
                                'event': _temp['event']})

    YConnList = assignArray2TimeSlot(Xaxis, respMessageList)

    return YConnList


def getPreviousConnStatus(vin, date):
    dataSource = 'event_emq_conn.txt'
    startDateStr = Timeutils.timeArray2timeString(Timeutils.timeString2timeArray(date, format="%Y-%m-%d")
                                                  + datetime.timedelta(days=-1), format="%Y-%m-%d")
    while True:
        fullPathList = service.public.getFullPathList(vin=vin,
                                                      dateList=[startDateStr],
                                                      dataSources=dataSource)
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
        connEvent = json.loads(connStatusList[-1])
        previousConnStatus = {'event': connEvent.get('event')}

    return previousConnStatus


# 按照Xaxis的刻度，把没有值的刻度填充无效值
def makeResponse(resp):
    makeResp = {}
    makeResp['Xaxis'] = resp['Xaxis']
    makeResp['dateList'] = resp['dateList']
    makeResp['YaxisList'] = resp['YaxisList']
    makeResp['YaxisSignal'] = {}
    makeResp['YaxisOverall'] = {}
    del(resp['Xaxis'])
    del(resp['dateList'])
    del(resp['YaxisList'])

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
            if Yaxis.get(str(Xaxis[XaxisCursor-1])):
                Yaxis[str(Xaxis[XaxisCursor-1])] += 1
            else:
                Yaxis[str(Xaxis[XaxisCursor-1])] = 1

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
