from service.gb32960.public_fun import *

# 用于解析实发补发国标报文的content部分
class parse_tj_ns_message:
    def __init__(self, data, ):

        self.model=['01', '02', '03', '04', '05', '06', '07', '08', '09']
        self.sourcedata = data

        self.rawdict = {'数据采集时间': self.sourcedata[:12]}
        self.dict = {'数据采集时间': get_datetime(self.rawdict['数据采集时间'])}

        data_f = data[12:]
        message_segment_header = data[12:14]

        # 要做的list
        self.mo_list = self.model
        # 做完了的list
        self.do_list = []

        while (message_segment_header in self.mo_list):

            # 记录已执行的
            self.do_list.append(message_segment_header)
            # 删除已执行的
            self.mo_list.remove(message_segment_header)

            # 解析01整车数据部分
            if message_segment_header == '01':
                self.f_01 = fun_01(data_f)
                message_segment_header = self.f_01.get('message_segment_header')
                data_f =  self.f_01.get('data_f')

                continue

            # 解析02驱动电机数据
            elif message_segment_header == '02':
                self.f_02 = fun_02(data_f)
                message_segment_header = self.f_02.get('message_segment_header')
                data_f =  self.f_02.get('data_f')
                continue

            # 03燃料电池数据
            elif message_segment_header == '03':
                self.f_03 = fun_03(data_f)
                message_segment_header = self.f_03.get('message_segment_header')
                data_f =  self.f_03.get('data_f')
                continue

            # 04发动机数据
            elif message_segment_header == '04':
                self.f_04 = fun_04(data_f)
                message_segment_header = self.f_04.get('message_segment_header')
                data_f =  self.f_04.get('data_f')
                continue

            # 05车辆位置数据
            elif message_segment_header == '05':
                self.f_05 = fun_05(data_f)
                message_segment_header = self.f_05.get('message_segment_header')
                data_f =  self.f_05.get('data_f')
                continue

            # 06极值数据
            elif message_segment_header == '06':
                self.f_06 = fun_06(data_f)
                message_segment_header = self.f_06.get('message_segment_header')
                data_f =  self.f_06.get('data_f')
                continue

            # 报警数据
            elif message_segment_header == '07':
                self.f_07 = fun_07(data_f)
                message_segment_header = self.f_07.get('message_segment_header')
                data_f =  self.f_07.get('data_f')
                continue

            # 电池电压数据
            elif message_segment_header == '08':
                self.f_08 = fun_08(data_f)
                message_segment_header = self.f_08.get('message_segment_header')
                data_f =  self.f_08.get('data_f')
                continue

            # 电池温度数据
            elif message_segment_header == '09':
                self.f_09 = fun_09(data_f)
                message_segment_header = self.f_09.get('message_segment_header')
                data_f =  self.f_09.get('data_f')
                continue

            else:
                # print(f"Enovatemotors national message parse finish... left: {data_f} Char.")
                self.leftstr = data_f

        self.do_list.sort()
        for i in self.do_list:
            if i == '01':
                self.rawdict = dict(self.rawdict, **self.f_01.get('oj2'))

                self.dict = dict(self.dict, **self.f_01.get('pj2'))

            elif i == '02':
                self.rawdict = dict(self.rawdict, **self.f_02.get('oj2'))

                self.dict = dict(self.dict, **self.f_02.get('pj2'))

            elif i == '03':
                self.rawdict = dict(self.rawdict, **self.f_03.get('oj2'))

                self.dict = dict(self.dict, **self.f_03.get('pj2'))

            elif i == '04':
                self.rawdict = dict(self.rawdict, **self.f_04.get('oj2'))

                self.dict = dict(self.dict, **self.f_04.get('pj2'))

            elif i == '05':
                self.rawdict = dict(self.rawdict, **self.f_05.get('oj2'))

                self.dict = dict(self.dict, **self.f_05.get('pj2'))

            elif i == '06':
                self.rawdict = dict(self.rawdict, **self.f_06.get('oj2'))

                self.dict = dict(self.dict, **self.f_06.get('pj2'))

            elif i == '07':
                self.rawdict = dict(self.rawdict, **self.f_07.get('oj2'))

                self.dict = dict(self.dict, **self.f_07.get('pj2'))

            elif i == '08':
                self.rawdict = dict(self.rawdict, **self.f_08.get('oj2'))

                self.dict = dict(self.dict, **self.f_08.get('pj2'))

            elif i == '09':
                self.rawdict = dict(self.rawdict, **self.f_09.get('oj2'))

                self.dict = dict(self.dict, **self.f_09.get('pj2'))

        # print(f"Enovatemotors national message parse finish... left char: {data_f} .")
        self.leftstr = data_f


## 01整车信息
def fun_01(data):
    cf = [1, 1, 1, 2, 4, 2, 2, 1, 1, 1, 2, 1, 1]
    cf_a = hexlist2(cf)
    data = data[2:]
    sourcedata = data[0:cf_a[-1]]
    list_o = [
        '车辆状态',
        '充电状态',
        '运行模式',
        '车速',
        '累计里程',
        '总电压',
        '总电流',
        'SOC',
        'DC-DC状态',
        '挡位',
        '绝缘电阻',
        '加速踏板行程值',
        '制动踏板状态',
    ]
    sourcedataj = list2dict(sourcedata, list_o, cf_a)
    sourcedataj2 = {'整车数据': sourcedataj}
    sourcedatal = pd.DataFrame([sourcedataj]).reindex(columns=list_o)
    pj = {
        '车辆状态': dict_list_replace("07_02_01_01", sourcedataj['车辆状态']),
        '充电状态': dict_list_replace("07_02_01_02", sourcedataj['充电状态']),
        '运行模式': dict_list_replace("07_02_01_03", sourcedataj['运行模式']),
        '车速': hex2dec(sourcedataj['车速'], k=0.1),
        '累计里程': hex2dec(sourcedataj['累计里程'], k=0.1),
        '总电压': hex2dec(sourcedataj['总电压'], k=0.1),
        '总电流': hex2dec(sourcedataj['总电流'], n=-1000, k=0.1),
        'SOC': hex2dec(sourcedataj['SOC']),
        'DC-DC状态': dict_list_replace("07_02_01_06", sourcedataj['DC-DC状态']),
        '挡位': fun_07_02_01_10(sourcedataj['挡位']),
        '绝缘电阻': hex2dec(sourcedataj['绝缘电阻']),
        '加速踏板行程值': fun_07_02_01_12(sourcedataj['加速踏板行程值']),
        '制动踏板状态': fun_07_02_01_13(sourcedataj['制动踏板状态']),
    }
    pj2 = {'整车数据': pj}
    pl = pd.DataFrame([pj]).reindex(columns=list_o)

    next = data[len(sourcedata):]
    nextMark = data[len(sourcedata):len(sourcedata) + 2]

    return {'data_f': next,
            'message_segment_header': nextMark,
            'data_07_02_01': sourcedata,
            "pj2": pj2,
            "oj2": sourcedataj2}
# 02_01_10 挡位
def fun_07_02_01_10(data):
    n = '{:08b}'.format(int(data, 16))
    dangwei = n[-4:]
    zhidongli = n[-5]
    qudongli = n[-6]

    # 挡位
    if dangwei == '0000':
        dangwei_s = '空挡'
    elif dangwei == '1101':
        dangwei_s = '倒挡'
    elif dangwei == '1110':
        dangwei_s = '自动D挡'
    elif dangwei == '1111':
        dangwei_s = '停车P挡'
    else:
        dangwei_s = (str(int(dangwei, 2)) + "档")

    # 制动力
    if zhidongli == "1":
        zhidongli_s = "有制动力"
    else:
        zhidongli_s = "无制动力"

        # 驱动力
    if qudongli == "1":
        qudongli_s = "有驱动力"
    else:
        qudongli_s = "无驱动力"

    output = [n, dangwei_s, zhidongli_s, qudongli_s]
    return output
# 02_01_12 加速踏板行程值
def fun_07_02_01_12(data):
    data = data.upper()
    if data == 'FE':
        return "异常"
    elif data == "FF":
        return "无效"
    else:
        return hex2dec(data)
# 02_01_13 制动踏板状态
def fun_07_02_01_13(data):
    data = data.upper()
    if data == 'FE':
        return "异常"
    elif data == "FF":
        return "无效"
    elif data == "65":
        return "制动有效"
    else:
        return hex2dec(data)


# 02驱动电机部分的解析
def fun_02(data):
    data = data[2:]
    dj_n_o = data[0:2]
    dj_n_j = hex2dec(dj_n_o)  # 电机个数

    cf_u = [1, 1, 1, 2, 2, 1, 2, 2]
    cf = [1] + cf_u * dj_n_j
    cf_a = hexlist2(cf)

    sourcedata = data[0:cf_a[-1]]
    list_o = [
        "驱动电机个数",
        "驱动电机序号",
        "驱动电机状态",
        "驱动电机控制器温度",
        "驱动电机转速",
        "驱动电机转矩",
        "驱动电机温度",
        "电机控制器输入电压",
        "电机控制器直流母线电流",
    ]

    sourcedataj = fun_07_02_02_oj(sourcedata, cf, dj_n_o, dj_n_j)
    sourcedataj2 = {'驱动电机数据': sourcedataj}
    sourcedatal = pd.DataFrame([sourcedataj]).reindex(columns=list_o)
    pj = {
        "驱动电机个数": dj_n_j,
        "驱动电机序号": [hex2dec(i) for i in sourcedataj['驱动电机序号']],
        "驱动电机状态": [dict_list_replace('07_02_02_02', i) for i in sourcedataj['驱动电机状态']],
        "驱动电机控制器温度": [hex2dec(i, n=-40) for i in sourcedataj['驱动电机控制器温度']],
        "驱动电机转速": [hex2dec(i, n=-20000) for i in sourcedataj['驱动电机转速']],
        "驱动电机转矩": [hex2dec(i, k=0.1, n=-2000) for i in sourcedataj['驱动电机转矩']],
        "驱动电机温度": [hex2dec(i, n=-40) for i in sourcedataj['驱动电机温度']],
        "电机控制器输入电压": [hex2dec(i, k=0.1) for i in sourcedataj['电机控制器输入电压']],
        "电机控制器直流母线电流": [hex2dec(i, k=0.1, n=-1000) for i in sourcedataj['电机控制器直流母线电流']],
    }
    pj2 = {'驱动电机数据': pj}
    pl = pd.DataFrame([pj]).reindex(columns=list_o)

    next = data[len(sourcedata):]
    nextMark = data[len(sourcedata):len(sourcedata) + 2]


    return {'data_f': next,
            'message_segment_header': nextMark,
            'data_07_02_01': sourcedata,
            "pj2": pj2,
            "oj2": sourcedataj2}
#  oj
def fun_07_02_02_oj(sourcedata, cf, dj_n_o, dj_n_j):
    data = sourcedata[2:]
    cf_a = hexlist2(cf[1:])
    dict_oj = {
        "驱动电机个数": dj_n_o,
    }

    list_o = [
        "驱动电机序号",
        "驱动电机状态",
        "驱动电机控制器温度",
        "驱动电机转速",
        "驱动电机转矩",
        "驱动电机温度",
        "电机控制器输入电压",
        "电机控制器直流母线电流",
    ]

    dict_oj_u = {
        "驱动电机序号": [],
        "驱动电机状态": [],
        "驱动电机控制器温度": [],
        "驱动电机转速": [],
        "驱动电机转矩": [],
        "驱动电机温度": [],
        "电机控制器输入电压": [],
        "电机控制器直流母线电流": [],
    }
    for i in range(dj_n_j):
        for j in range(len(dict_oj_u)):
            data_unit = data[cf_a[i * len(dict_oj_u) + j]:cf_a[i * len(dict_oj_u) + j + 1]]
            dict_oj_u[list_o[j]].append(data_unit)

    dict_all = dict(dict_oj, **dict_oj_u)
    return dict_all


# 03
def fun_03(data):
    cf = [2, 2, 2, 2, None, 2, 1, 2, 1, 2, 1, 1]
    data = data[2:]
    dc_no = data[12:16]
    dc_np = hex2dec(dc_no)
    cf[4] = dc_np
    cf_a = hexlist2(cf)

    sourcedata = data[0:cf_a[-1]]

    list_o = [
        "燃料电池电压",
        "燃料电池电流",
        "燃料消耗率",
        "燃料电池温度探针总数",
        "探针温度值",
        "氢系统中最高温度",
        "氢系统中最高温度探针代号",
        "氢气最高浓度",
        "氢气最高浓度传感器代号",
        "氢气最高压力",
        "氢气最高压力传感器代号",
        "高压DC/DC状态",
    ]
    sourcedataj = list2dict(sourcedata, list_o, cf_a)
    sourcedataj2 = {'燃料电池数据': sourcedataj}
    sourcedatal = pd.DataFrame.from_dict(sourcedataj, orient='index').T
    pj = {
        "燃料电池电压": hex2dec(sourcedataj['燃料电池电压'], k=0.1),
        "燃料电池电流": hex2dec(sourcedataj['燃料电池电流'], k=0.1),
        "燃料消耗率": hex2dec(sourcedataj['燃料消耗率'], k=0.01),
        "燃料电池温度探针总数": hex2dec(sourcedataj['燃料电池温度探针总数']),
        "探针温度值": [hex2dec(i, n=-40, k=0.1) for i in sourcedataj['燃料电池温度探针总数']],
        "氢系统中最高温度": hex2dec(sourcedataj['氢系统中最高温度'], n=-40, k=0.1),
        "氢系统中最高温度探针代号": hex2dec(sourcedataj['氢系统中最高温度探针代号']),
        "氢气最高浓度": hex2dec(sourcedataj['氢气最高浓度']),
        "氢气最高浓度传感器代号": hex2dec(sourcedataj['氢气最高浓度传感器代号']),
        "氢气最高压力": hex2dec(sourcedataj['氢气最高压力'], k=0.1),
        "氢气最高压力传感器代号": hex2dec(sourcedataj['氢气最高压力传感器代号']),
        "高压DC/DC状态": dict_list_replace('07_02_03_12', sourcedataj['高压DC/DC状态']),
    }
    pj2 = {'燃料电池数据': pj}
    pl = pd.DataFrame.from_dict(pj, orient='index').T

    next = data[len(sourcedata):]
    nextMark = data[len(sourcedata):len(sourcedata) + 2]

    return {'data_f': next,
            'message_segment_header': nextMark,
            'data_07_02_01': sourcedata,
            "pj2": pj2,
            "oj2": sourcedataj2}


#_04
def fun_04(data):
    cf = [1, 2, 2]
    cf_a = hexlist2(cf)
    data = data[2:]
    sourcedata = data[0:cf_a[-1]]
    list_o = [
        "发动机状态",
        "曲轴转速",
        "燃料消耗率",
    ]
    sourcedataj = list2dict(sourcedata, list_o, cf_a)
    sourcedataj2 = {'发动机数据': sourcedataj}
    sourcedatal = pd.DataFrame.from_dict(sourcedataj, orient='index').T
    pj = {
        "发动机状态": dict_list_replace("07_02_01_01", sourcedataj['发动机状态']),
        "曲轴转速": fun_07_02_04_02(sourcedataj['曲轴转速']),
        "燃料消耗率": fun_07_02_04_03(sourcedataj['燃料消耗率']),
    }
    pj2 = {'发动机数据': pj}
    pl = pd.DataFrame.from_dict(pj, orient='index').T

    next = data[cf_a[-1]:]
    nextMark = data[cf_a[-1]:cf_a[-1] + 2]

    return {'data_f': next,
            'message_segment_header': nextMark,
            'data_07_02_01': sourcedata,
            "pj2": pj2,
            "oj2": sourcedataj2}
# 02_04_02 曲轴转速
def fun_07_02_04_02(data):
    data = data.upper()
    if data == 'FFFE':
        return "异常"
    elif data == "FFFF":
        return "无效"
    else:
        return hex2dec(data)
# 02_04_03 燃料消耗率
def fun_07_02_04_03(data):
    data = data.upper()
    if data == 'FFFE':
        return "异常"
    elif data == "FFFF":
        return "无效"
    else:
        return hex2dec(data, k=0.01)


# 05车辆位置
def fun_05(data):
    cf = [1, 4, 4]
    cf_a = hexlist2(cf)
    data = data[2:]
    sourcedata = data[0:cf_a[-1]]
    list_o = [
        "定位状态",
        "经度",
        "纬度",
    ]
    sourcedataj = list2dict(sourcedata, list_o, cf_a)
    sourcedataj2 = {'车辆位置数据': sourcedataj}
    sourcedatal = pd.DataFrame([sourcedataj]).reindex(columns=list_o)
    pj = {
        '定位状态': fun_07_02_05_01(sourcedataj['定位状态']),
        '经度': hex2dec(sourcedataj['经度'], k=0.000001),
        '纬度': hex2dec(sourcedataj['纬度'], k=0.000001),
    }
    pj2 = {'车辆位置数据': pj}
    pl = pd.DataFrame([pj]).reindex(columns=list_o)

    next = data[len(sourcedata):]
    nextMark = data[len(sourcedata):len(sourcedata) + 2]

    return {'data_f': next,
            'message_segment_header': nextMark,
            'data_07_02_01': sourcedata,
            "pj2": pj2,
            "oj2": sourcedataj2}
def fun_07_02_05_01(data):
    n = '{:08b}'.format(int(data, 16))
    state = n[-1]
    longitude = n[-2]
    latitude = n[-3]

    if state == '0':
        state_s = "定位有效"
    else:
        state_s = "定位无效"

    if longitude == '0':
        longitude_s = "北纬"
    else:
        longitude_s = "南纬"

    if latitude == '0':
        latitude_s = "东经"
    else:
        latitude_s = "西经"

    output = [n, state_s, longitude_s, latitude_s]
    return output


# fun_06
def fun_06(data):
    cf = [1, 1, 2, 1, 1, 2, 1, 1, 1, 1, 1, 1]
    cf_a = hexlist2(cf)
    data = data[2:]
    sourcedata = data[0:cf_a[-1]]
    list_o = [
        "最高电压电池子系统号",
        "最高电压电池单体代号",
        "电池单体电压最高值",
        "最低电压电池子系统号",
        "最低电压电池单体代号",
        "电池单体电压最低值",
        "最高温度子系统号",
        "最高温度探针序号",
        "最高温度值",
        "最低温度子系统号",
        "最低温度探针序号",
        "最低温度值",
    ]
    sourcedataj = list2dict(sourcedata, list_o, cf_a)
    sourcedataj2 = {'极值数据': sourcedataj}
    sourcedatal = pd.DataFrame([sourcedataj]).reindex(columns=list_o)
    pj = {
        '最高电压电池子系统号': hex2dec(sourcedataj['最高电压电池子系统号'], e=True),
        '最高电压电池单体代号': hex2dec(sourcedataj['最高电压电池单体代号'], e=True),
        '电池单体电压最高值': hex2dec(sourcedataj['电池单体电压最高值'], k=0.001, e=True),
        '最低电压电池子系统号': hex2dec(sourcedataj['最低电压电池子系统号'], e=True),
        '最低电压电池单体代号': hex2dec(sourcedataj['最低电压电池单体代号'], e=True),
        '电池单体电压最低值': hex2dec(sourcedataj['电池单体电压最低值'], k=0.001, e=True),
        '最高温度子系统号': hex2dec(sourcedataj['最高温度子系统号'], e=True),
        '最高温度探针序号': hex2dec(sourcedataj['最高温度探针序号'], e=True),
        '最高温度值': hex2dec(sourcedataj['最高温度值'], n=-40, e=True),
        '最低温度子系统号': hex2dec(sourcedataj['最低温度子系统号'], e=True),
        '最低温度探针序号': hex2dec(sourcedataj['最低温度探针序号'], e=True),
        '最低温度值': hex2dec(sourcedataj['最低温度值'], n=-40, e=True),
    }
    pj2 = {'极值数据': pj}
    pl = pd.DataFrame([pj]).reindex(columns=list_o)

    next = data[len(sourcedata):]
    nextMark = data[len(sourcedata):len(sourcedata) + 2]

    return {'data_f': next,
            'message_segment_header': nextMark,
            'data_07_02_01': sourcedata,
            "pj2": pj2,
            "oj2": sourcedataj2}


# 07报警信息
def fun_07(data):
    cf = [1, 4, 1, 1, 1, 1]
    cf_a = hexlist2(cf)
    data = data[2:]
    sourcedata = data[0:cf_a[-1]]
    list_o = [
        "最高报警等级",
        "通用报警标志",
        "可充电储能装置故障总数N1",
        "驱动电机故障总数N2",
        "发动机故障总数N3",
        "其他故障总数N4",
    ]
    sourcedataj = list2dict(sourcedata, list_o, cf_a)
    sourcedataj2 = {'报警数据': sourcedataj}
    sourcedatal = pd.DataFrame([sourcedataj]).reindex(columns=list_o)
    pj = {
        '最高报警等级': hex2dec(sourcedataj['最高报警等级'], e=True),
        '通用报警标志': fun_07_02_07_02(sourcedataj['通用报警标志']),
        '可充电储能装置故障总数N1': hex2dec(sourcedataj['可充电储能装置故障总数N1'], e=True),
        '驱动电机故障总数N2': hex2dec(sourcedataj['驱动电机故障总数N2'], e=True),
        '发动机故障总数N3': hex2dec(sourcedataj['发动机故障总数N3'], e=True),
        '其他故障总数N4': hex2dec(sourcedataj['其他故障总数N4'], e=True),
    }
    pj2 = {'报警数据': pj}
    pl = pd.DataFrame([pj]).reindex(columns=list_o)

    next = data[len(sourcedata):]
    nextMark = data[len(sourcedata):len(sourcedata) + 2]

    return {'data_f': next,
            'message_segment_header': nextMark,
            'data_07_02_01': sourcedata,
            "pj2": pj2,
            "oj2": sourcedataj2}
def fun_07_02_07_02(data):
    n = '{:032b}'.format(int(data, 16))

    baojing_list = [
        "温度差异报警",
        "电池高温报警",
        "车载储能装置类型过压报警",
        "车载储能装置类型欠压报警",
        "SOC低报警",
        "单体电池过压报警",
        "单体电池欠压报警",
        "SOC过高报警",
        "SOC跳变报警",
        "可充电储能系统不匹配报警",
        "电池单体一致性差报警",
        "绝缘报警",
        "DC-DC温度报警",
        "制动系统报警",
        "DC-DC状态报警",
        "驱动电机控制器温度报警",
        "高压互锁状态报警",
        "驱动电机温度报警",
        "车载储能装置类型过充",
    ]

    baojing = [n]

    for i in range(0, 19):
        if n[-i] == "1":
            baojing.append(baojing_list[i])

    return baojing


# 08电池电压
def fun_08(data):
    data = data[2:]
    sourcedata = data
    dj_n_o = data[0:2]
    dj_n_j = hex2dec(dj_n_o)  # 电池个数

    cf_u = [1, 1]
    cf = fun_07_02_08_cf(sourcedata, dj_n_j)
    cf_a = hexlist2(cf)
    sourcedata = data[0:cf_a[-1]]
    list_o = [
        "可充电储能子系统个数",
        "可充电储能子系统号",
        "可充电储能装置电压",
        "可充电储能装置电流",
        "单体电池总数",
        "本帧起始电池序号",
        "本帧单体电池总数",
        "单体电池电压",
    ]

    sourcedataj = fun_07_02_08_oj(sourcedata, cf, dj_n_o, dj_n_j)
    sourcedataj2 = {'可充电储能装置电压数据': sourcedataj}
    sourcedatal = pd.DataFrame([sourcedataj]).reindex(columns=list_o)
    pj = {
        "可充电储能子系统个数": dj_n_j,
        "可充电储能子系统号": [hex2dec(i) for i in sourcedataj['可充电储能子系统号']],
        "可充电储能装置电压": [hex2dec(i, k=0.1) for i in sourcedataj['可充电储能装置电压']],
        "可充电储能装置电流": [hex2dec(i, k=0.1, n=-1000) for i in sourcedataj['可充电储能装置电流']],
        "单体电池总数": [hex2dec(i) for i in sourcedataj['单体电池总数']],
        "本帧起始电池序号": [hex2dec(i) for i in sourcedataj['本帧起始电池序号']],
        "本帧单体电池总数": [hex2dec(i) for i in sourcedataj['本帧单体电池总数']],
        "单体电池电压": [hex2list(i, num=2, kk=0.001) for i in sourcedataj['单体电池电压']],
    }
    pj2 = {'可充电储能装置电压数据': pj}
    pl = pd.DataFrame([pj]).reindex(columns=list_o)

    next = data[len(sourcedata):]
    nextMark = data[len(sourcedata):len(sourcedata) + 2]

    return {'data_f': next,
            'message_segment_header': nextMark,
            'data_07_02_01': sourcedata,
            "pj2": pj2,
            "oj2": sourcedataj2}
def fun_07_02_08_cf(sourcedata, dj_n_j):
    cf_u = [1, 2, 2, 2, 2, 1, None]
    c = []
    data = sourcedata
    for i in range(dj_n_j):
        cf_u[6] = hex2dec(data[20:22]) * 2
        c = c + cf_u

        cf_a = hexlist2(c)
        data = data[cf_a[-1]:]

    c = [1] + c

    return c
def fun_07_02_08_oj(sourcedata, cf, dj_n_o, dj_n_j):
    data = sourcedata[2:]
    cf_a = hexlist2(cf[1:])
    dict_oj = {
        "可充电储能子系统个数": dj_n_o,
    }

    list_o = [
        "可充电储能子系统号",
        "可充电储能装置电压",
        "可充电储能装置电流",
        "单体电池总数",
        "本帧起始电池序号",
        "本帧单体电池总数",
        "单体电池电压",
    ]

    dict_oj_u = {
        "可充电储能子系统号": [],
        "可充电储能装置电压": [],
        "可充电储能装置电流": [],
        "单体电池总数": [],
        "本帧起始电池序号": [],
        "本帧单体电池总数": [],
        "单体电池电压": [],
    }

    for i in range(dj_n_j):
        for j in range(len(dict_oj_u)):
            data_unit = data[cf_a[i * len(dict_oj_u) + j]:cf_a[i * len(dict_oj_u) + j + 1]]
            dict_oj_u[list_o[j]].append(data_unit)

    dict_all = dict(dict_oj, **dict_oj_u)
    return dict_all


# 09电池电流
def fun_09(data):
    data = data[2:]
    sourcedata = data
    dj_n_o = data[0:2]
    dj_n_j = hex2dec(dj_n_o)  # 电池个数

    cf_u = [1, 1]
    cf = fun_07_02_09_cf(sourcedata, dj_n_j)
    cf_a = hexlist2(cf)

    sourcedata = data[0:cf_a[-1]]

    list_o = [
        "可充电储能子系统个数",
        "可充电储能子系统号",
        "可充电储能温度探针个数",
        "可充电储能子系统各温度探针检测到的温度值",
    ]

    sourcedataj = fun_07_02_09_oj(sourcedata, cf, dj_n_o, dj_n_j)
    sourcedataj2 = {'可充电储能装置温度数据': sourcedataj}
    sourcedatal = pd.DataFrame([sourcedataj]).reindex(columns=list_o)
    pj = {
        "可充电储能子系统个数": dj_n_j,
        "可充电储能子系统号": [hex2dec(i) for i in sourcedataj['可充电储能子系统号']],
        "可充电储能温度探针个数": [hex2dec(i) for i in sourcedataj['可充电储能温度探针个数']],
        "可充电储能子系统各温度探针检测到的温度值": [hex2list(i, num=1, kn=-40) for i in sourcedataj['可充电储能子系统各温度探针检测到的温度值']],
    }
    pj2 = {'可充电储能装置温度数据': pj}
    pl = pd.DataFrame([pj]).reindex(columns=list_o)

    next = data[len(sourcedata):]
    nextMark = data[len(sourcedata):len(sourcedata) + 2]

    return {'data_f': next,
            'message_segment_header': nextMark,
            'data_07_02_01': sourcedata,
            "pj2": pj2,
            "oj2": sourcedataj2}
def fun_07_02_09_cf(sourcedata, dj_n_j):
    cf_u = [1, 2, None]
    c = []
    data = sourcedata
    for i in range(dj_n_j):
        n_str = data[4:8]
        n = hex2dec(n_str)
        cf_u[2] = n
        c = c + cf_u

        cf_a = hexlist2(c)
        data = data[cf_a[-1]:]

    c = [1] + c

    return c
def fun_07_02_09_oj(sourcedata, cf, dj_n_o, dj_n_j):
    data = sourcedata[2:]
    cf_a = hexlist2(cf[1:])
    dict_oj = {
        "可充电储能子系统个数": dj_n_o,
    }

    list_o = [
        "可充电储能子系统号",
        "可充电储能温度探针个数",
        "可充电储能子系统各温度探针检测到的温度值",
    ]

    dict_oj_u = {
        "可充电储能子系统号": [],
        "可充电储能温度探针个数": [],
        "可充电储能子系统各温度探针检测到的温度值": [],
    }

    for i in range(dj_n_j):
        for j in range(len(dict_oj_u)):
            data_unit = data[cf_a[i * len(dict_oj_u) + j]:cf_a[i * len(dict_oj_u) + j + 1]]
            dict_oj_u[list_o[j]].append(data_unit)

    dict_all = dict(dict_oj, **dict_oj_u)
    return dict_all
