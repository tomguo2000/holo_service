# 载入的工具包
import numpy as np
import pandas as pd
import binascii
import datetime
import math
import re

# 公共函数
# 8进制转10进制
def oct2dec(x):
    output = int(str(x), 8)
    return output

# 16进制转10进制（先乘精度，后偏移）
def hex2dec(x, n=0, k=1, e=False):
    x = x.upper()
    k_n = 0 if k>=1 else int(math.log10(k))
    if e:
        if x == "EF" or x == "FFFE" or x == "FFFFFFFE":
            return "异常"
        elif x == "FF" or x == "FFFF" or x == "FFFFFFFF":
            return "无效"
        else:
            output_s = int(str(x), 16)
            output = output_s*k + n
            return round(output, -k_n)
    else :
        output_s = int(str(x), 16)
        output = output_s*k + n
        return round(output, -k_n)

    # 16进制转字符串
def hex2str(x):
    output = binascii.a2b_hex(x).decode("utf8")
    return output

# 时间
def get_datetime(x):
    year = hex2dec(x[0:2]) + 2000
    month = hex2dec(x[2:4])
    day = hex2dec(x[4:6])
    hour = hex2dec(x[6:8])
    minute = hex2dec(x[8:10])
    second = hex2dec(x[10:12])
    output = datetime.datetime(year, month, day, hour, minute, second).strftime('%Y-%m-%d %H:%M:%S')
    return output

# 构建
def list2dict(data, list_o, cf_a, mode='s'):
    dict_o = {}
    if mode == 's':
        for i in range(len(cf_a)-1):
            dict_o[list_o[i]] = data[cf_a[i]:cf_a[i+1]]
        return dict_o
    elif mode == 'l':
        for i in range(len(cf_a)-1):
            dict_o[list_o[i]] = [data[cf_a[i]:cf_a[i+1]]]
        return dict_o

# 累加
def hexlist2(data, n=1):
    output = [0]
    data = data * n
    for i in range(0, len(data)):
        output.append(output[i] + data[i])
    output = [i*2 for i in output]
    return output

# 列表
def hex2list(data, num=1, kn=0, kk=1, ke=False):
    num = num * 2
    n = '.{'+str(num)+'}'
    output = re.findall(n, data)
    output = [hex2dec(i, n=kn ,k=kk ,e=ke) for i in output]
    return output

# 报文解析
# 解析替换函数
def dict_list_replace(n, x):
    x = x.upper()
    try:
        index = jx_dict_o[n].index(x)
        output = jx_dict[n][index]
    except:
        output = "ERROR"
    return output

# 解析列表
jx_dict_o = {
    "02":["01", "02", "03", "04", "05", "06"],
    "03":["01", "02", "03", "FE"],
    "05":["01", "02", "03", "FE", "FF"],
    "07_02_01_01":["01", "02", "03", "FE", "FF"],
    "07_02_01_02":["01", "02", "03", "04", "FE", "FF"],
    "07_02_01_03":["01", "02", "03", "FE", "FF"],
    "07_02_01_06":["01", "02", "FE", "FF"],
    "07_02_02_02":["01", "02", "03", "04", "FE", "FF"],
    "07_02_03_12":["01", "02", "FE", "FF"],
    "07_02_04_01":["01", "02", "FE", "FF"],
    "07_05_05":["01", "02", "03", "FE", "FF"],
}

jx_dict = {
    "02":["车辆登入", "实时信息上报", "补发信息上报", "车辆登出", "平台登入", "平台登出"],
    "03":["成功", "错误", "VIN重复", "命令"],
    "05":["不加密", "RSA加密", "AES128位加密", "异常", "无效"],
    "07_02_01_01":["启动", "熄火", "其他", "异常", "无效"],
    "07_02_01_02":["停车充电", "行驶充电", "未充电", "充电完成", "异常", "无效"],
    "07_02_01_03":["纯电", "混动", "燃油", "异常", "无效"],
    "07_02_01_06":["工作", "断开", "异常", "无效"],
    "07_02_02_02":["耗电", "发电", "关闭", "准备", "异常", "无效"],
    "07_02_03_12":["工作", "断开", "异常", "无效"],
    "07_02_04_01":["启动", "关闭", "异常", "无效"],
    "07_05_05":["不加密", "RSA加密", "AES128位加密", "异常", "无效"],
}