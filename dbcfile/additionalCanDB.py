class additionalCanDB(object):
    additionalCanID = {
        'misc': {'source': 'message_misc.txt', 'cycle_time': 1000}
         }

    additionalSignal = [
        {'misc':
            [
                {'modemMode': {'comment': 'modem网络制式',
                               'choices': {0: '无网络', 1: '4G', 2: '3G', 3: '2G'},
                               'maximum': 3,
                               'minimum': 0}},
                {'modemSignal': {'comment': 'modem信号强度',
                                 'choices': {},
                                 'maximum': 31,
                                 'minimum': 0}},
                {'locationStatus': {'comment': '定位状态',
                                    'choices': {0: '有效定位', 1: '无效定位'},
                                    'maximum': 1,
                                    'minimum': 0}},
                {'longitudeEW': {'comment': '东经西经',
                                 'choices': {0: '东经', 1: '西经'},
                                 'maximum': 1,
                                 'minimum': 0}},
                {'longitude': {'comment': '经度值',
                               'choices': {},
                               'maximum': 179.999999,
                               'minimum': 0.}},
                {'latitudeNS': {'comment': '北纬南纬',
                                'choices': {0: '北纬', 1: '南纬'},
                                'maximum': 1,
                                'minimum': 0}},
                {'latitude': {'comment': '纬度值',
                              'choices': {},
                              'maximum': 89.999999,
                              'minimum': 0.}},
                {'UTC': {'comment': 'UTC时间',
                         'choices': {},
                         'maximum': None,
                         'minimum': None}},
                {'UTCms': {'comment': 'UTC毫秒',
                           'choices': {},
                           'maximum': 999,
                           'minimum': 0}},
                {'groundSpeed': {'comment': '地面速度',
                                 'choices': {},
                                 'maximum': 999.9,
                                 'minimum': 0.}},
                {'speedDirection': {'comment': '速度方向',
                                    'choices': {},
                                    'maximum': 359,
                                    'minimum': 0}}
            ]
         }
    ]

    def __init__(self):
        pass

    @classmethod
    def get_signal(cls, signalName):
        for can in cls.additionalSignal:
            for k,v in can.items():
                for signal in v:
                    for key, value in signal.items():
                        if key == signalName:
                            # return value
                            return additionalSignalInfo(value['comment'], value['choices'], value['maximum'], value['minimum'])

        return None

    @classmethod
    def get_cycle_time(cls, signalName):
        _canID = cls.get_canID_from_signalName(signalName=signalName)
        if _canID:
            return cls.additionalCanID[_canID]['cycle_time']
        else:
            return None

    @classmethod
    def get_canID_from_signalName(cls, signalName):
        for can in cls.additionalSignal:
            for k,v in can.items():         # misc级别
                for signal in v:
                    for key, value in signal.items():
                        if key == signalName:
                            return k

    @classmethod
    def get_message_signals(cls):
        resp = {}
        for can in cls.additionalSignal:
            for k, v in can.items():
                _respValue = set()
                for signal in v:
                    for key, value in signal.items():
                        _respValue.add(key)
                resp[k] = _respValue
        return resp





class additionalSignalInfo(object):
    def __init__(self, comment, choices, maximum, minimum):
        self.comment = comment
        self.choices = choices
        self.maximum = maximum
        self.minimum = minimum