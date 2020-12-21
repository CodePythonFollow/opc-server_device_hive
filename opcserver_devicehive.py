import datetime
import json
import os
import threading
import time
from enum import Enum
from devicehive import Handler, DeviceHiveApi, DeviceHive
from opcua import Client, ua, Subscription
from opcua.client.ua_client import UASocketClient, UaClient
from opcua.ua.ua_binary import struct_to_binary
from concurrent.futures._base import Future
from opcua.tools import SubHandler
from opcua.ua.uaerrors import BadTypeMismatch


# 在子线程添加异常捕获事件
class MyUaSocketClient(UASocketClient):

    # 如果配置文件更新希望重新启动程序   重写UASocketClient方法  这里增加了异常处理
    def _send_request(self, request, callback=None, timeout=1000, message_type=ua.MessageType.SecureMessage):
        global connect_status
        # print("send", connect_status)
        """
        send request to server, lower-level method
        timeout is the timeout written in ua header
        returns future
        """
        # 当配置文件改变通知子线程结束
        if config_status != Status.CONFIG_UPDATE:
            with self._lock:
                try:
                    request.RequestHeader = self._create_request_header(timeout)
                    # self.logger.debug("Sending: %s", request)
                    try:
                        binreq = struct_to_binary(request)
                    except Exception:
                        # reset reqeust handle if any error
                        # see self._create_request_header
                        self._request_handle -= 1
                        raise
                    self._request_id += 1
                    future = Future()
                    if callback:
                        future.add_done_callback(callback)
                    self._callbackmap[self._request_id] = future

                    # Change to the new security token if the connection has been renewed.
                    if self._connection.next_security_token.TokenId != 0:
                        self._connection.revolve_tokens()

                    msg = self._connection.message_to_binary(binreq, message_type=message_type,
                                                             request_id=self._request_id)
                    self._socket.write(msg)
                except Exception as e:
                    connect_status = Status.FAILED
                    print(e)
            return future
        else:
            try:
                raise ConfigFileUpdate("配置文件更新", "400")
            except ConfigFileUpdate as e:
                print(e)
                print("希望重启订阅线程")


# 处理让ua连接继承自我的方法
class MyUaClient(UaClient):
    def connect_socket(self, host, port):
        """
        connect to server socket and start receiving thread
        """
        self._uasocket = MyUaSocketClient(self._timeout, security_policy=self.security_policy)
        return self._uasocket.connect_socket(host, port)


# 重写客户端的 ua client
class MyClient(Client):
    def __init__(self, url):
        super().__init__(url)
        self.uaclient = MyUaClient(timeout=4)

    def create_subscription(self, period, handler):
        """
        Create a subscription.
        returns a Subscription object which allow
        to subscribe to events or data on server
        handler argument is a class with data_change and/or event methods.
        period argument is either a publishing interval in milliseconds or a
        CreateSubscriptionParameters instance. The second option should be used,
        if the opcua-server has problems with the default options.
        These methods will be called when notfication from server are received.
        See example-client.py.
        Do not do expensive/slow or network operation from these methods
        since they are called directly from receiving thread. This is a design choice,
        start another thread if you need to do such a thing.
        """

        if isinstance(period, ua.CreateSubscriptionParameters):
            return Subscription(self.uaclient, period, handler)
        params = ua.CreateSubscriptionParameters()
        params.RequestedPublishingInterval = period
        params.RequestedLifetimeCount = 10000
        params.RequestedMaxKeepAliveCount = 3000
        params.MaxNotificationsPerPublish = 10000
        params.PublishingEnabled = True
        params.Priority = 0
        return MySubscription(self.uaclient, params, handler)


# 重写回调方法将数据存储到序列
class MySubscription(Subscription):

    # 订阅节点之发生变化回调  这里需要传递出要发送到平台的数据          重写Subscription类方法
    def _call_datachange(self, datachange):
        global connect_status
        global config_status
        print(config_status)

        for item in datachange.MonitoredItems:
            with self._lock:
                if item.ClientHandle not in self._monitoreditems_map:
                    # self.logger.warning("Received a notification for unknown handle: %s", item.ClientHandle)
                    self.has_unknown_handlers = True
                    continue
                data = self._monitoreditems_map[item.ClientHandle]
                node = data.node
                node_name = node_names[str(node)]
                value = item.Value.Value.Value
                data_type = item.Value.Value.VariantType.name
                # status_code = item.Value.StatusCode
                source_times_tamp = item.Value.SourceTimestamp
                msg = {
                    "notification": node_name,
                    "parameters": {
                        "value": str(value),
                        "dataType": str(data_type),
                        "nodeId": str(node),
                        "timestamp": str(source_times_tamp)
                    }
                }
                # 1、维护一个消息队列，这里发送一个消息让devicehive进行处理
                need_send_to_device_hive.append(msg)


# 自定义异常让程序结束
class ConfigFileUpdate(Exception):
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status


# 连接状态
class Status(Enum):
    WAIT_CONNECT = 0
    SUCCEED = 1
    FAILED = 2
    ERROR = 3

    CONFIG_UPDATE = 4
    CONFIG_NOT_CHANGE = 5


# opc数据获取
class OPCScript(UASocketClient):
    # 初始化客户端
    def __init__(self, find_nodes=None):
        super().__init__(timeout=1, security_policy=ua.SecurityPolicy())
        if find_nodes is None:
            find_nodes = ['']
        self.ip = ''
        self.port = ''
        self.client = ''

        self.find_nodes = find_nodes
        self.node_names = ''

    # 读取配置文件
    def read_config(self):
        dirs = os.listdir('./')
        if 'config.json' in dirs:
            with open('config.json') as fi:
                config = json.loads(fi.read())
            if config.get("ip", '') and config.get("port", ""):
                if config.get("nodes", ''):
                    self.ip = config["ip"]
                    self.port = config["port"]
                    # 首先将原来的置空
                    self.find_nodes = []
                    for _, value in config["nodes"].items():
                        self.find_nodes.append(value)

    # 检测配置文件信息并连接到opc服务
    def handle_connect(self):
        # 第一次读取配置文件      配置文件改变读取配置文件
        if config_status == Status.CONFIG_UPDATE or self.ip == '':
            self.read_config()
            print("读取配置文件")
        global connect_status
        try:
            self.client = MyClient(f"opc.tcp://{self.ip}:{self.port}/")
            self.client.connect()
            connect_status = Status.SUCCEED
            return True
        except Exception as e:
            print("连接")
            print(e)
            connect_status = Status.FAILED
            return False

    # 检测配置文件是否更新
    def config_change(self):

        global config_status
        config_status = Status.CONFIG_UPDATE
        dirs = os.listdir('./')
        if 'config.json' in dirs:
            with open('config.json') as fi:
                config = json.loads(fi.read())
            if config.get("ip", '') == self.ip and config.get("port", "") == self.port:
                if config.get("nodes"):
                    new_to_find_nodes = []
                    for _, value in config["nodes"].items():
                        new_to_find_nodes.append(value)
                    if new_to_find_nodes == self.find_nodes:
                        config_status = Status.CONFIG_NOT_CHANGE
                        print("配置文件未更新")
        threading.Timer(5, self.config_change).start()

    # 订阅节点
    def subscribe_nodes(self):
        print("订阅")
        global connect_status
        global node_names
        if self.handle_connect():
            try:
                nodes = [self.client.get_node(find_node) for find_node in self.find_nodes]
                node_names = {
                    find_node: self.client.get_node(find_node).get_browse_name().Name for find_node in self.find_nodes
                }
                handler = SubHandler()
                sub = self.client.create_subscription(500, handler)
                sub.subscribe_data_change(nodes)
            except Exception as e:
                print(e)
                connect_status = Status.FAILED
        else:
            connect_status = Status.FAILED

    # 当收到 device hive 平台传到的命令调用此方法   传入command.command
    def read_device_hive_msg(self):
        while need_achieve_commands:
            command = need_achieve_commands.pop()
            print(command.parameters)
            if self.handle_connect():
                command_dic = command.parameters
                data = command_dic["data"]
                error = []
                for node in data:
                    nodeId = node["nodeId"]
                    value = node["value"]
                    dataType = node["dataType"]
                    node = self.client.get_node(nodeId)
                    if node.get_value_rank() < 0:
                        try:
                            if dataType == "int":
                                value = eval(value)
                                node.set_value(value, node.get_data_type_as_variant_type())
                            elif dataType == "float":
                                try:
                                    d_value = ua.DataValue(ua.Variant(eval(value), ua.VariantType.Double))
                                    node.set_value(d_value)
                                except BadTypeMismatch:
                                    # print(e)
                                    value = ua.DataValue(ua.Variant(float(value), ua.VariantType.Float))
                                    node.set_value(value)
                            elif dataType == "string":
                                value = ua.DataValue(ua.Variant(value, ua.VariantType.String))
                                node.set_value(value)
                            elif dataType == "datetime":
                                value = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
                                value = ua.DataValue(ua.Variant(value, ua.VariantType.DateTime))
                                node.set_value(value)
                        except Exception as e:
                            print("错误")
                            error.append(e)
                    else:
                        error.append("可能由于数据为数组类型引起的错误")
                self.client.disconnect()
                if not error:
                    result = 'succeed'
                else:
                    error_msg = ''
                    for e in error:
                        error_msg += 'error :' + str(e) + '\n'
                    result = error_msg
            else:
                result = "OPC服务关闭无法执行命令"
            command.result = {"result": result}
            command.save()
        threading.Timer(1, self.read_device_hive_msg).start()

    # 每隔1秒发送一次连接状态   线程
    def exchange_connect_status(self):
        source_times_tamp = datetime.datetime.utcnow().isoformat()
        try:
            if self.handle_connect():
                self.client.disconnect()
                submit_status = 1
            else:
                submit_status = 0
        except Exception as e:
            submit_status = 0
            print(e)
        # print(self.handle_connect())
        msg = {
            "notification": 'HeatBeat',
            "parameters": {
                "status": submit_status,
                "timestamp": source_times_tamp,
                "connectType": "opc ua"
            }
        }
        need_send_to_device_hive.append(msg)
        threading.Timer(1, self.exchange_connect_status).start()


# device hive 监听平台传递过来的命令
class ReceiverHandler(Handler):

    def __init__(self, api, accept_command_name='update'):
        Handler.__init__(self, api)
        self._device_id = device_id
        self._accept_command_name = accept_command_name
        self.device = None

    # 订阅含有update的语句
    def handle_connect(self):
        self.device = self.api.put_device(self._device_id)

        self.device.subscribe_insert_commands([self._accept_command_name])
        # self._device.subscribe_notifications()

    # 需要将收到的command 交给opc处理并且 得到结果返回给status
    def handle_command_insert(self, command):
        need_achieve_commands.append(command)


# device hive 平台对象
class Device:
    def __init__(self, rest_url='http://10.159.44.180/api/rest',
                 login='ctqUser', password='program111'):
        self.rest_url = rest_url
        self.login = login
        self.password = password
        self.device_hive_api = DeviceHiveApi(self.rest_url, login=self.login, password=self.password)

    # 发送消息   一直监控消息队列
    def send_notification(self):
        while need_send_to_device_hive:
            msg = need_send_to_device_hive.pop()
            try:
                result = self.device_hive_api.send_notification(device_id, msg['notification'], msg['parameters'])
            except Exception:
                need_send_to_device_hive.append(msg)
                print("即将重新登陆")
                self.device_hive_api = DeviceHiveApi(self.rest_url, login=self.login, password=self.password)
        threading.Timer(1, self.send_notification).start()

    # 监控命令线程
    def subscribe_command(self):
        dh = DeviceHive(ReceiverHandler)
        dh.connect(self.rest_url, login=self.login, password=self.password)


# 主线程激活opc服务
def eternal():
    global connect_status
    opcserver = OPCScript()
    device = Device()
    # 连接服务订阅节点
    try:
        # 开启订阅线程  默认
        opcserver.subscribe_nodes()
        time.sleep(3)
        # 开启监控连接状态  1s
        opcserver.exchange_connect_status()
        # 开启平台发送线程  1s
        device.send_notification()
        # 开启平台订阅线程  默认
        threading.Thread(target=device.subscribe_command).start()
        # 开启处理命令线程  1s
        opcserver.read_device_hive_msg()
        # 每5秒检测一次配置文件的状态
        opcserver.config_change()
    except Exception as e:
        print(e)
        # send error msg


if __name__ == '__main__':
    device_id = 'r3GKj5VBtVE3WqdAxqtDzbe39pSIMBpNerAO'
    # 主线程监控connect status，控制程序运行   同时有一个线程负责上传连接状态
    connect_status = Status.WAIT_CONNECT
    # 默认配置文件状态为未改变
    config_status = Status.CONFIG_NOT_CHANGE
    # 需要徐上传到平台的 msg opc订阅线程负责追加，device_hive上传线程负责上传
    need_send_to_device_hive = []
    # 需要执行的命令 ， device订阅线程负责追加， opc read线程负责上传
    need_achieve_commands = []

    # 初始化，激活所有线程
    eternal()

    # 主线程监控连接状态
    while True:
        time.sleep(5)
        # 第一种状态启动程序
        if connect_status == Status.WAIT_CONNECT:
            print("等待连接中")
            # 重启订阅线程
            opc = OPCScript()
            opc.subscribe_nodes()
        elif connect_status == Status.SUCCEED:
            print("连接正常")
        elif connect_status == Status.FAILED:
            print("连接失败")
            # 重启订阅线程
            opc = OPCScript()
            opc.subscribe_nodes()
        elif connect_status == Status.ERROR:
            print("连接错误")
            # 重启订阅线程
            opc = OPCScript()
            opc.subscribe_nodes()
        if config_status == Status.CONFIG_UPDATE:
            time.sleep(3)
            # 重启订阅线程
            print("重启订阅")
            opc = OPCScript()
            opc.subscribe_nodes()
            config_status = Status.CONFIG_NOT_CHANGE
