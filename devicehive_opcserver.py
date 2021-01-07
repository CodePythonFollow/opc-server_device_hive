import datetime
import json
import os
import queue
import threading
import time
from enum import Enum
from json import JSONDecodeError
import requests
from opcua import Client, ua
from opcua.client.ua_client import UASocketClient
from opcua.ua.uaerrors import BadTypeMismatch


# 连接状态
class Status(Enum):
    WAIT_CONNECT = 0
    SUCCEED = 1
    FAILED = 2
    NOT_FIND = 3
    CONFIG_UPDATE = 4
    CONFIG_NOT_CHANGE = 5


# OPC 服务对象
class OPCScript(UASocketClient):
    class SubHandler:
        @staticmethod
        def datachange_notification(node, val, data):
            node_name = OPCScript.node_names[str(node)]
            data_type = data.monitored_item.Value.Value.VariantType.name
            source_times_tamp = data.monitored_item.Value.ServerTimestamp

            msg = {
                "notification": node_name,
                "parameters": {
                    "value": str(val),
                    "dataType": str(data_type),
                    "nodeId": str(node),
                    "timestamp": str(source_times_tamp)
                }
            }
            # print(msg)
            # 1、维护一个消息队列，这里发送一个消息让devicehive进行处理
            need_send_to_device_hive.put(msg)

    node_names = dict()
    sub = ''
    connect_status = Status.WAIT_CONNECT
    config_status = Status.CONFIG_NOT_CHANGE
    ip = ''
    port = ''
    client = ''
    handler = SubHandler()
    find_nodes = ''

    # 初始化客户端
    def __init__(self):
        super().__init__(timeout=1, security_policy=ua.SecurityPolicy())

    # 读取配置文件
    @staticmethod
    def read_config():
        dirs = os.listdir('./')
        if 'config.json' in dirs:
            try:
                with open('config.json') as fi:
                    config = json.loads(fi.read())
                if config.get("ip", '') and config.get("port", ""):
                    if config.get("nodes", ''):
                        OPCScript.ip = config["ip"]
                        OPCScript.port = config["port"]
                        # 首先将原来的置空
                        OPCScript.find_nodes = []
                        for _, value in config["nodes"].items():
                            OPCScript.find_nodes.append(value)
            except Exception as e:
                msg = {
                    "notification": "error",
                    "parameters": {
                        "msg": str(e) + "读取配置文件错误"
                    }
                }
                need_send_to_device_hive.put(msg)

    # 读取配置文件信息并连接到opc服务
    def handle_connect(self):
        self.read_config()
        try:
            OPCScript.client = Client(f"opc.tcp://{self.ip}:{self.port}/")
            OPCScript.client.connect()
            OPCScript.connect_status = Status.SUCCEED
            return True
        except Exception as e:
            msg = {
                "notification": "error",
                "parameters": {
                    "msg": str(e) + "连接opc出错"
                }
            }
            need_send_to_device_hive.put(msg)
            OPCScript.connect_status = Status.FAILED
            return False

    # 检测配置文件是否更新
    def config_change(self):
        dirs = os.listdir('./')
        if 'config.json' in dirs:
            with open('config.json') as fi:
                config = json.loads(fi.read())
            if config.get("ip", '') == OPCScript.ip and config.get("port", "") == OPCScript.port:
                if config.get("nodes"):
                    new_to_find_nodes = []
                    for _, value in config["nodes"].items():
                        new_to_find_nodes.append(value)
                    if new_to_find_nodes == OPCScript.find_nodes:
                        pass
                    else:
                        OPCScript.config_status = Status.CONFIG_UPDATE
                else:
                    OPCScript.config_status = Status.CONFIG_UPDATE
            else:
                OPCScript.config_status = Status.CONFIG_UPDATE
        else:
            OPCScript.config_status = Status.NOT_FIND
        threading.Timer(3, self.config_change).start()

    # 订阅节点
    def subscribe_nodes(self):
        if self.handle_connect():
            try:
                nodes = [OPCScript.client.get_node(find_node) for find_node in OPCScript.find_nodes]
                OPCScript.node_names = {
                    find_node: OPCScript.client.get_node(find_node).get_browse_name().Name for find_node in
                    self.find_nodes
                }
                OPCScript.sub = OPCScript.client.create_subscription(500, OPCScript.handler)
                OPCScript.sub.subscribe_data_change(nodes)
            except Exception as e:
                msg = {
                    "notification": "error",
                    "parameters": {
                        "msg": str(e)
                    }
                }
                need_send_to_device_hive.put(msg)
                OPCScript.connect_status = Status.FAILED
        else:
            OPCScript.connect_status = Status.FAILED

    # 取消订阅
    def unsubscribe_nodes(self):
        try:
            OPCScript.sub.unsubscribe(self.handler)
            OPCScript.client.reconciliate_subscription(self.handler)
            OPCScript.client.disconnect()
        except Exception as e:
            if 'str' in str(e):
                print("还未订阅节点")

    # 当收到 device hive 平台传到的命令调用此方法   传入command.command
    def read_device_hive_msg(self):
        while not need_achieve_commands.empty():
            command = need_achieve_commands.get()
            command_dic = command['parameters']
            try:
                data = command_dic["data"]
                # print(data)
                error = []
                for node in data:
                    node_id = node["nodeId"]
                    value = node["value"]
                    data_type = node["dataType"]
                    node = OPCScript.client.get_node(node_id)
                    if node.get_value_rank() < 0:
                        try:
                            if data_type == "int":
                                value = eval(value)
                                node.set_value(value, node.get_data_type_as_variant_type())
                            elif data_type == "float":
                                try:
                                    d_value = ua.DataValue(ua.Variant(eval(value), ua.VariantType.Double))
                                    node.set_value(d_value)
                                except BadTypeMismatch:
                                    # print(e)
                                    value = ua.DataValue(ua.Variant(float(value), ua.VariantType.Float))
                                    node.set_value(value)
                            elif data_type == "string":
                                value = ua.DataValue(ua.Variant(value, ua.VariantType.String))
                                node.set_value(value)
                            elif data_type == "datetime":
                                value = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
                                value = ua.DataValue(ua.Variant(value, ua.VariantType.DateTime))
                                node.set_value(value)
                        except Exception as e:
                            print("错误")
                            error.append(e)
                    else:
                        error.append("可能由于数据为数组类型引起的错误")
                # OPCScript.client.disconnect()
                if not error:
                    result = {'result': 'Succeed'}
                    status = 200
                else:
                    error_msg = ''
                    for e in error:
                        error_msg += 'error :' + str(e) + '\n'
                    result = {"error": error}
                    status = 300
                command['result'] = result
                command['status'] = status
            except Exception as e:
                command['result'] = {"error": str(e)}
                command['status'] = 300
            need_update_commands.put(command)
        threading.Timer(5, self.read_device_hive_msg).start()

    # 每隔5秒发送一次连接状态   线程
    def exchange_connect_status(self):
        source_times_tamp = datetime.datetime.utcnow().isoformat()
        try:
            # 不用同一个对象防止关闭同一个连接
            client = Client(f"opc.tcp://{self.ip}:{self.port}/")
            client.connect()
            client.disconnect()
            submit_status = 1
        except Exception as e:
            OPCScript.connect_status = Status.FAILED
            submit_status = 0
            print(e)
        msg = {
            "notification": 'HeatBeat',
            "parameters": {
                "status": submit_status,
                "timestamp": source_times_tamp,
                "connectType": "opc ua",
                "processStatus": True if (threading.activeCount() >= 7 and submit_status == 1)
                            or (threading.activeCount() >= 5 and submit_status == 0) else False
            }
        }
        need_send_to_device_hive.put(msg)
        threading.Timer(5, self.exchange_connect_status).start()


# device hive 平台对象
class Device:

    def __init__(self):
        self.rest_url = f'http://{PlatformIp}/api/rest'
        self.token_url = f"http://{PlatformIp}/auth/rest/token"

    # 获取token
    def get_token(self):
        headers = {
            "Content-type": "application/json"
        }
        data = {
            "login": Login,
            "password": Password
        }
        response = requests.post(self.token_url, headers=headers, data=json.dumps(data))
        # response.
        return response.json()['accessToken']

    # 利用post发送请求
    def post_notification(self, msg):
        token = self.get_token()
        url = f"http://{PlatformIp}/api/rest/device/{DeviceId}/notification"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-type": "application/json",
        }

        res = requests.post(url, headers=headers, data=json.dumps(msg))
        return res.json()

    # 发送消息   一直监控消息队列
    def send_notification(self):
        while not need_send_to_device_hive.empty():
            if need_send_to_device_hive.full():
                print("队列满了, 清空队列。")
                need_send_to_device_hive.queue.clear()
                break
            msg = need_send_to_device_hive.get()
            print(msg)
            try:
                self.post_notification(msg)
            # logging.info(msg['notification'], result.timestamp)
            except Exception as e:
                time.sleep(2)
                need_send_to_device_hive.put(msg)
                print("网络故障或者平台信息更改")
                # need_send_to_device_hive.put(error)
        threading.Timer(1, self.send_notification).start()

    # 监控命令线程  并更新命令
    def get_commands(self):
        try:
            token = self.get_token()
            url = f"http://{PlatformIp}/api/rest/device/{DeviceId}/command"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-type": "application/json",
            }
            response = requests.get(url, headers=headers)
            if response.json():
                for command in response.json():
                    if command['status'] is None:
                        # 已接受将status置为 100
                        command['status'] = '100'
                        self.update_commands(command)
                        print('get')
                        need_achieve_commands.put(command)
        except Exception as e:
            time.sleep(5)
            print("网络故障或者平台信息更改 命令获取错误")
        while not need_update_commands.empty():
            command = need_update_commands.get()
            self.update_commands(command)
        threading.Timer(5, self.get_commands).start()

    # 命令执行完成后跟新
    def update_commands(self, command):
        try:
            token = self.get_token()
            url = f"http://{PlatformIp}/api/rest/device/{DeviceId}/command/{command['id']}"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-type": "application/json",
            }
            data = {
                "status": command['status'],
                "result": command['result']
            }

            res = requests.put(url, headers=headers, data=json.dumps(data))
            # 网络问题， 命令过期几乎不可能
            if res.status_code > 300:
                #  发生此概率可能性太小
                try:
                    error = res.json()['error']
                    print(error)
                except JSONDecodeError:
                    print("确定是网络问题")
                    pass
        except Exception as e:
            time.sleep(5)
            print("网络故障或者平台信息更改 命令更新错误，等待网络恢复重新更新")
            need_update_commands.put(command)


# 主线程激活opc服务
def eternal():
    opc_server = OPCScript()
    device = Device()
    # 连接服务订阅节点
    try:
        # 开启订阅线程  默认
        opc_server.subscribe_nodes()
        time.sleep(3)
        # 配置文件更新
        # opc_server.config_change()
        # 开启平台订阅线程  默认
        device.get_commands()
        # 开启处理命令线程  5s
        opc_server.read_device_hive_msg()
        # 开启平台发送线程  1s
        device.send_notification()
        # 开启监控连接状态  1s
        opc_server.exchange_connect_status()
    except Exception as e:
        print(e)

    # 监控连接状态
    while True:
        time.sleep(5)
        print(threading.activeCount())
        # 配置文件更新重启订阅，  在所有发送到ua的请求，如果配置文件更新都会出错，主要是为了结束订阅线程。
        if OPCScript.config_status == Status.CONFIG_UPDATE:
            print("配置文件更新，先取消订阅，再重新订阅节点")
            opc_server.unsubscribe_nodes()
            time.sleep(2)
            opc_server.subscribe_nodes()
            OPCScript.config_status = Status.CONFIG_NOT_CHANGE

        elif OPCScript.connect_status == Status.SUCCEED:
            print("连接正常")

        elif OPCScript.connect_status == Status.FAILED:
            print("连接失败")
            # 重启订阅线程
            opc_server.unsubscribe_nodes()
            time.sleep(2)
            print("重启订阅")
            opc_server.subscribe_nodes()


if __name__ == '__main__':
    # 设备数据
    DeviceId = 'r3GKj5VBtVE3WqdAxqtDzbe39pSIMBpNerAO'
    PlatformIp = '10.159.44.180'
    Login = 'ctqUser'
    Password = 'program111'
    accept_command_name = 'update'

    # 需要徐上传到平台的 msg opc订阅线程负责追加，device_hive上传线程负责上传
    need_send_to_device_hive = queue.Queue(maxsize=100)
    # 需要执行的命令 ， device订阅线程负责追加， opc read线程负责上传
    need_achieve_commands = queue.Queue()
    # 执行完需要更新命令状态
    need_update_commands = queue.Queue()

    # 初始化，激活所有线程
    eternal()
