# opc-server_device_hive
![image](https://github.com/CodePythonFollow/opc-server_device_hive/blob/master/关系.png)
那么这时候我用到了python的多线程。我们看一下需要的线程，以及个线程需要做的事
![image](https://github.com/CodePythonFollow/opc-server_device_hive/blob/master/线程.png)
因为这是**解耦合**的思想， 把功能分开。分成设备和平台两个对象。
这是非常典型的 **生产者(生产数据) 消费者(执行后删除数据)** 模型。
![image](https://github.com/CodePythonFollow/opc-server_device_hive/blob/master/数据.png)
