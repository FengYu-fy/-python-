#coding=utf-8
import Queue  
from multiprocessing.managers import BaseManager  
from multiprocessing import freeze_support  
  
#任务个数  
task_number = 10  
  
# 第一步，建立task_queue和result_queue来存放任务和结果  
task_queue = Queue.Queue(task_number)  
result_queue = Queue.Queue(task_number)  
  
def get_task():  
    return task_queue  
  
def get_result():  
    return result_queue  
class QueueManager(BaseManager):
    pass
  
def win_run():  
    # 第二步，把创建的两个队列注册在网络上，利用register方法，callable参数关联了Queue对象，将Queue对象在网络中暴露  
    # Windows下绑定调用接口不能使用lambda，所以只能先定义函数再绑定  
    BaseManager.register('get_task_queue', callable=get_task)  
    BaseManager.register('get_result_queue', callable=get_result)  
  
    # 第三步，绑定端口8001，设置验证口令，相当于对象初始化，Windows下需要填写IP地址  
    manager = BaseManager(address=('127.0.0.1', 8001), authkey='test')  
  
    # 第四步，启动管理，监听信息通道  
    manager.start()  
  
    try:  
        # 第五步，通过管理实例的方法获得通过网络访问的Queue对象  
        task = manager.get_task_queue()  
        result = manager.get_result_queue()  
  
        # 第六步，添加任务  
        for url in ["ImageUrl_" + str(i) for i in range(10)]:  
            print "put task %s ..." % url  
            task.put(url)  
  
        # 获取返回结果  
        print "try get result..."  
        for i in range(10):  
            print "result is %s" % result.get(timeout=10)  
    except:  
        print "Manager error"  
    finally:  
        # 一定要关闭管理，否则会报管道未关闭错误  
        manager.shutdown()  
  
if __name__ == '__main__':  
    # Windows下多进程可能会有问题，添加这句可以缓解  
    freeze_support()  
    win_run()  