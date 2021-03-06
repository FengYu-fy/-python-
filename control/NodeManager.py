#coding=utf-8  
import time  
from multiprocessing import Queue, Process  
from multiprocessing.managers import BaseManager  
  
from DataOutput import DataOutput  
from UrlManager import UrlManager  
  
class NodeManager(object):  
    # 创建一个分布式管理器  
    def start_Manager(self, url_q, result_q):  
        # 把创建的两个队列注册在网络上，利用register方法，callable参数关联了Queue对象，将Queue对象在网络中暴露  
        BaseManager.register('get_task_queue', callable=lambda:url_q)  
        BaseManager.register('get_result_queue', callable=lambda:result_q)  
        # 绑定端口8001，设置验证口令，相当于对象初始化  
        manager = BaseManager(address=('', 8001), authkey='test')  
        # 返回manager对象  
        return manager  
  
    def url_manager_proc(self, url_q, conn_q, root_url):  
        url_manager = UrlManager()  
        url_manager.add_new_url(root_url)  
        while True:  
            while(url_manager.has_new_url()):  
                # 从URL管理器获取新的url  
                new_url = url_manager.get_new_url()  
                # 将新的URL发给工作节点  
                url_q.put(new_url)  
                print "[*]The number of crawled url is: ", url_manager.old_url_size()  

                # 加一个判断条件，当爬去2000个链接后就关闭,并保存进度  
                if(url_manager.old_url_size()>500):  
                    # 通知爬行节点工作结束，添加标识符end  
                    url_q.put('end')  
                    print u"\n[*]控制节点通知爬行结点结束工作..."  
                    # 关闭管理节点，同时存储set状态  
                    url_manager.save_progress('new_urls.txt', url_manager.new_urls)  
                    url_manager.save_progress('old_urls.txt', url_manager.old_urls)  
                    return  
            # 将从result_solve_proc获取到的urls添加到URL管理器  
            try:  
                if not conn_q.empty():  
                    urls = conn_q.get()  
                    url_manager.add_new_urls(urls)  
            except BaseException, e:  
                # 延时休息  
                time.sleep(5)  
  
    def result_solve_proc(self, result_q, conn_q, store_q):  
        while True:  
            try:  
                if not result_q.empty():  
                    content = result_q.get(True)  
                    if content['new_urls'] == 'end':  
                        # 结果分析进程接受通知然后结束  
                        print u"[*]关闭数据提取进程"  
                        store_q.put('end')  
                        return  
                    # url为set类型  
                    conn_q.put(content['new_urls'])  
                    # 解析出来的数据为dict类型  
                    store_q.put(content['data'])  
                else:  
                    # 延时休息  
                    time.sleep(5)  
            except BaseException, e:  
                # 延时休息  
                time.sleep(5)  
  
    def store_proc(self, store_q):  
        output = DataOutput()  
        while True:  
            if not store_q.empty():  
                data = store_q.get()  
                if data == 'end':  
                    print u"[*]关闭数据存储进程"  
                    output.output_end(output.filepath)  
                    return  
                output.store_data(data)  
            else:  
                time.sleep(5)  
  
if __name__ == '__main__':  

        # 初始化4个队列  
        url_q = Queue()  
        result_q = Queue()  
        conn_q = Queue()  
        store_q = Queue()  
        # 创建分布式管理器  
        node = NodeManager()  
        manager = node.start_Manager(url_q, result_q)  
        # 创建URL管理进程、 数据提取进程和数据存储进程  
        url_manager_proc = Process(target=node.url_manager_proc, args=(url_q, conn_q, 'https://baike.baidu.com/view/284853.htm'))  
        result_solve_proc = Process(target=node.result_solve_proc, args=(result_q, conn_q, store_q))  
        store_proc = Process(target=node.store_proc, args=(store_q, ))  
        # 启动3个进程和分布式管理器  
        url_manager_proc.start()  
        result_solve_proc.start()  
        store_proc.start()  
        manager.get_server().serve_forever()  
