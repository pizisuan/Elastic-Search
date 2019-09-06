"""
本模块用于ES操作
"""

__author__ = "Pizisuan"

import csv
csv.field_size_limit(500 * 1024 * 1024)

from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

import requests

class HandleEsObj:
    def __init__(self, ip, port):
        '''
        构造函数
        :param ip: ip地址
        :param port: 端口号
        '''
        self.ip = ip
        self.port = port

        # no password sign in
        self.es = Elasticsearch([ip], port=port)

        #sign in by password
        #self.es = Elasticsearch([ip],http_auth=('elastic', 'password'),port=9200)

    def is_ping(self):
        """
        测试集群是否启动
        """
        return self.es.ping()

    def get_info(self):
        """
        获取集群基本信息
        """
        return self.es.info()

    def is_health(self):
        """
        获取集群的健康状态信息
        """
        return self.es.cluster.health()

    def get_nodes(self):
        """
        获取集群节点信息
        """
        return self.es.cluster.client.info()

    def get_indices(self):
        """
        获取集群中的索引列表
        """
        return self.es.cat.indices()


    def create_index(self,index_name,index_type):
        '''
        创建索引
        :param index_name: 索引名称
        :param index_type: 索引类型
        :return:
        '''
        #创建映射
        _index_mappings = {
            "mappings": {
                index_type: {
                    "properties": {
                        "title": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "keyword": {
                            "type": "text",
                            "analyzer": "standard",
                            "search_analyzer": "standard"
                        },
                        "source": {
                            "type": "keyword"
                        }
                    }
                }
            }
        }
        if self.es.indices.exists(index=index_name) is not True:
            res = self.es.indices.create(index=index_name, body=_index_mappings)
            print(res)
        else:
            print("索引已经存在, 请确认")

    def show_indexfields(self, index_name, index_type):
        """
        获取索引中包含的字段
        :param index_name: 索引名称
        :param index_type: 索引类型
        """
        url = "http://" + self.ip + ":" + self.port + "/" + index_name
        try:
            response = requests.get(url)
            json_r = response.json()
            return json_r[index_name]["mappings"][index_type]["properties"].keys()
        except :
            return ""


    def insert_datas(self,index_name,index_type):
        '''
        数据存储到es
        :param index_name: 索引名称
        :param index_type: 索引类型
        :return:
        '''

        if self.es.indices.exists(index=index_name) is not True:
            print("当前索引不存在, 请确认")
            return

        data = [
                {
                    "source": "慧聪网",
                    "keyword": "电视",
                    "title": "付费 电视 行业面临的转型和挑战"
                },
                {
                    "source": "中国文明网",
                    "keyword": "电视",
                    "title": "电视专题片《巡视利剑》广获好评：铁腕反腐凝聚党心民心"
                }
            ]
        for item in data:
            res = self.es.index(index=index_name, doc_type=index_type, body=item)
            print(res)


    def bulk_index_data(self, index_name, index_type):
        '''
        用bulk将批量数据存储到es
        :param index_name: 索引名称
        :param index_type: 索引类型
        :return:
        '''
        datas = [
            {
             "source": "大街网",
             "keyword": "招聘",
             "title": "网络通讯工程师招聘"
             },
            {
             "source": "人民电视",
             "keyword": "电视",
             "title": "中国第21批赴刚果（金）维和部隊启程--人民 电视 --人民网"
             },
            {
             "source": "站长之家",
             "keyword": "电视",
             "title": "电视 盒子 哪个牌子好？ 吐血奉献三大选购秘笈"
             }
        ]
        ACTIONS = []
        for line in datas:
            action = {
                "_index": index_name,
                "_type": index_type,
                # "_id": , #_id 也可以默认生成，不赋值
                "_source": {
                    "source": line['source'],
                    "keyword": line['keyword'],
                    "title": line['title']}
            }
            ACTIONS.append(action)
            # 批量处理
        success, _ = bulk(self.es, ACTIONS, index=index_name, raise_on_error=True)
        print("Performed {0} actions".format(success))


    def Index_Data_FromCSV(self,csvfile, index_name, index_type):
        '''
        从CSV文件中读取数据，并存储到es中, 注意文件字段与ES中索引字段一致
        :param csvfile: csv文件，包括完整路径
        :return:
        '''
        with open(csvfile,"r",encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for data in enumerate(reader):
                res = self.es.index(index=index_name, doc_type=index_type, body=data)

            print("success")



    def get_data_by_id(self, index_name, index_type, id):
        """
        根据ID号查看文档
        :param index_name: 索引名称
        :param index_type: 索引类型
        :param id: 待查看数据的id号
        :return:
        """
        res = self.es.get(index=index_name, doc_type=index_type,id=id)
        print(res['_source'])
        print ('------------------------------------------------------------------')
        for hit in res['hits']['hits']:
            print (hit['_source']['source'],hit['_source']['keyword'],hit['_source']['title'])


    def get_data_by_body(self, index_name):
        """
        根据ID号查看文档
        :param index_name: 索引名称
        :param index_type: 索引类型
        :return:
        """
        # doc = {'query': {'match_all': {}}}
        doc = {
            "query": {
                "match": {
                    "keyword": "电视"
                }
            }
        }
        _searched = self.es.search(index=index_name, body=doc)

        for hit in _searched['hits']['hits']:
            print(hit['_source']['source'], hit['_source']['keyword'], hit['_source']['title'])



    def update_data_by_id(self, index_name, index_type, id):
        """
        根据ID号进行数据更新
        :param index_name: 索引名称
        :param index_type: 索引类型
        :param id: 待更新数据的id号
        :return:
        """
        new_data = {
            "doc":{"source":"芒果网","keyword":"新闻","title":"国庆大阅兵"}
        }

        self.es.update(index=index_name,doc_type=index_type,id=id,body=new_data)
        # 更新结果
        result = self.es.get(index=index_name,doc_type=index_type,id=id)
        print(result)
        print('数据{0}更新完成：{1}\t'.format(id,result['_source']))


    def delete_index_data(self,index_name,index_type,id):
        '''
        删除索引中的一条
        :param index_name: 索引名称
        :param index_type: 索引类型
        :param id: 待删除数据的id号
        :return:
        '''
        res = self.es.delete(index=index_name, doc_type=index_type, id=id)
        print(res)



if __name__ == "__main__":
    obj = HandleEsObj(ip = "127.0.0.1", port = "9200")
    print(obj.is_ping())
    print("----------------------")
    print(obj.get_info())
    print("----------------------")
    print(obj.is_health())
    print("----------------------")
    print(obj.get_nodes())
    print("----------------------")
    print(obj.get_indices())
