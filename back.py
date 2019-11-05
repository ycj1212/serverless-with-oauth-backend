from elasticsearch import Elasticsearch
import pprint as ppr
import json
import random
from datetime import datetime,timedelta

class ElaAPI:
    es = Elasticsearch(hosts="13.125.255.30", port=9200)   # 객체 생성
    def allIndex(cls):
        # Elasticsearch에 있는 모든 Index 조회
        print (cls.es.cat.indices())

#재헌이꺼 시작
    def grants_dataInsert(cls,username):
        #문자열 셔플 user id
        s = ''.join(random.sample(username,len(username)))
        #datetime init
        now = datetime.now()
        current = str(now.isoformat())
        next_current = str(now+timedelta(seconds=5))
        print("gggg")
        grants_data = {
            "id" : s,
            "created" : current,
            "expired" : next_current
            }
        res = cls.es.search(
            index = "grants",
            body = {
                "query": {
                    "match" : {
                        "_id" : username
                    }
                }
            })
        boolean_value = res["hits"]["total"]["value"]
        print("kkkk")
        if boolean_value == 0:
            res = cls.es.index(index="grants",doc_type="_doc",id=username,body=grants_data)
            print(res)
        else:
            print("user already exist")
    
    def grants_search(cls, indx=None):
        res = cls.es.search(
        index = "grants",
        body = {
            "query":{"match_all":{}}
            }
        )
        data = json.dumps(res, ensure_ascii=False, indent=4)
        data = json.loads(data)
        for i in range(len(data["hits"]["hits"])):
            print(data["hits"]["hits"][i]["_id"])
            print(data["hits"]["hits"][i]["_source"]["id"])
            print(data["hits"]["hits"][i]["_source"]["created"])
            print(data["hits"]["hits"][i]["_source"]["expired"])
    def grants_delete(cls,username):
        res = cls.es.delete(index="grants",doc_type="_doc",id=username)
        print(res)
    
#재헌이꺼 끝
##철주꺼 시작
    def tokens_dataInsert(cls,username):
        now = datetime.now()
        current = str(now.isoformat())
        next_current = str(now+timedelta(hours=1))
        tokens_data = {
            "accessToken" : "value of accessToken",
            "refreshToken" : "value of refreshToken",
            "expired_accessToken" : current,
            "expired_refreshToken" : next_current
            }
        res = cls.es.search(
            index = "tokens",
            body = {
                "query": {
                    "match" : {
                        "_id" : username
                    }
                }
            })
        boolean_value = res["hits"]["total"]["value"]
        if boolean_value == 0:
            res = cls.es.index(index="tokens",doc_type="_doc",id=username,body=tokens_data)
            print(res)
        else:
            print("user already exist")


    def tokens_search(cls, indx=None):
    # ===============
    # 데이터 조회 [전체]
    # ===============
        res = cls.es.search(
        index = "tokens",
        body = {
            "query":{"match_all":{}}
            }
        )
        data = json.dumps(res, ensure_ascii=False, indent=4)
        data = json.loads(data)
        for i in range(len(data["hits"]["hits"])):
            print(data["hits"]["hits"][i]["_id"])
            print(data["hits"]["hits"][i]["_source"]["accessToken"])
            print(data["hits"]["hits"][i]["_source"]["refreshToken"])
            print(data["hits"]["hits"][i]["_source"]["expired_accessToken"])
            print(data["hits"]["hits"][i]["_source"]["expired_refreshToken"])
    def tokens_delete(cls,username):
        res = cls.es.search(
                index = "users",
                body = {
                    "query": {
                        "match" : {
                            "_id" : username
                        }
                    }
                })
        boolean_value = res["hits"]["total"]["value"]
        if boolean_value == 1:
            res = cls.es.delete(index="tokens",doc_type="_doc",id=username)
            print("success delete for tokens")
        else:
            print("not exist username for tokens")
#철쭈꺼 끝
#한설꺼 시작
    def users_search(cls, indx=None):
    # ===============
    # 데이터 조회 [전체]
    # ===============
        res = cls.es.search(
        index = "users",
        body = {
            "query":{"match_all":{}}
            }
        )
        data = json.dumps(res, ensure_ascii=False, indent=4)
        data = json.loads(data)
        for i in range(len(data["hits"]["hits"])):
            print(data["hits"]["hits"][i]["_id"])
            print(data["hits"]["hits"][i]["_source"]["password"])

    def users_dataInsert(cls,username,password):
        users_data = {
            "password" : password,
            }
        res = cls.es.search(
            index = "users",
            body = {
                "query": {
                    "match" : {
                        "_id" : username
                    }
                }
            })
        boolean_value = res["hits"]["total"]["value"]
        if boolean_value == 0:
            res = cls.es.index(index="users",doc_type="_doc",id=username,body=users_data)
            print(res)
        else:
            print("user already exist")
            
    def users_delete(cls,username):
        res = cls.es.search(
                index = "users",
                body = {
                    "query": {
                        "match" : {
                            "_id" : username
                        }
                    }
                })
        boolean_value = res["hits"]["total"]["value"]
        if boolean_value == 1:
            res = cls.es.delete(index="users",doc_type="_doc",id=username)
            print("success delete for users")
        else:
            print("not exist username for users")

    def login(cls,username,passwd):
        res = cls.es.search(
                index = "users",
                body = {
                    "query": {
                        "match" : {
                            "_id" : username
                        }
                    }
                })
        boolean_value = res["hits"]["total"]["value"]
        if boolean_value == 1:
            ps = res["hits"]["hits"][0]["_source"]["password"]
            if passwd == ps:
                # 검증 성공
                print("verified")
            else:
                print("failed")
                return {
                    'statusCode': 401,
                    'body': ''
                }
        else:
            print("not exist user")
            return {
                'statusCode': 401,
                'body': ''
            }
        
#한설꺼 끝
    def deleteIndex(cls,index_name):
        cls.es.indices.delete(index=index_name)

es = ElaAPI()

#es.users_dataInsert("hanseol","123456")
#es.users_search()
#es.users_delete("ko")
#es.login("hanseol","1212412456")

#es.grants_dataInsert("kohanseol")
#es.grants_search()
#es.grants_delete("pppp")

#es.tokens_dataInsert("yang")
#es.tokens_search()
#es.tokens_delete("yggg")


def handler(event):
    es.login(event['userId'], event['password'])

if __name__ == "__main__":
    handler({'userId': 'yang', 'password': '1234'})