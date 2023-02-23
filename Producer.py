from os.path import exists
import os
import requests
import json

import datetime
import time

from Logger import Logger

log = Logger('Producer')




from kafka import KafkaProducer


with open('Config.json', 'r') as f:
    config = json.load(f)


from time import sleep



# # One more example
# producer.send(topic1, key=b'event#2', value=b'This is a Kafka-Python basic tutorial')
# producer.flush()
#
class Prodcuer:
 def __init__(self, arangoDBUrl,topicName,brokers):
    self.arangoDBUrl = arangoDBUrl
    self.topicName=topicName
    self.brokers=brokers
    self.producer = KafkaProducer(bootstrap_servers=brokers)


 def getExectionPlanIDs(self,createdDate):

     # createdDate='1676322519258'
     query = "FOR u IN executionPlan  FILTER u._created > xy and u._created>1676322519258  RETURN {_key:u._key}".replace(
          'xy', createdDate)
     # #
     # query = "FOR u IN executionPlan  FILTER u._created > xy   RETURN {_key:u._key}".replace(
     #      'xy', createdDate)
     # query = "FOR u IN executionPlan  FILTER u._created > xy and u._id  in ['executionPlan/a2220c6f-cf0e-5fba-8f05-b04559a6a28c']  RETURN {_key:u._key}".replace(
     #      'xy', createdDate)
     # query = "FOR u IN executionPlan  FILTER u._created > xy and u._id  in ['executionPlan/14a6582e-669f-5977-b7c7-6d1fd487168f']  RETURN {_key:u._key}".replace(
     #       'xy', createdDate)
     #query="FOR u IN executionPlan  FILTER u._created > xy and u._id=='executionPlan/b156e991-a538-5052-b513-1d25648906d8'  RETURN {_key:u._key}".replace('xy',createdDate)
     #query = "FOR u IN executionPlan  FILTER  u._id =='executionPlan/fdd38b4a-493f-5b9e-b462-28c07ac7fc07'  RETURN u".replace('xy', createdDate)
     #print(query)
     log.info(query)

     payload = json.dumps({
                "query": query
      })
     headers = {
                    'accept': 'application/json',
                    'Content-Type': 'application/json'
          }

     response = requests.request("POST", self.arangoDBUrl, headers=headers, data=payload)

     listKeys=json.loads(response.text)['result']
     #print(listKeys)
     log.info(listKeys)
     return json.dumps(listKeys)

     #return  listKeys

 def sendMessageToKafka(self,message):
     if  message:
         try:

             self.producer.send(self.topicName, value=message.encode('utf-8'))
             self.producer.flush()
         except Exception as e:
              #print(e)
               log.error(e)


# Topics/Brokers




class ProducerExecution:

    def __init__(self):
        self.topic1 = config['topic']
        self.brokers = [config['kafka-broker']]
        # self.url = "http://34.242.14.60:8529/_db/spline/_api/cursor"
        self.url = config['arangoDBUrl']

        self.p1 = Prodcuer(self.url,self.topic1,self.brokers)

    def run(self):

        path_to_file= "lastRun.json"
        while True:

            time.sleep(5)
            file_exists = exists(path_to_file)
            if (not file_exists or os.stat(path_to_file).st_size == 0):

                unixtime ='0'
                json.dumps({"lastRun":unixtime })
                excutionplanIDS = self.p1.getExectionPlanIDs(unixtime)

                with open('lastRun.json', 'w') as fcc_file:
                    fcc_file.write(json.dumps({"lastRun":unixtime }))
                if json.loads(excutionplanIDS):
                    self.p1.sendMessageToKafka(excutionplanIDS)
                    unixtime = self.getCurrentLinuxTime()
                    with open('lastRun.json', 'w') as fcc_file:
                          dicty={'lastRun':unixtime}
                          fcc_file.write(json.dumps(dicty))

            else:
                with open('lastRun.json', 'r') as fcc_file:
                    txt=fcc_file.read()
                    fcc_data = json.loads(txt)
                    lastrun=fcc_data["lastRun"]
                    excutionplanIDS = self.p1.getExectionPlanIDs(lastrun)

                    if json.loads(excutionplanIDS):
                        self.p1.sendMessageToKafka(excutionplanIDS)
                        unixtime = self.getCurrentLinuxTime()
                        with open('lastRun.json', 'w') as fcc_file:
                              dicty={'lastRun':unixtime}
                              fcc_file.write(json.dumps(dicty))




    def getCurrentLinuxTime(self):
        d = datetime.datetime.now()
        unixtime = str(int(datetime.datetime.timestamp(d) * 1000))
        return  str(int(unixtime))




producerEx=ProducerExecution()
producerEx.run()




