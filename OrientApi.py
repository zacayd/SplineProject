
import hashlib
import os
from datetime import datetime

import pandas as pd
import requests
import json
import threading

import sqlalchemy

from Logger import Logger

log = Logger('Consumer')

class OrientDBApi:
    def __init__(self, orientURL,databaseName,dictFileName,dictFileNameEdges,sqlApi):
        self.orientURL = orientURL
        self.databaseName=databaseName


        self.dictFileName=dictFileName
        self.dictFileNameEdges=dictFileNameEdges
        self.dictRids={}
        self.dictEdges={}
        self.sqlApi=sqlApi


        if os.path.exists(dictFileName):
            with open(dictFileName) as user_file:
                file_contents = user_file.read()
                parsed_json = json.loads(file_contents)
                self.dictRids =parsed_json

        if os.path.exists(dictFileNameEdges):
            with open(dictFileNameEdges) as user_file:
                file_contents = user_file.read()
                parsed_json = json.loads(file_contents)
                self.dictEdges = parsed_json



        if (not self.CheckIfDbExists()):
            url = f'{orientURL}/database/{self.databaseName}/plocal'


            payload = {}
            headers = {
                'Authorization': 'Basic cm9vdDpyb290'

            }

            response = requests.request("POST", url, headers=headers, data=payload)
            if (response.status_code==200):
                #remember ti remove
                if str(databaseName).split('_')[-1]=='E2E':
                    self.RunApiPost("CREATE CLASS COLUMN EXTENDS V","POST")
                    self.RunApiPost("CREATE CLASS DATAFLOW EXTENDS E","POST")
                else:
                    self.RunApiPost("CREATE CLASS LINEAGEOBJECT EXTENDS V","POST")
                    self.RunApiPost("CREATE CLASS LINEAGEFLOW EXTENDS E","POST")
            else:
                log.info(response.text)



            #log.info(response.text)
        else:
            log.info(f'Database {databaseName} already exists!')

    def search(self,df, row, result_df,OrigDict):


        try:
            targetDict={'Target_id':row['Target_id'],
                        'TargetLayerName':row['TargetLayerName'],
                        'TargetSchema':row['TargetSchema'],
                        'TargetDB':row['TargetDB'],
                        'TargetTableName':row['TargetTableName'],
                        'TargetColumn':row['TargetColumn'],
                        'TargetColumnId':row['TargetColumnId'],
                        'TargetDataType':row['TargetDataType'],
                        'TargetPrecision':row['TargetPrecision'],
                        'TargetScale':row['TargetScale'],
                        'TargetObjectType':row['TargetObjectType'],
                        'TargetIsObjectData':row['TargetIsObjectData'],
                        'TargetServer':row['TargetServer'],
                        'TargetProvider':row['TargetProvider']


                        }


            if targetDict['TargetIsObjectData'] == "1":

                myDict = OrigDict.copy()
                myDict.update(targetDict)

                try:

                        myDictDf={'ConnectionID':myDict['ConnectionID'],
                                  'ConnLogicName': myDict['ConnLogicName'],
                                  'ContainerObjectName': myDict['ContainerObjectName'],
                                  'ContainerObjectPath': myDict['ContainerObjectPath'],
                                  'ContainerObjectType': myDict['ContainerObjectType'],
                                  'ContainerToolType': myDict['ContainerToolType'],
                                  'ControlFlowName': myDict['ControlFlowName'],
                                  'ControlFlowPath': myDict['ControlFlowPath'],
                                  'Source_id': myDict['Source_id'],
                                  'sourceLayerName': myDict['sourceLayerName'],
                                  'SourceSchema': myDict['SourceSchema'],
                                  'SourceDB': myDict['SourceDB'],
                                  'SourceTableName':myDict['SourceTableName'],
                                  'SourceColumn':myDict['SourceColumn'],
                                  'SourceColumnId': myDict['SourceColumnId'],
                                  'SourceDataType':myDict['SourceDataType'],
                                  'SourcePrecision': myDict['SourcePrecision'],
                                  'SourceScale': myDict['SourceScale'],
                                  'SourceObjectType': myDict['SourceObjectType'],
                                  'SourceProvider': myDict['SourceProvider'],
                                  'SourceServer': myDict['SourceServer'],

                                  'Target_id': myDict['Target_id'],
                                  'TargetLayerName': myDict['TargetLayerName'],
                                  'TargetSchema': myDict['TargetSchema'],
                                  'TargetDB': myDict['TargetDB'],
                                  'TargetTableName': myDict['TargetTableName'],
                                  'TargetColumn': myDict['TargetColumn'],
                                  'TargetColumnId': myDict['TargetColumnId'],
                                  'TargetDataType': myDict['TargetDataType'],
                                  'TargetPrecision': myDict['TargetPrecision'],
                                  'TargetScale': myDict['TargetScale'],
                                  'TargetObjectType': myDict['TargetObjectType'],
                                  'TargetProvider': myDict['TargetProvider'],
                                  'TargetServer': myDict['TargetServer'],
                                  'SourceIsObjectData':myDict['SourceIsObjectData'],
                                  'TargetIsObjectData': myDict['TargetIsObjectData']

                                  }
                except Exception as e:
                 log.error(e)


                result_df = result_df.append(myDictDf, ignore_index=True)

        except Exception as e:
            log.error(e)

            return result_df


        else:
            target_column_id=targetDict['TargetColumnId']
            TargetLayerName=targetDict['TargetLayerName']
            TargetIsObjectData=targetDict['TargetIsObjectData']
            try:
                if TargetIsObjectData=="0" and not df.loc[(df['SourceColumnId'] == target_column_id) & (df['sourceLayerName'] == TargetLayerName)].empty:
                    #row = df.loc[df['SourceColumnId'] == target_column_id & df['sourceLayerName']==TargetLayerName].iloc[0]
                    row=  df.loc[(df['SourceColumnId']==target_column_id) & (df['sourceLayerName'] == TargetLayerName)].iloc[0]




                    xDf = self.search(df, row, result_df, OrigDict)
                    result_df = pd.concat([xDf, result_df])

            except Exception as e:
             log.error(e)
            return result_df

    def ShrinkDf(self,df):
        listOfDf=[]
        dfIsObject=df[df['SourceIsObjectData']=='1']

        for index, row in dfIsObject.iterrows():
            result_df = pd.DataFrame(columns=df.columns)
            srcDict=row.to_dict()
            keys = ['ConnectionID', 'ConnLogicName','ContainerObjectName','ContainerObjectPath',
                        'ContainerObjectType','ContainerToolType','ControlFlowName','ControlFlowPath',
                        'Source_id','sourceLayerName','SourceSchema','SourceDB','SourceTableName','SourceColumn',
                        'SourceColumnId','SourceDataType','SourceObjectType','SourceIsObjectData','SourceServer','SourceProvider',
                        'SourcePrecision','SourceScale','SourceSql','SourceConnectionKey','SourcePhysicalTable','SourceFilePath',
                        'SourceCode','SourceCoordinate','ISColumnOrphand','ContainerObjectDesciption','LinkType','LinkDescription',
                        'UpdatedDate'
                        ]

            new_dict = dict((k, srcDict[k]) for k in keys if k in srcDict)

            r_df = self.search(df, row, result_df, new_dict)
            listOfDf.append(r_df)

        shrinkedDF=pd.DataFrame()
        if len(listOfDf)>0:
            shrinkedDF=pd.concat(listOfDf)

        return shrinkedDF


    def CheckIfDbExists(self):


        url = f'{self.orientURL}/database/{self.databaseName}'

        payload = {}
        headers = {
            'Authorization': 'Basic cm9vdDpyb290'

        }

        response = requests.request("GET", url, headers=headers, data=payload)

        #log.info(response.text)
        exists=False
        dictResult=json.loads(response.text)
        try:

            if 'error' in dictResult:
                exists=False
            elif 'server' in dictResult:
                return  True
        except  Exception as e:
           log.error(e)
           return  False


    def RunApiPost(self,command,method):
        url = f'{self.orientURL}/command/{self.databaseName}/sql'

        payload = json.dumps({
            "command": command
        })
        headers = {
            'Authorization': 'Basic cm9vdDpyb290',
            'Content-Type': 'application/json'

        }

        response = requests.request(method, url, headers=headers, data=payload)

        #log.info(response.text)
        x="-1"
        if(response.status_code==200):
            if len(json.loads(response.text)["result"])==0:
                return x
            if "@rid" in json.loads(response.text)["result"][0]:
                x= json.loads(response.text)["result"][0]["@rid"]
            else:
                x = json.loads(response.text)["result"][0]

        return x

    def RunMultipleRows(self,command,method):
        url = f'{self.orientURL}/command/{self.databaseName}/sql'

        payload = json.dumps({
            "command": command
        })
        headers = {
            'Authorization': 'Basic cm9vdDpyb290',
            'Content-Type': 'application/json'

        }

        response = requests.request(method, url, headers=headers, data=payload)

        # log.info(response.text)
        x="-1"
        if(response.status_code==200):
            x=[ x['ObjectGUID'] for x in json.loads(response.text)["result"]]



        return x


    def RunMultipleRowsAll(self,command,method):
        url = f'{self.orientURL}/command/{self.databaseName}/sql'

        payload = json.dumps({
            "command": command
        })
        headers = {
            'Authorization': 'Basic cm9vdDpyb290',
            'Content-Type': 'application/json'

        }

        response = requests.request(method, url, headers=headers, data=payload)

        # log.info(response.text)

        result=json.loads(response.text)["result"]



        return result


    def DeleteRow(self,ControlFlowName,ConnectionID,uq, tableName):
        try:

            deleteTxt = f"Delete from {tableName} where ControlFlowName=? and ConnectionID=?"
            self.sqlApi.cursor.execute(deleteTxt, ControlFlowName+'_'+uq, ConnectionID)
            self.sqlApi.cursor.commit()

        except  Exception as e:
            log.error(e)

    def DeleteVertex(self,ControlFlowPath,ConnectionID):

        try:
            dictOfRids=self.RunMultipleRows(f"Select @rid,ObjectGUID  from  COLUMN where ConnectionID={ConnectionID}  and controlFlowPath='{ControlFlowPath}' and IsMap=1","POST")
            for k in dictOfRids :
                self.dictRids.pop(k,None)

            with open(self.dictFileName, "w") as outfile:
                outfile.write(json.dumps(self.dictRids))

            self.RunApiPost(f"DELETE FROM COLUMN where controlFlowPath='{ControlFlowPath}' and ConnectionID={ConnectionID}"
                            f" and  IsMap=1  unsafe","POST")
        except  Exception as e:
            log.error(e)

    def CreateVertices(self,df,uq,num):

        log.info('Start E2E:')
        log.info(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

        df=self.ShrinkDf(df)

        log.info("size:")
        log.info(len(df))



        if len(df)>0:
            dfSource = df[
                ['ConnectionID', 'ConnLogicName', 'ContainerObjectName', 'ContainerObjectPath', 'ContainerObjectType',
                 'ContainerToolType', 'ControlFlowName', 'ControlFlowPath',"sourceLayerName"]]

            dfSource = dfSource.drop_duplicates()
            for index, row in dfSource.iterrows():

                try:
                    # self.DeleteVertex(row['ControlFlowPath'],row['ConnectionID'])
                    # row['sourceLayerName']=row['sourceLayerName'][:-2]
                    self.DeleteRow(row['ControlFlowName'], row['ConnectionID'],uq,"TI.COLUMN_FULL")
                except  Exception as e:
                    log.error(e)

            # same for Targets

            log.info("Create vertexs:")
            currentIndex=1;
            total=len(df)

            # create a lock to synchronize access to OrientDB

            batch_cmds = []
            for index, row in df.iterrows():
                #self.DoWork(currentIndex, row, total,lock,num)
                self.DoWorkBatch(currentIndex,row,total,num,batch_cmds,uq)
                currentIndex=currentIndex+1



            self.BatchApi(batch_cmds)




            # create a ThreadPoolExecutor with 24 workers
            # with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
            #     # submit a separate write_to_orientdb task for each row
            #     #futures = [executor.submit(self.DoWork,currentIndex, row,total, lock) for index, row in df.iterrows()]
            #     futures = []
            #     for index, row in df.iterrows():
            #         future = executor.submit(self.DoWork, currentIndex, row, total, lock)
            #         futures.append(future)
            #         currentIndex=currentIndex+1
            #
            #     #currentIndex, row, total, lock
            #     # wait for all tasks to complete
            #     concurrent.futures.wait(futures)


            #populate TI.COLUMN_FULL from OrietDBE2E:

            self.SyncOrientToSql(uq)
    def BatchApi(self,batch_cmds):

        # Build the REST API request
        url = "{}/batch/{}".format(self.orientURL, self.databaseName)

        headers = {
            'Authorization': 'Basic cm9vdDpyb290',
            'Content-Type': 'application/json'

        }

        payload = {"transaction": True, "operations": batch_cmds}
        payload=json.dumps(payload)

        # Send the request to the server
        response = requests.request("POST", url, headers=headers, data=payload)

        # Check the response for errors
        if response.status_code == 200:
            log.info("Batch command executed successfully!")
        else:
            log.info("Error executing batch command: {}".format(response.text))


        if response.status_code == 200:
            log.info("Batch command executed successfully!")
        else:
            log.info("Error executing batch command: {}".format(response.text))

    #currentIndex, row, total, lock
    def Test(self,currentIndex,row,total,lock):
        with lock:
            log.info('Print me'+ str(currentIndex) )


    def DoWork(self, currentIndex, row, total,lock,num):

            try:
                with lock:
                    log.info(f"Execution plan number{str(num)}")

                    log.info(f"Create vertexs number:{str(currentIndex)} out of {str(total)}")


                    log.info(f"Create vertexs number:{str(currentIndex)} out of {str(total)}")

                    log.info(row["SourceDB"])
                    ObjectID_1 = row["SourceDB"] + row["SourceSchema"] + row["SourceTableName"] + row[
                        'SourceColumn']
                    ObjectGUID_1 = hashlib.md5(ObjectID_1.encode('utf_16_le')).hexdigest()

                    ObjectID_2 = row["ConnectionID"] + row["ContainerObjectPath"] + row["ControlFlowPath"] + row[
                        "sourceLayerName"] + row['SourceColumn']
                    ObjectGUID_2 = hashlib.md5(ObjectID_2.encode('utf_16_le')).hexdigest()

                    ###
                    ObjectID_3 = row["TargetDB"] + row["TargetSchema"] + row["TargetTableName"] + row[
                        'TargetColumn']
                    ObjectGUID_3 = hashlib.md5(ObjectID_3.encode('utf_16_le')).hexdigest()

                    ObjectID_4 = row["ConnectionID"] + row["ContainerObjectPath"] + row["ControlFlowPath"] + row[
                        "TargetLayerName"] + row['TargetColumn']
                    ObjectGUID_4 = hashlib.md5(ObjectID_4.encode('utf_16_le')).hexdigest()
                    VSourceToolType = 'FILE' if row["SourceDB"] == '-1' else row["ContainerToolType"]
                    VTargetToolType = 'FILE' if row["TargetDB"] == '-1' else row["ContainerToolType"]

                    listOfParams1 = [row["ConnectionID"], row["ConnLogicName"], VSourceToolType,
                                     row["ContainerObjectName"],
                                     row["ContainerObjectPath"], row["ContainerObjectType"], row["ControlFlowName"],
                                     row["ControlFlowPath"],
                                     ObjectID_1, ObjectGUID_1, row["SourceObjectType"], row["SourceColumn"],
                                     row["SourceDataType"], row["SourceTableName"],
                                     row['SourceSchema'], row['SourceDB'], row['SourceTableName'], row["SourceIsObjectData"],
                                     1, 0, 1, str(pd.datetime.datetime.now()), 'DB'
                                     ]
                    listOfParams2 = [row["ConnectionID"], row["ConnLogicName"], row["ContainerToolType"],
                                     row["ContainerObjectName"],
                                     row["ContainerObjectPath"], row["ContainerObjectType"], row["ControlFlowName"],
                                     row["ControlFlowPath"],
                                     ObjectID_2, ObjectGUID_2, row["SourceObjectType"], row["SourceColumn"],
                                     row["SourceDataType"], row["sourceLayerName"],
                                     row['SourceSchema'], row['SourceDB'], row['SourceTableName'], row["SourceIsObjectData"],
                                     0, 1, 1, str(pd.datetime.datetime.now()), 'ETL'
                                     ]

                    listOfParams3 = [row["ConnectionID"], row["ConnLogicName"], VTargetToolType,
                                     row["ContainerObjectName"],
                                     row["ContainerObjectPath"], row["ContainerObjectType"], row["ControlFlowName"],
                                     row["ControlFlowPath"],
                                     ObjectID_3, ObjectGUID_3, row["TargetObjectType"], row["TargetColumn"],
                                     row["TargetDataType"], row["TargetTableName"],
                                     row['TargetSchema'], row['TargetDB'], row['TargetTableName'], row["TargetIsObjectData"],
                                     1, 0, 1, str(pd.datetime.datetime.now()), 'DB'
                                     ]

                    listOfParams4 = [row["ConnectionID"], row["ConnLogicName"], row["ContainerToolType"],
                                     row["ContainerObjectName"],
                                     row["ContainerObjectPath"], row["ContainerObjectType"], row["ControlFlowName"],
                                     row["ControlFlowPath"],
                                     ObjectID_4, ObjectGUID_4, row["TargetObjectType"], row["TargetColumn"],
                                     row["TargetDataType"], row["TargetLayerName"],
                                     row['TargetSchema'], row['TargetDB'], row['TargetTableName'], row["TargetIsObjectData"],
                                     0, 1, 1, str(pd.datetime.datetime.now()), 'ETL'
                                     ]

                    if (row["SourceIsObjectData"] == "1" and row["TargetIsObjectData"] == "1"):

                        # command=self.CreateStatemt(listOfParams1)

                            log.info("4 vertex ")

                            rid1 = self.CheckInDictionary(ObjectGUID_1, listOfParams1, False)
                            # self.InsertStatement(listOfParams1,"TI.COLUMN_FULL")

                            # command = self.CreateStatemt(listOfParams2)
                            rid2 = self.CheckInDictionary(ObjectGUID_2, listOfParams2, False)
                            # self.InsertStatement(listOfParams2, "TI.COLUMN_FULL")

                            # command = self.CreateStatemt(listOfParams3)
                            rid3 = self.CheckInDictionary(ObjectGUID_3, listOfParams3, False)
                            # self.InsertStatement(listOfParams3, "TI.COLUMN_FULL")

                            # command = self.CreateStatemt(listOfParams4)
                            rid4 = self.CheckInDictionary(ObjectGUID_4, listOfParams4, False)
                            # self.InsertStatement(listOfParams4, "TI.COLUMN_FULL")

                            # CREATE EDGE DATAFLOW FROM #10:3 TO #11:4
                            # command=f'CREATE DATAFLOW FROM {rid1} TO {rid2}'
                            # self.RunApiPost(command, "POST")
                            edgeId = self.CheckInDictionaryLinks(rid1, rid2, True)
                            log.info(f'{edgeId} created!')

                            edgeId = self.CheckInDictionaryLinks(rid2, rid4, True)
                            log.info(f'{edgeId} created!')
                            edgeId = self.CheckInDictionaryLinks(rid4, rid3, True)
                            log.info(f'{edgeId} created!')
                            currentIndex = currentIndex + 1

                    if (row["SourceIsObjectData"] == "1" and row["TargetIsObjectData"] == "0"):

                            log.info("3 vertex ")

                            rid1 = self.CheckInDictionary(ObjectGUID_1, listOfParams1, False)
                            # self.InsertStatement(listOfParams1, "TI.COLUMN_FULL")

                            # command = self.CreateStatemt(listOfParams2)
                            rid2 = self.CheckInDictionary(ObjectGUID_2, listOfParams2, False)
                            # self.InsertStatement(listOfParams2, "TI.COLUMN_FULL")

                            # command = self.CreateStatemt(listOfParams4)
                            listOfParams4[20] = 0
                            rid4 = self.CheckInDictionary(ObjectGUID_4, listOfParams4, False)
                            # self.InsertStatement(listOfParams4, "TI.COLUMN_FULL")

                            # CREATE EDGE DATAFLOW FROM #10:3 TO #11:4
                            edgeId = self.CheckInDictionaryLinks(rid1, rid2, True)
                            log.info(f'Edge {edgeId} created!')
                            edgeId = self.CheckInDictionaryLinks(rid2, rid4, True)
                            log.info(f'Edge {edgeId} created!')
                            currentIndex = currentIndex + 1

                    if (row["SourceIsObjectData"] == "0" and row["TargetIsObjectData"] == "1"):

                            log.info("3 vertex ")
                            # rid1 -ph
                            # rid2-sem
                            # rid3-ph
                            # rid4-sem

                            # command = self.CreateStatemt(listOfParams2)
                            listOfParams2[20] = 0
                            rid2 = self.CheckInDictionary(ObjectGUID_2, listOfParams2, False)
                            # self.InsertStatement(listOfParams2, "TI.COLUMN_FULL")

                            # command = self.CreateStatemt(listOfParams3)
                            rid3 = self.CheckInDictionary(ObjectGUID_3, listOfParams3, False)
                            # self.InsertStatement(listOfParams3, "TI.COLUMN_FULL")

                            # command = self.CreateStatemt(listOfParams4)
                            rid4 = self.CheckInDictionary(ObjectGUID_4, listOfParams4, False)
                            # self.InsertStatement(listOfParams4, "TI.COLUMN_FULL")

                            # CREATE EDGE DATAFLOW FROM #10:3 TO #11:4
                            edgeId = self.CheckInDictionaryLinks(rid2, rid4, True)
                            log.info(f'{edgeId} created!')
                            edgeId = self.CheckInDictionaryLinks(rid4, rid3, True)
                            log.info(f'{edgeId} created!')
                            currentIndex = currentIndex + 1

                    if (row["SourceIsObjectData"] == "0" and row["TargetIsObjectData"] == "0"):

                            log.info("2 vertex")

                            # command = self.CreateStatemt(listOfParams2)
                            listOfParams2[20] = 0
                            rid2 = self.CheckInDictionary(ObjectGUID_2, listOfParams2, False)
                            # self.InsertStatement(listOfParams2, "TI.COLUMN_FULL")
                            listOfParams4[20] = 0
                            # command = self.CreateStatemt(listOfParams4)
                            rid4 = self.CheckInDictionary(ObjectGUID_4, listOfParams4, False)
                            # self.InsertStatement(listOfParams4, "TI.COLUMN_FULL")

                            # CREATE EDGE DATAFLOW FROM #10:3 TO #11:4
                            edgeId = self.CheckInDictionaryLinks(rid2, rid4, True)
                            log.info(f'{edgeId} created!')
                            currentIndex = currentIndex + 1
            except Exception as e:
                log.error(e)

    def create_vertex(self,ObjectGid,listOfParams):
        shared_resource = []
        lock = threading.Lock()
        lock.acquire()
        try:

            content = self.UpdateSetStatemt(listOfParams)

            return {
                "type": "cmd",
                "language": "sql",
                "command": f"{content} UPSERT  RETURN AFTER $current.@rid  WHERE ObjectGUID='{ObjectGid}' "
            }
        finally:
            lock.release()

    def create_edge(self,from_vertex, to_vertex):

        shared_resource = []
        lock = threading.Lock()
        lock.acquire()
        try:
            return {
                "type": "cmd",
                "language": "sql",
                "command": f"CREATE EDGE DATAFLOW FROM  (SELECT FROM COLUMN WHERE ObjectGUID = '{from_vertex}')  TO (SELECT FROM COLUMN WHERE ObjectGUID = '{to_vertex}')"

            }
        finally:
            lock.release()

    def delete_edge(self, from_vertex, to_vertex):
        return {
            "type": "cmd",
            "language": "sql",
            "command": f"DELETE EDGE DATAFLOW FROM  (SELECT FROM COLUMN WHERE ObjectGUID = '{from_vertex}')  TO (SELECT FROM COLUMN WHERE ObjectGUID = '{to_vertex}')"

        }



    def DoWorkBatch(self, currentIndex, row, total, num,batch_cmds,uq):

        try:

                log.info(f"Execution plan number{str(num)}")

                log.info(f"Create vertexs number:{str(currentIndex)} out of {str(total)}")

                log.info(f"Create vertexs number:{str(currentIndex)} out of {str(total)}")

                # log.info(row["SourceDB"])
                row['ControlFlowName']=row['ControlFlowName']+'_'+uq

                ObjectID_1 = row["SourceDB"] + row["SourceSchema"] + row["SourceTableName"] + row['SourceColumn']
                ObjectGUID_1 = hashlib.md5(ObjectID_1.encode('utf_16_le')).hexdigest()

                ObjectID_2 = row["ConnectionID"] + row["ContainerObjectPath"] + row["ControlFlowPath"] + row[
                        "sourceLayerName"] + row['SourceColumn']
                ObjectGUID_2 = hashlib.md5(ObjectID_2.encode('utf_16_le')).hexdigest()

                    ###
                ObjectID_3 = row["TargetDB"] + row["TargetSchema"] + row["TargetTableName"] + row[
                        'TargetColumn']
                ObjectGUID_3 = hashlib.md5(ObjectID_3.encode('utf_16_le')).hexdigest()

                ObjectID_4 = row["ConnectionID"] + row["ContainerObjectPath"] + row["ControlFlowPath"] + row[
                        "TargetLayerName"] + row['TargetColumn']
                ObjectGUID_4 = hashlib.md5(ObjectID_4.encode('utf_16_le')).hexdigest()
                VSourceToolType = 'FILE' if row["SourceDB"] == '-1' else row["ContainerToolType"]
                VTargetToolType = 'FILE' if row["TargetDB"] == '-1' else row["ContainerToolType"]

                listOfParams1 = [row["ConnectionID"], row["ConnLogicName"], VSourceToolType,
                                     row["ContainerObjectName"],
                                     row["ContainerObjectPath"], row["ContainerObjectType"], row['ControlFlowName'],
                                     row['SourceSchema'],
                                     ObjectID_1, ObjectGUID_1, row["SourceObjectType"], row["SourceColumn"],
                                     row["SourceDataType"], row["SourceTableName"],
                                     row['SourceSchema'], row['SourceDB'], row['SourceTableName'],
                                     row["SourceIsObjectData"],
                                     1, 0, 1, str(pd.datetime.datetime.now()), 'DB'
                                     ]
                listOfParams2 = [row["ConnectionID"], row["ConnLogicName"], row["ContainerToolType"],
                                     row["ContainerObjectName"],
                                     row["ContainerObjectPath"], row["ContainerObjectType"], row["ControlFlowName"],
                                     row["ControlFlowPath"],
                                     ObjectID_2, ObjectGUID_2, row["SourceObjectType"], row["SourceColumn"],
                                     row["SourceDataType"], row["sourceLayerName"],
                                     row['SourceSchema'], row['SourceDB'], row['SourceTableName'],
                                     row["SourceIsObjectData"],
                                     0, 1, 1, str(pd.datetime.datetime.now()), 'ETL'
                                     ]

                listOfParams3 = [row["ConnectionID"], row["ConnLogicName"], VTargetToolType,
                                     row["ContainerObjectName"],
                                     row["ContainerObjectPath"], row["ContainerObjectType"], row['ControlFlowName'],
                                     row['TargetSchema'],
                                     ObjectID_3, ObjectGUID_3, row["TargetObjectType"], row["TargetColumn"],
                                     row["TargetDataType"], row["TargetTableName"],
                                     row['TargetSchema'], row['TargetDB'], row['TargetTableName'],
                                     row["TargetIsObjectData"],
                                     1, 0, 1, str(pd.datetime.datetime.now()), 'DB'
                                     ]

                listOfParams4 = [row["ConnectionID"], row["ConnLogicName"], row["ContainerToolType"],
                                     row["ContainerObjectName"],
                                     row["ContainerObjectPath"], row["ContainerObjectType"], row["ControlFlowName"],
                                     row["ControlFlowPath"],
                                     ObjectID_4, ObjectGUID_4, row["TargetObjectType"], row["TargetColumn"],
                                     row["TargetDataType"], row["TargetLayerName"],
                                     row['TargetSchema'], row['TargetDB'], row['TargetTableName'],
                                     row["TargetIsObjectData"],
                                     0, 1, 1, str(pd.datetime.datetime.now()), 'ETL'
                                     ]

                if (row["SourceIsObjectData"] == "1" and row["TargetIsObjectData"] == "1"):
                    # command=self.CreateStatemt(listOfParams1)


                        # log.info("4 vertex ")


                        v1 = self.create_vertex(ObjectGUID_1, listOfParams1)

                        v2 = self.create_vertex(ObjectGUID_2, listOfParams2)
                        v3 = self.create_vertex(ObjectGUID_3, listOfParams3)
                        v4 = self.create_vertex(ObjectGUID_4, listOfParams4)

                        d1 = self.delete_edge(ObjectGUID_1, ObjectGUID_2)
                        d2 = self.delete_edge(ObjectGUID_2, ObjectGUID_4)
                        d3 = self.delete_edge(ObjectGUID_4, ObjectGUID_3)


                        e1 = self.create_edge(ObjectGUID_1, ObjectGUID_2)
                        e2 = self.create_edge(ObjectGUID_2, ObjectGUID_4)
                        e3 = self.create_edge(ObjectGUID_4, ObjectGUID_3)

                        batch_cmds.extend([v1, v2, v3, v4,d1,d2,d3, e1, e2, e3])



                    # self.RunApiPost(command, "POST")
                    # edgeId = self.CheckInDictionaryLinks(rid1, rid2, True)
                    # log.info(f'{edgeId} created!')

                    # edgeId = self.CheckInDictionaryLinks(rid2, rid4, True)
                    # log.info(f'{edgeId} created!')
                    # edgeId = self.CheckInDictionaryLinks(rid4, rid3, True)
                    # log.info(f'{edgeId} created!')


                if (row["SourceIsObjectData"] == "1" and row["TargetIsObjectData"] == "0"):
                    # log.info("3 vertex ")

                    listOfParams4[20] = 0

                    v1 = self.create_vertex(ObjectGUID_1, listOfParams1)
                    v2 = self.create_vertex(ObjectGUID_2, listOfParams2)

                    v4 = self.create_vertex(ObjectGUID_4, listOfParams4)

                    d1 = self.delete_edge(ObjectGUID_1, ObjectGUID_2)
                    d2 = self.delete_edge(ObjectGUID_2, ObjectGUID_4)

                    e1 = self.create_edge(ObjectGUID_1, ObjectGUID_2)
                    e2 = self.create_edge(ObjectGUID_2, ObjectGUID_4)


                    batch_cmds.extend([v1, v2, v4,d1,d2, e1, e2])


                    # command1 = self.CreateStatemt(listOfParams1)
                    # # Create the vertices
                    # batch_cmds.append({"type": "cmd", "language": "sql",
                    #                    "command": f"LET rid1 = {command1}"})
                    #
                    #
                    #
                    #
                    # command2 = self.CreateStatemt(listOfParams2)
                    # # Create the vertices
                    # batch_cmds.append({"type": "cmd", "language": "sql",
                    #                    "command": f"LET rid2 = {command2}"})
                    #
                    #
                    # listOfParams4[20] = 0
                    #
                    # command4 = self.CreateStatemt(listOfParams4)
                    # # Create the vertices
                    # batch_cmds.append({"type": "cmd", "language": "sql",
                    #                    "command": f"LET rid4 = {command4}"})
                    #
                    # command = f'DELETE EDGE FROM $rid1 TO $rid2'
                    # batch_cmds.append({"type": "cmd", "language": "sql", "command": command})
                    #
                    # command = f'CREATE EDGE FROM $rid1 TO $rid2'
                    # batch_cmds.append({"type": "cmd", "language": "sql", "command": command})
                    #
                    # command = f'DELETE EDGE FROM $rid1 TO $rid2'
                    # batch_cmds.append({"type": "cmd", "language": "sql", "command": command})
                    #
                    # command = f'CREATE EDGE FROM $rid2 TO $rid4'
                    # batch_cmds.append({"type": "cmd", "language": "sql", "command": command})




                if (row["SourceIsObjectData"] == "0" and row["TargetIsObjectData"] == "1"):
                    # log.info("3 vertex ")
                    # rid1 -ph
                    # rid2-sem
                    # rid3-ph
                    # rid4-sem

                    # command = self.CreateStatemt(listOfParams2)
                    listOfParams2[20] = 0


                    v2 = self.create_vertex(ObjectGUID_2, listOfParams2)

                    v3 = self.create_vertex(ObjectGUID_3, listOfParams2)

                    v4 = self.create_vertex(ObjectGUID_4, listOfParams4)

                    d2 = self.delete_edge(ObjectGUID_2, ObjectGUID_4)
                    d3 = self.delete_edge(ObjectGUID_4, ObjectGUID_3)

                    e2 = self.create_edge(ObjectGUID_2, ObjectGUID_4)
                    e3 = self.create_edge(ObjectGUID_4, ObjectGUID_3)

                    batch_cmds.extend([ v2, v3, v4, d2,d3, e2,e3])





                    # edgeId = self.CheckInDictionaryLinks(rid2, rid4, True)
                    # log.info(f'{edgeId} created!')
                    # edgeId = self.CheckInDictionaryLinks(rid4, rid3, True)
                    # log.info(f'{edgeId} created!')


                if (row["SourceIsObjectData"] == "0" and row["TargetIsObjectData"] == "0"):
                    # log.info("2 vertex")

                    # command = self.CreateStatemt(listOfParams2)
                    listOfParams2[20] = 0
                    listOfParams4[20] = 0

                    v2 = self.create_vertex(ObjectGUID_2, listOfParams2)
                    v4 = self.create_vertex(ObjectGUID_4, listOfParams4)

                    d1 = self.delete_edge(ObjectGUID_2, ObjectGUID_4)

                    e1 = self.create_edge(ObjectGUID_2, ObjectGUID_4)

                    batch_cmds.extend([v2, v4, d1,e1])




                    # CREATE EDGE DATAFLOW FROM #10:3 TO #11:4
                    # edgeId = self.CheckInDictionaryLinks(rid2, rid4, True)
                    # log.info(f'{edgeId} created!')

        except Exception as e:
            log.error(e)

    def SyncOrientToSql(self, uq):
        command = f"select ConnectionID" \
                  f",ConnLogicName" \
                  f",ToolName" \
                  f",ToolType" \
                  f",ConnectionID as DisplayConnectionId" \
                  f",ContainerObjectName" \
                  f",ContainerObjectPath" \
                  f",ContainerObjectType" \
                  f",ControlFlowPath" \
                  f",ControlFlowName" \
                  f",ObjectID" \
                  f",ObjectGUID" \
                  f",ObjectType" \
                  f",ObjectName" \
                  f",DataType" \
                  f",Precision" \
                  f",Scale" \
                  f",LayerName" \
                  f",SchemaName" \
                  f",DatabaseName" \
                  f",TableName" \
                  f",IsObjectData" \
                  f",IsMap" \
                  f",IsVisible" \
                  f",Lvl5" \
                  f",Lvl4" \
                  f",Lvl3" \
                  f",Lvl2" \
                  f",Lvl1" \
                  f",UpdatedDate" \
                  f",ServerName" \
                  f",IsSrcColumnOrphan" \
                  f" from COLUMN " \
                  f"where ControlFlowName like '%{uq}'"
        try:
            list_of_dicts = self.RunMultipleRowsAll(command, 'POST')
            df = pd.DataFrame(list_of_dicts)
            df['UpdatedDate'] = pd.to_datetime(df['UpdatedDate'], format='%Y-%m-%d')
            tableName = "COLUMN_FULL"

            df.to_sql(tableName, self.sqlApi.engine, schema=self.sqlApi.schema, if_exists='append', index=False , dtype={'date_column': sqlalchemy.types.DateTime()})
        except Exception as e:
            log.error(e)






    def CheckIfExistsObjectGUID(self,tableName,ObjectGUID):
        try:

            self.sqlApi.cursor.execute(f"SELECT ObjectGUID FROM {tableName} where ObjectGUID=?",ObjectGUID)
            myresult = self.sqlApi.cursor.fetchall()
            if len(myresult)==0:
                return True
            else:
                return False


        except Exception as e:
            log.error(e)

    def InsertStatement(self,listOfParams,tableName):
        try:
            ObjectGUID=listOfParams[9]
            if(self.CheckIfExistsObjectGUID(tableName,ObjectGUID)):
                insertTxt=f"INSERT INTO {tableName} \
                       ([ConnectionID] \
                       ,[ConnLogicName]\
                       ,[ToolName]\
                       ,[ToolType]\
                       ,[DisplayConnectionId]\
                       ,[ContainerObjectName]\
                       ,[ContainerObjectPath]\
                       ,[ContainerObjectType]\
                       ,[ControlFlowPath]\
                       ,[ControlFlowName]\
                       ,[ObjectID]\
                       ,[ObjectGUID]\
                       ,[ObjectType]\
                       ,[ObjectName]\
                       ,[DataType]\
                       ,[Precision]\
                       ,[Scale]\
                       ,[LayerName]\
                       ,[SchemaName]\
                       ,[DatabaseName]\
                       ,[TableName]\
                       ,[IsObjectData]\
                       ,[IsMap]\
                       ,[IsVisible]\
                       ,[UpdatedDate]\
                       ,[ServerName])\
                     VALUES \
                           (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,cast ('{listOfParams[21]}' as datetime2),?)"
                # log.info(insertTxt)

                try:

                        self.sqlApi.cursor.execute(insertTxt,
                                                   listOfParams[0],
                                                   listOfParams[1],
                                                   listOfParams[2],
                                                   listOfParams[22],
                                                   listOfParams[0],
                                                   listOfParams[3],
                                                   listOfParams[4],
                                                   listOfParams[5],
                                                   listOfParams[6],
                                                   listOfParams[7],
                                                   listOfParams[8],
                                                   listOfParams[9],
                                                   listOfParams[10],
                                                   listOfParams[11],
                                                   listOfParams[12],
                                                   '',
                                                   '',
                                                   listOfParams[13],
                                                   listOfParams[14],
                                                   listOfParams[15],
                                                   listOfParams[16],
                                                   listOfParams[18],
                                                   listOfParams[19],
                                                   listOfParams[20],
                                                   '',
                                                   )
                        self.sqlApi.cursor.commit()
                except Exception as e:
                        log.error(e)
            else:
                log.info(f"ObjectGuid:{ObjectGUID} exists!")


        except Exception as e:
            log.error(e)


    def UpdateSetStatemt(self,listOfParams):
        cmdDict1 = {'ConnectionID': listOfParams[0],
                    'ConnLogicName': listOfParams[1],
                    'ToolName': listOfParams[2],
                    'ToolType': listOfParams[22],
                    'DisplayConnectionID':listOfParams[0],
                    'ContainerObjectName': listOfParams[3],
                    'ContainerObjectPath': listOfParams[4],
                    'ContainerObjectType': listOfParams[5],
                    'ControlFlowName': listOfParams[6],
                    'ControlFlowPath': listOfParams[7],
                    'ObjectID': listOfParams[8],
                    'ObjectGUID': listOfParams[9],
                    'ObjectType': listOfParams[10],
                    'ObjectName':listOfParams[11],
                    'DataType':listOfParams[12],
                    'Precision': '',
                    'Scale': '',
                    'LayerName': listOfParams[13],
                    'SchemaName': listOfParams[14],
                    'DatabaseName': listOfParams[15],
                    'TableName': listOfParams[16],
                    'IsObjectData': listOfParams[18],
                    'IsMap': listOfParams[19],
                    'IsVisible': listOfParams[20],
                    'UpdatedDate': listOfParams[21]
                    }

        # txt = ", ".join([f"{k}={v}" for k, v in cmdDict1.items()])

        txt = str(cmdDict1)
        command = "UPDATE  COLUMN MERGE " + txt
        #log.info(command)
        return command


    def CreateStatemt(self,listOfParams):
        cmdDict1 = {'ConnectionID': listOfParams[0],
                    'ConnLogicName': listOfParams[1],
                    'ToolName': listOfParams[2],
                    'ToolType': listOfParams[22],
                    'DisplayConnectionID':listOfParams[0],
                    'ContainerObjectName': listOfParams[3],
                    'ContainerObjectPath': listOfParams[4],
                    'ContainerObjectType': listOfParams[5],
                    'ControlFlowName': listOfParams[6],
                    'ControlFlowPath': listOfParams[7],
                    'ObjectID': listOfParams[8],
                    'ObjectGUID': listOfParams[9],
                    'ObjectType': listOfParams[10],
                    'ObjectName':listOfParams[11],
                    'DataType':listOfParams[12],
                    'Precision': '',
                    'Scale': '',
                    'LayerName': listOfParams[13],
                    'SchemaName': listOfParams[14],
                    'DatabaseName': listOfParams[15],
                    'TableName': listOfParams[16],
                    'IsObjectData': listOfParams[18],
                    'IsMap': listOfParams[19],
                    'IsVisible': listOfParams[20],
                    'UpdatedDate': listOfParams[21]
                    }

        txt=str(cmdDict1)
        command = "CREATE VERTEX COLUMN CONTENT " +txt
        #log.info(command)
        return command

    def CheckVertexByGuid(self,ObjectGUID):
        command=f"select @rid from COLUMN where ObjectGUID='{ObjectGUID}'"
        rid = self.RunApiPost(command, "POST")
        return rid

    def CheckEdgesByFromRidToRid(self, fromRid,toRid):
        command = f"SELECT @RID FROM DATAFLOW WHERE out ={fromRid} AND in = {toRid}"

        rid = self.RunApiPost(command, "POST")
        return rid


    def CheckInDictionary(self,ObjectGUID,listParams,isIgnore):
        rid="0"
        try :


            result=self.CheckVertexByGuid(ObjectGUID)



            if isIgnore:
                command = self.CreateStatemt(listParams)


                rid = self.RunApiPost(command, "POST")



                self.dictRids[ObjectGUID] = rid
                with open(self.dictFileName, "w") as outfile:
                    outfile.write(json.dumps(self.dictRids))

            elif result=="-1":

                    command = self.CreateStatemt(listParams)


                    rid = self.RunApiPost(command, "POST")


                    self.dictRids[ObjectGUID] = rid
                    with open(self.dictFileName, "w") as outfile:
                        outfile.write(json.dumps(self.dictRids))
            else:
                rid = result
        except Exception as e:
            log.error(e)
        return  rid


    def CheckInDictionaryLinks(self,fromRid,toRid,isIgnore):

        edgeId="0"
        try:

            if isIgnore:
                className='DATAFLOW'
                command = f'DELETE EDGE FROM {fromRid} TO {toRid} where @class= \'{className}\''
                #log.info(command)



                edgeId = self.RunApiPost(command, "POST")


                command = f'CREATE EDGE DATAFLOW FROM {fromRid} TO {toRid}'
                edgeId = self.RunApiPost(command, "POST")


                self.dictEdges[fromRid] = toRid
                with open(self.dictFileNameEdges, "w") as outfile:
                    outfile.write(json.dumps(self.dictEdges))
                return edgeId

            else:
                result=self.CheckEdgesByFromRidToRid(fromRid,toRid)
                if result =="-1":
                    #if self.dictEdges[fromRid] != toRid:


                    command = f'CREATE EDGE DATAFLOW FROM {fromRid} TO {toRid}'
                    edgeId=self.RunApiPost(command, "POST")



                    self.dictEdges[fromRid]=toRid
                    with open(self.dictFileNameEdges, "w") as outfile:
                            outfile.write(json.dumps(self.dictEdges))
                    #else:
                    #return edgeId
                    return edgeId


        except Exception as e:
            log.error(e)











