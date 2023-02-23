import hashlib
import os

import pandas as pd
import requests
import json

from Logger import Logger

log = Logger('Consumer')

class OrientDBApiCross:
    def __init__(self, relationalConnString,databaseName,dictFileName,dictFileNameEdges):
        self.orientURL = relationalConnString
        self.databaseName=databaseName


        self.dictFileName=dictFileName
        self.dictFileNameEdges=dictFileNameEdges
        self.dictRids={}
        self.dictEdges={}


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
            url = f'{self.orientURL}/database/{self.databaseName}/plocal'


            payload = {}
            headers = {
                'Authorization': 'Basic cm9vdDpyb290'

            }

            response = requests.request("POST", url, headers=headers, data=payload)
            if (response.status_code==200):
                if str(databaseName).split('_')[-1]=='E2E':
                    self.RunApiPost("CREATE CLASS COLUMN EXTENDS V","POST")
                    self.RunApiPost("CREATE CLASS DATAFLOW EXTENDS E","POST")
                else:
                    self.RunApiPost("CREATE CLASS LINEAGEOBJECT EXTENDS V","POST")
                    self.RunApiPost("CREATE CLASS LINEAGEFLOW EXTENDS E","POST")
            else:
                log.info(response.text)



            log.info(response.text)
        else:
            log.info(f'Database {databaseName} already exists!')



    def CheckIfDbExists(self):


        url = f'{self.orientURL}/database/{self.databaseName}'

        payload = {}
        headers = {
            'Authorization': 'Basic cm9vdDpyb290'

        }

        response = requests.request("GET", url, headers=headers, data=payload)

        log.info(response.text)
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

        # log.info(response.text)
        x="-1"
        if(response.status_code==200):
            if  len(json.loads(response.text)["result"])==0:
                return x

            elif "@rid" in json.loads(response.text)["result"][0]:
                x= json.loads(response.text)["result"][0]["@rid"]
            else:
                x = json.loads(response.text)["result"][0]

        return x


    def DeleteVertex(self,ObjectID):

        try:


            dictOfRids = self.RunMultipleRows(
                f"Select @rid,ObjectGUID  from  LINEAGEOBJECT where ObjectID='{ObjectID}'",
                "POST")
            for k in dictOfRids:
                self.dictRids.pop(k, None)

            with open(self.dictFileName, "w") as outfile:
                outfile.write(json.dumps(self.dictRids))

            self.RunApiPost(f"DELETE FROM LINEAGEOBJECT where ObjectID='{ObjectID}' unsafe", "POST")
        except  Exception as e:
            log.error(e)



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

        log.info(response.text)
        x="-1"
        if(response.status_code==200):
            x=[ x['ObjectGUID'] for x in json.loads(response.text)["result"]]



        return x

    def CreateVertices(self,df):

        dfSource =df[['ConnectionID','ConnLogicName','ContainerObjectName','ContainerObjectPath','ContainerObjectType',
                     'ContainerToolType','ControlFlowName','ControlFlowPath','SourceDB','SourceSchema','SourceTableName','SourceServer','SourceObjectType','SourceIsObjectData' ]]
        dfSource=dfSource[dfSource['SourceIsObjectData']=="1"]
        dfSource=dfSource.drop_duplicates()
        dfSourceCount=len(dfSource)
        log.info(dfSourceCount)

        # log.info(tabulate(dfSource, headers='keys', tablefmt='psql'))



        dfTarget =df[['ConnectionID','ConnLogicName','ContainerObjectName','ContainerObjectPath','ContainerObjectType',
                     'ContainerToolType','ControlFlowName','ControlFlowPath','TargetDB','TargetSchema','TargetTableName','TargetServer','TargetObjectType','TargetIsObjectData' ]]
        dfTarget=dfTarget[dfTarget['TargetIsObjectData']=="1"]
        dfTarget=dfTarget.drop_duplicates()

        dfTargetCount = len(dfTarget)
        log.info(dfTargetCount)

        # log.info(tabulate(dfTarget, headers='keys', tablefmt='psql'))





        #from table to map
        for index, row in dfSource.iterrows():

            try:

                    ObjectID_1 = str(row["ConnectionID"] + row["ControlFlowPath"])


                    ObjectGUID_1 = hashlib.md5(ObjectID_1.encode('utf_16_le')).hexdigest()

                    #self.DeleteVertex(ObjectID_1)

                    if row["SourceDB"] == '-1':
                        ObjectID_2 = str(row['SourceServer'] + row["SourceDB"] + row["SourceSchema"] + row[
                                "SourceTableName"])
                    else:
                        ObjectID_2 = str(
                            row['SourceServer'] + row["SourceDB"] + row["SourceSchema"] + row[
                                "SourceTableName"])

                    ObjectID_2=ObjectID_2.upper()




                    ObjectGUID_2 = hashlib.md5(ObjectID_2.encode('utf_16_le')).hexdigest()

                    ToolType=""
                    if row["SourceDB"]=='-1':
                        ToolType="FILE"
                    else:
                        ToolType = row["ContainerToolType"]

                    listOfParams1 = [row["ConnectionID"], #0
                                     row["ConnLogicName"],#1
                                     row["ContainerToolType"],  #ToolName #2
                                     'ETL',
                                     ObjectGUID_1,  #ObjectGuid #3
                                     ObjectID_1,  # ObjectId #4
                                     row["ControlFlowPath"],  #ObjectName #5
                                     row["ContainerObjectType"],  #ObjectType #6
                                     row["ContainerObjectPath"],  #Folder #7
                                     row['ContainerObjectName'],  #ObjectContainer #8
                                     row['ContainerObjectName'],  #ProjectName #9
                                     row['ContainerObjectName'],  #Model #10
                                     '',  #DatabaseName # 11
                                     '',  #SchemaName 12
                                     0,  # isFaked, 13
                                     0, #isDb, 14
                                     '',#IsDefinition 15
                                     'ETL',#ModuleType, 16
                                     '' , #ViewType,17
                                     '' , #OrdID,18
                                     str(pd.datetime.datetime.now()), #19
                                     ''# server 20

                                     ]
                    listOfParams2 =[row["ConnectionID"],
                                     row["ConnLogicName"],
                                     ToolType,  #ToolName
                                    'DATABASE',
                                     ObjectGUID_2,  #ObjectGuid
                                     ObjectID_2,  # ObjectId
                                     row["SourceTableName"],  #ObjectName
                                     row["SourceObjectType"],  #ObjectType
                                     row["SourceDB"]+'.'+row["SourceSchema"],  #Folder
                                     row['SourceDB'],  #ObjectContainer
                                     row['SourceDB'],  #ProjectName
                                     row['SourceDB'],  #Model
                                     row['SourceDB'],  #DatabaseName
                                     row["SourceSchema"],  #SchemaName
                                     0,  # isFaked,
                                     1, #isDb,
                                     '',#IsDefinition
                                     'DATABASE',#ModuleType,
                                     '' , #ViewType,
                                     '' , #OrdID,
                                     str(pd.datetime.datetime.now()),
                                    ''

                                     ]




                    #command=self.CreateStatemt(listOfParams1)

                    rid1=self.CheckInDictionary(ObjectGUID_1,listOfParams1,False)

                        #command = self.CreateStatemt(listOfParams3)
                    rid2 = self.CheckInDictionary(ObjectGUID_2, listOfParams2,False)

                        #command = self.CreateStatemt(listOfParams4)


                    #CREATE EDGE DATAFLOW FROM #10:3 TO #11:4
                    # command=f'CREATE DATAFLOW FROM {rid1} TO {rid2}'
                    # self.RunApiPost(command, "POST")
                    edgeId=self.CheckInDictionaryLinks(rid2,rid1,True,"DataFlow")
                    log.info(f' Edge {edgeId} created!')


            except Exception as e:
                   log.error(e)

        # from map to table
        for index, row in dfTarget.iterrows():

            try:


                ObjectID_1 = str(row["ConnectionID"] + row["ControlFlowPath"])
                ObjectGUID_1 = hashlib.md5(ObjectID_1.encode('utf_16_le')).hexdigest()
                #self.DeleteVertex(ObjectID_1)


                if row["TargetDB"] == '-1':
                    ObjectID_2 = str(row['TargetServer'] + row["TargetDB"] + row["TargetSchema"] + row[
                            "TargetTableName"])
                else:
                    ObjectID_2 = str(
                        row['TargetServer'] + row["TargetDB"] + row["TargetSchema"] + row[
                            "TargetTableName"])

                ObjectID_2=ObjectID_2.upper()



                ToolType = ""
                if row["TargetDB"] == '-1':
                    ToolType = "FILE"
                else:
                    ToolType = row["ContainerToolType"]


                ObjectGUID_2 =hashlib.md5(ObjectID_2.encode('utf_16_le')).hexdigest()




                listOfParams1 = [row["ConnectionID"],
                                 row["ConnLogicName"],
                                 row["ContainerToolType"],
                                 'ETL',
                                 #ToolName
                                 ObjectGUID_1,  #ObjectGuid
                                 ObjectID_1,  # ObjectId
                                 row["ControlFlowPath"],  #ObjectName
                                 row["ContainerObjectType"],  #ObjectType
                                 row["ContainerObjectPath"],  #Folder
                                 row['ContainerObjectName'],  #ObjectContainer
                                 row['ContainerObjectName'],  #ProjectName
                                 row['ContainerObjectName'],  #Model
                                 '',  #DatabaseName
                                 '',  #SchemaName
                                 0,  # isFaked,
                                 0, #isDb,
                                 '',#IsDefinition
                                 'ETL',#ModuleType,
                                 '' , #ViewType,
                                 '' , #OrdID,
                                 str(pd.datetime.datetime.now()),
                                 ''

                                 ]

                log.info(listOfParams1)
                listOfParams2 =[row["ConnectionID"],
                                 row["ConnLogicName"],
                                 ToolType,  #ToolName
                                 'DATABASE',
                                 ObjectGUID_2,  #ObjectGuid
                                 ObjectID_2,  # ObjectId
                                 row["TargetTableName"],  #ObjectName
                                 row["TargetObjectType"],  #ObjectType
                                 row["TargetDB"]+'.'+row["TargetSchema"],  #Folder
                                 row['TargetDB'],  #ObjectContainer
                                 row['TargetDB'],  #ProjectName
                                 row['TargetDB'],  #Model
                                 row['TargetDB'],  #DatabaseName
                                 row["TargetSchema"],  #SchemaName
                                 0,  # isFaked,
                                 1, #isDb,
                                 '',#IsDefinition
                                 'DATABASE',#ModuleType,
                                 '' , #ViewType,
                                 '' , #OrdID,
                                 str(pd.datetime.datetime.now()),
                                ''

                                 ]
                # log.info(listOfParams2)




                #command=self.CreateStatemt(listOfParams1)
                rid1=self.CheckInDictionary(ObjectGUID_1,listOfParams1,False)



                    #command = self.CreateStatemt(listOfParams3)
                rid2 = self.CheckInDictionary(ObjectGUID_2, listOfParams2,False)

                    #command = self.CreateStatemt(listOfParams4)


                #CREATE EDGE DATAFLOW FROM #10:3 TO #11:4
                # command=f'CREATE DATAFLOW FROM {rid1} TO {rid2}'
                # self.RunApiPost(command, "POST")
                edgeId=self.CheckInDictionaryLinks(rid1,rid2,True,"DataFlow")
                log.info(f'Edge {edgeId} created!')


            except Exception as e:
                   log.error(e)

    def CreateStatemt(self,listOfParams):

        try:
            cmdDict1 = {'ConnectionID': listOfParams[0],
                        'ConnLogicName': listOfParams[1],
                        'ToolName': listOfParams[2],
                        'ToolType': listOfParams[3],
                        'ObjectGUID':listOfParams[4],
                        'ObjectID': listOfParams[5],
                        'ObjectName': listOfParams[6],
                        'ObjectType': listOfParams[7],
                        'Folder': listOfParams[8],
                        'ObjectContainer': listOfParams[9],
                        'ProjectName': listOfParams[10],
                        'Model': listOfParams[11],
                        'DatabaseName': listOfParams[12],
                        'SchemaName':listOfParams[13],
                        'IsFaked':listOfParams[14],
                        'IsDatabase': listOfParams[15],
                        'Definition': listOfParams[16],
                        'ModuleType': listOfParams[17],
                        'ViewType': listOfParams[18],
                        'OrderInd': listOfParams[19],
                        'UpdatedDate': listOfParams[20],
                        'ServerName': listOfParams[21]

                        }

            txt=str(cmdDict1)
            command = "CREATE VERTEX LINEAGEOBJECT CONTENT " +txt
            #log.info(command)
        except Exception as e:
            log.error(e)
        return command

    def CheckVertexByGuid(self,ObjectGUID):
        command=f"select @rid from LINEAGEOBJECT where ObjectGUID='{ObjectGUID}'"
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


    def CheckEdgesByFromRidToRid(self, fromRid,toRid):
        command = f"SELECT @RID FROM LINEAGEFLOW WHERE out ={fromRid} AND in = {toRid}"

        rid = self.RunApiPost(command, "POST")
        return rid

    def CheckInDictionaryLinks(self,fromRid,toRid,isIgnore,linkType):

        edgeId="0"
        try:


            if isIgnore:
                className = "LINEAGEFLOW"
                command = f'DELETE EDGE FROM {fromRid} TO {toRid} where @class= \'{className}\''
                edgeId = self.RunApiPost(command, "POST")

                command = f'CREATE EDGE LINEAGEFLOW FROM {fromRid} TO {toRid} SET  LinkType = \'{linkType}\''
                edgeId = self.RunApiPost(command, "POST")


                self.dictEdges[fromRid] = toRid
                with open(self.dictFileNameEdges, "w") as outfile:
                    outfile.write(json.dumps(self.dictEdges))
                return edgeId


            else:
                result = self.CheckEdgesByFromRidToRid(fromRid, toRid)
                if result=="-1" :
                # if self.dictEdges[fromRid] != toRid:
                    command = f'CREATE EDGE LINEAGEFLOW FROM {fromRid} TO {toRid} SET  LinkType = \'{linkType}\''
                    edgeId=self.RunApiPost(command, "POST")
                    self.dictEdges[fromRid]=toRid
                    with open(self.dictFileNameEdges, "w") as outfile:
                        outfile.write(json.dumps(self.dictEdges))
                    return edgeId
                # else:
                #     return  edgeId
                # else:
                #     command = f'CREATE EDGE LINEAGEFLOW FROM {fromRid} TO {toRid} SET  LinkType = \'{linkType}\''
                #     edgeId = self.RunApiPost(command, "POST")
                #     self.dictEdges[fromRid] = toRid
                #     with open(self.dictFileNameEdges, "w") as outfile:
                #         outfile.write(json.dumps(self.dictEdges))
                #     return edgeId

        except Exception as e:
            log.error(e)














