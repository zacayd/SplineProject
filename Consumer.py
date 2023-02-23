import hashlib


import  pandas as pd
import numpy as np
from tabulate import tabulate
import requests
import json
from datetime import datetime
from kafka import KafkaConsumer
from OrientApi import OrientDBApi
from OrientDBApiCross import  OrientDBApiCross
from SqlServerApi import SqlServerApi
from Logger import Logger


with open('Config.json', 'r') as f:
    config = json.load(f)


#topic1 = 'spline-topic'
topic1=config['topic']
brokers = config['kafka-broker']




# from Logger import Logger

log = Logger('Consumer')

class Consumer:
 def __init__(self,topicName, consumerUrl, arangoDBUrl,group_id,brokers,orientDB,sqlConnectionString,orientDBName,orientDBNameE2E):
    self.arangoDBUrl = arangoDBUrl
    self.topicName=topicName
    self.brokers=brokers
    self.sqlConnectionString=sqlConnectionString
    self.consumerUrl=consumerUrl
    self.orientDB=orientDB
    self.orientDBName=orientDBName

    self.orientDBNameE2E = orientDBNameE2E
    self.consumer = KafkaConsumer(topicName,bootstrap_servers=brokers)

 def GetMessagesFromTopic(self):
     # lock = threading.Lock()
     log.info(f'Start topic :{self.topicName } reading...')
     for message in self.consumer:
         log.info("Start handling message..")
         listOfMessages = json.loads(message.value)

         listOfExectionPlans=[x["_key"] for x in listOfMessages]
         # log.info(listOfExectionPlans)
         # i = 1

        #concurrent
         # with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
         #     futures = []
         #     i=1
         #     for exc in listOfExectionPlans:
         #         print(datetime.now())
         #         print(f'execution plan :{exc} number:{str(i)} out of {len(listOfExectionPlans)}')
         #         log.info(f"Start Exection plan number:{str(i)}:execId:{exc}...")
         #         log.info(datetime.now())
         #
         #         futures.append(executor.submit(self.AnalyzeExecutionPlan, exc,i,lock))
         #         log.info(f"End Exection plan number:{str(i)}:execId:{exc}")
         #         print(f"End Exection plan number:{str(i)}:execId:{exc}")
         #         i = i + 1
         #
         #     concurrent.futures.wait(futures)
         #     print(f"{str(datetime.now())} End All")



     #Serial
         i=1
         for exc in listOfExectionPlans:

             log.info(f'Start analyzing execution plan: {exc} number:{str(i)} out of {len(listOfExectionPlans)}')
             self.AnalyzeExecutionPlan(exc,i)
             log.info(f'End analyzing execution plan: {exc} number:{str(i)} out of {len(listOfExectionPlans)}')
             i = i + 1



 def AnalyzeExecutionPlan(self, exc,num):

     # print(datetime.now())
     # log.info("Start..")
     # log.info(datetime.now())
     txt = self.GetLineageDetailedForExecPlan(exc, 'lineage-detailed')
     # print("DataTypes:")
     dfDataTypes = self.WriteJsonToDataFrame(txt, 'executionPlan', ['extra', 'dataTypes'], ['_id', 'name'])
     # print("attributes:")
     dfAttributes = self.WriteJsonToDataFrame(txt, 'executionPlan', ['extra', 'attributes'], ['_id', 'name'])
     # print("NotebookData:")
     dfNotebookData = self.WriteJsonToDataFrame(txt, 'executionPlan', None, ['_id', 'name'])
     # print("inputs:")
     # dfInputs=self.WriteJsonToDataFrame(txt, 'executionPlan', ['inputs'], ['_id', 'name'])
     # dfOutput = self.WriteJsonToDataFrame(txt, 'executionPlan', 'output', ['_id', 'name'])
     # print("nodes:")
     dfNodes = self.WriteJsonToDataFrame(txt, 'graph', ['nodes'], None)
     # print("edges:")
     dfEdges = self.WriteJsonToDataFrame(txt, 'graph', ['edges'], None)
     self.CreateSourceToTargetTable \
         (exc, dfDataTypes, dfAttributes, dfNodes, dfEdges, dfNotebookData,num)
     # log.info(f"End Exection plan number:{num}:execId:{exc}")
     # print(f"End Exection plan number:{num}:execId:{exc}")
     # log.info(datetime.now())
     # print(datetime.now())

 def GetPathWithoutLast(self,str, delim):
     ls = str.split(delim)
     ls.pop()
     return delim.join(ls)

 def hash(self,guid: str) -> int:
     return str(int(guid[-2:], 16) % 10 + 1)

 def GetSchema(self,input):

     #dbfs:/mnt/stgods/logs/BulkSMS_JAM_logs
     try:
         # dbfs:/user/hive/warehouse/ods_dl.db/market
         if input=='' or str(input)=='nan':
             return ''
         lst = input.split('/')
         if lst[0] == "dbfs:" and '.db'  in input:
             return lst[-2].split('.')[0]
         else:
             return "-1"
     except Exception as e:
      log.error(e)
      return "-1"

 def GetTable(self,input):
     try:
         if input == '' or str(input) == 'nan':
             return ''
         # dbfs:/user/hive/warehouse/ods_dl.db/market
         #
         lst = input.split('/')
         if (lst[0] == "dbfs:"):
             return lst[-1]
         else:
             return ''
     except Exception as e:
         log.error(e)
         return ''


 def isDropCommand(self,df):
     try:
         df_copy = df.copy()

         df_copy['xname'] = df_copy['xname'].str.upper()

         # get only rows where 'DROP' is present in 'xname' column
         df_copy = df_copy[df_copy['xname'].str.contains('DROP')]

         # count number of rows
         count = df_copy.shape[0]

         return  count>0

     except Exception as e:
         log.error(e)




 def CreateSourceToTargetTable(self,excId, dfDataTypes,dfAttributes,dfNodes,dfEdges,dfNotebookData,num):
     log.info(f"Start execution plan {excId}...")

     if(not self.isDropCommand(dfNodes)):
         try:
             ##remove Drop command from lineage
             log.info(f"Start creating dataframe for  {excId} ...")


             resultTuple = self.CreateDFSourceToTarget(dfAttributes, dfDataTypes, dfEdges, dfNodes,
                                                      dfNotebookData, excId)
             result=resultTuple[-1]
             uq=resultTuple[0]
             log.info(f"End creating dataframe for  {excId}")

         except Exception as e:
           log.error(e)



         # run Inner

         try:

            sqlApi = SqlServerApi(self.sqlConnectionString)
         except Exception as e:
            print(e)

           #  "Data Source=sql.onprem.qa.octopai-corp.local;Initial Catalog=digiceljm_Prod;User ID=oumdb;Password=iAmOum!@")
         #
         log.info(f"Start write execution plan {excId} to source to target table...")
         sqlApi.SourceToTargetLoad(result)
         log.info(f"End  write execution plan {excId} to source to target table")
         # #
         log.info(f"Start write execution plan {excId} to lineageObject table...")
         sqlApi.LoadLineageObject(result)
         log.info(f"End write execution plan {excId} to lineageObject table")

         log.info(f"Start write execution plan {excId} to lineageFlow table...")
         sqlApi.LoadLineageFlow(result)
         log.info(f"End write execution plan {excId} to lineageFlow table")

         log.info(f"Start write execution plan {excId} to Discovery tables...")
         sqlApi.DiscoveryTableLoad(result)
         log.info(f"End write execution plan {excId} to Discovery tables")
         #

         #qa
         # orientApiCross = OrientDBApiCross('http://digiceljm-qa-prod.orient.onprem.qa.octopai-corp.local',
         #                                    "DIGICELJM_PROD", "vertexCross.json",
         #                                     "edgesCross.json")


         #PROD
         orientApiCross = OrientDBApiCross(self.orientDB,
                                            self.orientDBName, "vertexCross.json",
                                             "edgesCross.json")

         log.info(f"Start write execution plan {excId} to Cross Lineage...")
         orientApiCross.CreateVertices(result)
         log.info(f"End write execution plan {excId} to Cross Lineage...")

         #run E2E

         #qa
         # orientApi = OrientDBApi('http://digiceljm-qa-prod.orient.onprem.qa.octopai-corp.local',
         #                         "DIGICELJM_PROD_E2E", "vertex.json", "edges.json",sqlApi)

         #PROD
         orientApi = OrientDBApi(self.orientDB,
                                 self.orientDBNameE2E, "vertex.json", "edges.json", sqlApi)

         # orientApi = OrientDBApi('http://192.168.100.11:2480', "DIGICELJM_PROD_E2E", "vertex.json", "edges.json")
         #print(tabulate(result, headers='keys', tablefmt='psql'))
         log.info(f"Start write execution plan {excId} number {num} to E2E Lineage...")
         orientApi.CreateVertices(result,uq,num)
         log.info(f"End write execution plan {excId} number {num} to E2E Lineage")

         log.info(f"End execution plan {excId}!")
     else:
         log.info("Drop Command was removed from analysis!")














     #result = pd.merge(result, dfReadsFrom, left_on='Source_id', right_on='_from', how='left')[['_to','Source_id','sourceLayerName','SourceObjectType','Target_id','TargetLayerName','TargetObjectType']]

     #[['Source_id','sourceLayerName','SourceObjectType','Target_id','TargetLayerName','TargetObjectType']


     #df4 = pd.merge(pd.merge(dfEdges, dfNodes, on='Courses'), df3, on='Courses')

 def CreateDFSourceToTarget(self, dfAttributes, dfDataTypes, dfEdges, dfNodes, dfNotebookData, excId):
     result = pd.merge(dfEdges, dfNodes, left_on='xsource', right_on='x_id', how='left')[
         ['xsource', 'xtarget', 'xname', 'x_type']]
     result.rename(columns={'xsource': 'Source_id', 'xtarget': 'Target_id', 'xname': 'sourceLayerName',
                            'x_type': 'SourceObjectType'}, inplace=True)
     result = pd.merge(result, dfNodes, left_on='Target_id', right_on='x_id', how='left')[
         ['Source_id', 'Target_id', 'sourceLayerName', 'SourceObjectType', 'xname', 'x_type']]
     result.rename(columns={'xname': 'TargetLayerName', 'x_type': 'TargetObjectType'}, inplace=True)
     result["Source_id_op"] = 'operation/' + result["Source_id"].astype(str)
     result["Target_id_op"] = 'operation/' + result["Target_id"].astype(str)
     # if (len(dfReadsFrom) == 0):
     #     result['sourceDatasourceID'] = ''
     # else:
     #     result = pd.merge(result, dfReadsFrom, left_on='Source_id_op', right_on='_from', how='left')[
     #         ['Source_id', 'sourceLayerName', 'SourceObjectType', 'Target_id', 'Source_id_op', 'Target_id_op',
     #          'TargetLayerName', 'TargetObjectType', '_to']].rename(columns={'_to': 'sourceDatasourceID'})
     # result = pd.merge(result, dfDataSource, left_on='sourceDatasourceID', right_on='_id', how='left')[
     #     ['Source_id', 'sourceLayerName', 'SourceObjectType', 'Target_id', 'TargetLayerName', 'TargetObjectType', 'uri',
     #      'Source_id_op', 'Target_id_op']].rename(columns={'uri': 'SourceTableName'})
     # result = pd.merge(result, dfWritesTo, left_on='Target_id_op', right_on='_from', how='left')[
     #     ['Source_id', 'sourceLayerName', 'SourceObjectType', 'Target_id',
     #      'TargetLayerName', 'TargetObjectType', 'SourceTableName', '_to']].rename(
     #     columns={'_to': 'targetDatasourceID'})
     # result = pd.merge(result, dfDataSource, left_on='targetDatasourceID', right_on='_id', how='left')[
     #     ['Source_id', 'sourceLayerName', 'SourceObjectType', 'SourceTableName', 'Target_id',
     #      'TargetLayerName', 'TargetObjectType', 'uri']].rename(
     #     columns={'uri': 'TargetTableName'})
     result['ControlFlowPath'] = 'executionPlan/' + excId
     cols = self.GetColsForOps(dfNodes, excId)
     childs = self.GetChilds(dfNodes, excId)
     tbls=self.GetTableDf(dfNodes,excId)

     try:

         result=pd.merge(result,tbls,left_on='Source_id',right_on='opid',how='left')
         # [
         #     'Source_id', 'sourceLayerName', 'SourceObjectType', 'SourceTableName', 'Target_id',
         #     'TargetLayerName', 'TargetObjectType','TargetTableName','table','db','type'
         # ].rename(
         #     columns={'table': 'SourceTableNameT','db':'SourceDBT','type':'SourceObjectTypeT'
         #              })
         result=result[['ControlFlowPath','Source_id', 'sourceLayerName', 'SourceObjectType', 'table','db','type', 'Target_id','TargetLayerName',
                        'TargetObjectType',
                        ]].\
             rename(columns={'table': 'SourceTableName','db':'SourceDB','type':'SourceObjectTypeT'})

     except Exception as e:
        log.error(e)

     try:
         result = pd.merge(result, tbls, left_on='Target_id', right_on='opid', how='left')

         result = result[['ControlFlowPath','Source_id', 'sourceLayerName', 'SourceTableName','SourceObjectTypeT','SourceDB',
                          'SourceObjectType','Target_id','TargetLayerName',
                          'TargetObjectType',
                          'table', 'db', 'type',
                           ]]. \
             rename(columns={'table': 'TargetTableName', 'db': 'TargetDB', 'type': 'TargetObjectTypeT'})


     except Exception as e:
         log.error(e)

     # log.info(tabulate(cols, headers='keys', tablefmt='psql'))
     # log.info(tabulate(childs, headers='keys', tablefmt='psql'))
     if (len(childs) > 0):
         try:
             sttChilds = pd.merge(childs, cols, left_on=['opId', 'alias'],
                                  right_on=['operationId', 'name'], how='inner')

             sttChilds = sttChilds[['execId_y', 'opId', 'child.refId', 'id', 'child.name', 'name_y']].rename(
                 columns={'execId_y': 'execId',
                          'child.refId': 'SourceColumnId',
                          'id': 'TargetColumnId',
                          'child.name': 'SourceColumnName',
                          'name_y': 'TargetColumnName'

                          })
         except Exception as e:
             log.error(e)
     result = self.HandelLinks(cols, result, sttChilds)
     dfAll = result
     # to remove it
     # myRes=result
     # if(len(dfUses)>0):
     #    dfUses["_from"]=dfUses['_from'].apply(lambda x: x.replace("operation/",""))
     #    result = pd.merge(result, dfUses, left_on=['ControlFlowPath','Source_id'], right_on=['_belongsTo','_from'], how='inner')
     #    result = result[['Source_id', 'sourceLayerName', 'SourceObjectType', 'SourceTableName', 'Target_id',
     #                     'TargetLayerName', 'TargetObjectType', 'TargetTableName', 'ControlFlowPath', '_to'
     #                     ]].rename(columns={'_to': 'SourceColumnId'})
     # else:
     #     result['SourceColumnId']=''
     #
     # dfAll, result = self.ExpressionLineage(dfAttributes, dfTblLevel, dfderivesFrom, result,dfUses,dfProduces,myRes)

     try:
        dfAllSourceNames = pd.merge(dfAll, dfAttributes, left_on='SourceColumnId', right_on='xid', how='left') \
         .rename(columns={'xname': 'SourceColumn', 'xdataTypeId': 'SourceDataTypeId'})
     except Exception as e:
         log.error(e)

     dfAllTargetNames = pd.merge(dfAllSourceNames, dfAttributes, left_on='TargetColumnId', right_on='xid', how='left') \
         .rename(columns={'xname': 'TargetColumn', 'xdataTypeId': 'TargetDataTypeId'})
     dfAllSourceDt = pd.merge(dfAllTargetNames, dfDataTypes, left_on='SourceDataTypeId', right_on='xid', how='left') \
         .rename(columns={'xname': 'SourceDataType'})
     dfAllTargetDt = pd.merge(dfAllSourceDt, dfDataTypes, left_on='TargetDataTypeId', right_on='xid', how='left') \
         .rename(columns={'xname': 'TargetDataType'})
     # ControlFlowPath | Source_id | sourceLayerName | SourceObjectType | SourceTableName | SourceColumnId | Target_id | TargetLayerName | TargetObjectType | TargetTableName | TargetColumnId | xid_x | SourceColumnName | SourceDataTypeId | _id_x | name_x | xid_y | TargetColumnName | TargetDataTypeId | _id_y | name_y | x_typeHint_x | xid_x | SourceDataType | xnullable_x | _id_x | name_x | x_typeHint_y | xid_y | TargetDataType | xnullable_y | _id_y | name_y |

     try:
      dfAllTargetDt = dfAllTargetDt[
         ['ControlFlowPath', 'Source_id', 'sourceLayerName', 'SourceObjectType','SourceObjectTypeT', 'SourceTableName','SourceDB',
          'SourceColumnId', 'SourceColumn', 'SourceDataType',
          'Target_id', 'TargetLayerName', 'TargetObjectType','TargetObjectTypeT', 'TargetTableName','TargetDB',
          'TargetColumnId', 'TargetColumn', 'TargetDataType']]

     except Exception as e:
        log.error(e)
     result = dfAllTargetDt
     #
     #
     result["TargetObjectType"] = result["TargetLayerName"]
     result["SourceObjectType"] = result["sourceLayerName"]
     result['SourceIsObjectData'] = np.where(result["SourceObjectType"].isin(
         ['Project', 'SubqueryAlias', 'View', 'Filter', 'Distinct', 'Join', 'Aggregate']), '0', '1')
     result['TargetIsObjectData'] = np.where(result["TargetObjectType"].isin(
         ['Project', 'SubqueryAlias', 'View', 'Filter', 'Distinct', 'Join', 'Aggregate']), '0', '1')
     result["SourceTableName"] = np.where(result["SourceIsObjectData"] == "0", '', result["SourceTableName"])
     result["TargetTableName"] = np.where(result["TargetIsObjectData"] == "0", '', result["TargetTableName"])
     result["SourceIsObjectData"] = np.where(result["SourceTableName"].isna(), "0", result["SourceIsObjectData"])
     #
     #
     result['SourceOp'] = result['Source_id'].apply(lambda x: x.split(':')[-1])
     result['sourceLayerName'] = result['sourceLayerName'] + '_' + result['SourceOp']
     result['TargetOp'] = result['Target_id'].apply(lambda x: x.split(':')[-1])
     #
     #
     #
     result['TargetLayerName'] = result['TargetLayerName'] + '_' + result['TargetOp']
     result["execId"] = excId
     # result = pd.merge(result, dfInputs, left_on='execId', right_on='_id',
     #                          how='left') \
     #     .rename(columns={'xsourceType': 'SourceObjectTypeT'})
     #
     # result = pd.merge(result, dfOutput, left_on='execId', right_on='executionPlan._id',
     #                   how='left') \
     #     .rename(columns={'executionPlan.output.sourceType': 'TargetObjectTypeT'})
     # result['SourceSchema'] = result['SourceObjectTypeT'].apply(lambda x: '-1' if str(x) in ['parquet','csv']  else '')
     # result['SourceDB'] = result['SourceObjectTypeT'].apply(lambda x: '-1' if str(x) in ['parquet','csv']  else '')
     # result['TargetSchema'] = result['TargetObjectTypeT'].apply(lambda x: '-1' if str(x) in ['parquet','csv'] else '')
     # result['TargetDB'] = result['TargetObjectTypeT'].apply(lambda x: '-1' if str(x) in ['parquet','csv']  else '')
     #
     #
     # result['SourceSchema'] =  np.where((result["SourceObjectTypeT"] =='csv') | (result["SourceObjectTypeT"]) =='parquet', '-1', 'Schema')

     #----


     #result['SourceSchema'] = result['SourceTableName'].apply(lambda x: self.GetSchema(x))

     try:

         result['SourceSchema'] = result.apply(
             lambda x: x["SourceDB"] if x["SourceObjectTypeT"] == "delta" and x["SourceIsObjectData"] == '1' else -1 if
             x["SourceObjectTypeT"] != "delta" and x["SourceIsObjectData"] == '1' else "", axis=1)

         # result['SourceSchema'] =np.where( (result['SourceObjectTypeT'] == "delta" & result['SourceIsObjectData']=='1'), result["SourceDB"],"")
         # result['SourceSchema'] = np.where((result['SourceObjectTypeT'] != "delta" & result['SourceIsObjectData'] == '1'),
         #                                  "-1", "")


         result['SourceDB'] = result['SourceSchema']
        # result['TargetSchema'] = result['TargetTableName'].apply(lambda x: self.GetSchema(x))
         #result['TargetSchema']=np.where( (result['TargetObjectTypeT'] == "delta" & result['TargetIsObjectData']=='1'), result["TargetDB"], "")

         result['TargetSchema'] = result.apply(
             lambda x: x["TargetDB"] if x["TargetObjectTypeT"] == "delta" and x["TargetIsObjectData"] == '1' else -1 if
             x["TargetObjectTypeT"] != "delta" and x["TargetIsObjectData"] == '1' else "", axis=1)



         result['TargetDB'] = result['TargetSchema']

     except Exception as e:
       log.error(e)
         # result['SourceSchema'] = np.where(result['SourceSchema'] == "-1", "-1",
     #                                   result['SourceTableName'].apply(lambda x: self.GetSchema(x)))
     # result['SourceDB'] = np.where(result['SourceSchema'] == "-1", "-1",
     #                               result['SourceTableName'].apply(lambda x: self.GetSchema(x)))
     # result['TargetSchema'] = np.where(result['TargetSchema'] == "-1", "-1",
     #                                   result['TargetTableName'].apply(lambda x: self.GetSchema(x)))
     # result['TargetDB'] = np.where(result['TargetSchema'] == "-1", "-1",
     #                               result['TargetTableName'].apply(lambda x: self.GetSchema(x)))
     # result['TargetTableName'].apply(lambda x: self.GetSchema(x))
     result["SourceSchema"] = np.where(result["SourceIsObjectData"] == "0", '', result["SourceSchema"])
     result["SourceDB"] = np.where(result["SourceIsObjectData"] == "0", '', result["SourceDB"])
     result["SourceTableName"] = np.where(result["SourceIsObjectData"] == "0", '', result["SourceTableName"])
     result["TargetSchema"] = np.where(result["TargetIsObjectData"] == "0", '', result["TargetSchema"])
     result["TargetDB"] = np.where(result["TargetIsObjectData"] == "0", '', result["TargetDB"])
     result["TargetTableName"] = np.where(result["TargetIsObjectData"] == "0", '', result["TargetTableName"])
     # try:
     #     result['SourceTableName'] = np.where(result['SourceSchema'] == "-1", result['SourceTableName'],
     #                                          result['SourceTableName'].apply(lambda x: self.GetTable(x)))
     #
     #     result['TargetTableName'] = np.where(result['TargetSchema'] == "-1", result['TargetTableName'],
     #                                          result['TargetTableName'].apply(lambda x: self.GetTable(x)))
     # except Exception as e:
     #     log.error(e)
     result["ConnectionID"] = config['ConnectionID']  # from topic conn xml
     result["ConnLogicName"] = "Databricks"  # from topic conn xml
     result["SourceServer"] = ""
     result["TargetServer"] = ""
     result["ContainerObjectPath"] = result['ControlFlowPath']
     result["ContainerObjectName"] = result['ControlFlowPath']
     result["ContainerObjectType"] = 'Notebook'
     result["ContainerToolType"] = 'DATABRICKS'
     #
     result["ControlFlowName"] = result['ControlFlowPath']
     result["ControlFlowPath"] = result['ControlFlowPath']
     log.info('dfNotebookData:')
     dfNotebookData['id'] = 'executionPlan/' + dfNotebookData['_id']
     try:

         result = pd.merge(result, dfNotebookData, left_on='ControlFlowPath', right_on='id', how='left')[
             ['ConnectionID',
              'ConnLogicName',
              'ContainerObjectName',
              'ContainerObjectPath',
              'ContainerObjectType',
              'ContainerToolType',
              'ControlFlowName',
              'ControlFlowPath',
              'Source_id',
              'sourceLayerName',
              'SourceSchema',
              'SourceDB',
              'SourceTableName',
              'SourceColumn',
              'SourceColumnId',
              'SourceDataType',
              'SourceObjectType',
              'Target_id',
              'TargetLayerName',
              'TargetSchema',
              'TargetDB',
              'TargetTableName',
              'TargetColumn',
              'TargetColumnId',
              'TargetDataType',
              'TargetObjectType',
              'SourceIsObjectData',
              'TargetIsObjectData',
              'SourceServer',
              'TargetServer',
              'extra.notebookInfo.obj.name',
              'extra.notebookInfo.obj.workspaceName',
              #remenebr to move them back
              # 'extra.notebookInfo.obj.notebookURL',
              # 'extra.notebookInfo.obj.workspaceName',
              # 'extra.notebookInfo.obj.workspaceUrl'

              ]].rename(
             columns={'extra.notebookInfo.obj.name': 'NotebookName',
                      'extra.notebookInfo.obj.workspaceName': 'workspacepath',
                      # 'extra.notebookInfo.obj.notebookURL': 'notebookURL',
                      # 'extra.notebookInfo.obj.workspaceUrl': 'workspaceUrl'
                      }

         )
         # remember to remove this
         result['notebookURL']=result['workspacepath']
         result['workspaceUrl'] = result['workspacepath']


         # result['ContainerObjectName']=result['workspacepath'].apply(lambda x: x.split('/')[-2])
         # result['ContainerObjectName'] = 'https://' + result['workspaceUrl'] + '/'
         #
         # result['ContainerObjectPath'] = 'https://' + result['workspaceUrl'] + '/'

         result['ContainerObjectName'] =result['workspacepath']
         result['ContainerObjectPath'] = result['workspacepath']

         # result['ContainerObjectPath'] = result['workspacepath']
         result['ControlFlowName'] = result['NotebookName']
         result['ControlFlowPath'] = result['NotebookName']

         result['SourceObjectType'] = np.where(result["SourceDB"] == "-1", 'FILE', result["SourceObjectType"])

         result['TargetObjectType'] = np.where(result["TargetDB"] == "-1", 'FILE', result["TargetObjectType"])

         result['SourceObjectType'] = np.where((result['SourceObjectType'] != 'FILE') &
                                               (result['SourceIsObjectData'] == '1'), 'Table',
                                               result['SourceObjectType'])

         result['TargetObjectType'] = np.where((result['TargetDB'] != '-1') &
                                               (result['TargetIsObjectData'] == '1'), 'Table',
                                               result['TargetObjectType'])

         uqExecId = self.GetUniqueIDForExecutionPlan(result)[0:8]

         result["sourceLayerName"] = result["sourceLayerName"] + '_' + uqExecId
         result["TargetLayerName"] = result["TargetLayerName"] + '_' + uqExecId







     except Exception as e:
         # print(e)
         log.error(e)
     #

     try:
         result = result[
             ['ConnectionID',
              'ConnLogicName',
              'ContainerObjectName',
              'ContainerObjectPath',
              'ContainerObjectType',
              'ContainerToolType',
              'ControlFlowName',
              'ControlFlowPath',
              'Source_id',
              'sourceLayerName',
              'SourceSchema',
              'SourceDB',
              'SourceTableName',
              'SourceColumn',
              'SourceColumnId',
              'SourceDataType',
              'SourceObjectType',
              'Target_id',
              'TargetLayerName',
              'TargetSchema',
              'TargetDB',
              'TargetTableName',
              'TargetColumn',
              'TargetColumnId',
              'TargetDataType',
              'TargetObjectType',
              'SourceIsObjectData',
              'TargetIsObjectData',
              'SourceServer',
              'TargetServer'
              ]]
     except Exception as e:
         log.error(e)
     # print("Result:")
     # log.info("Result:")
     # print(len(result))
     # log.info(len(result))
     # result=result[((result["SourceColumn"] == "alt") |(result["SourceColumn"]=='CXX') )]
     # print(tabulate(result, headers='keys', tablefmt='psql'))
     # log.info(tabulate(result, headers='keys', tablefmt='psql'))
     return (uqExecId, result)

 def HandelLinks(self, cols, result, sttChilds):
     #print('cols:')
     # log.info('cols:')
     #print(tabulate(cols, headers='keys', tablefmt='psql'))
     #log.info(tabulate(cols, headers='keys', tablefmt='psql'))
     #print('result:')
     #print(tabulate(result, headers='keys', tablefmt='psql'))
     # log.info('result:')
     #log.info(tabulate(cols, headers='keys', tablefmt='psql'))

     # log.info('sttChilds:')
     #log.info(tabulate(sttChilds, headers='keys', tablefmt='psql'))
     cols['execId'] = 'executionPlan/' + cols['execId']
     sttChilds['execId'] = 'executionPlan/' + sttChilds['execId']
     resultWithcols = pd.merge(result, cols, left_on=['Source_id', 'ControlFlowPath'],
                               right_on=['operationId', 'execId'], how='inner')
     resultWithcols = resultWithcols[
         ['ControlFlowPath', 'Source_id', 'sourceLayerName', 'SourceObjectType','SourceObjectTypeT', 'SourceTableName','SourceDB',
          'id',
          'Target_id', 'TargetLayerName', 'TargetObjectType','TargetObjectTypeT',  'TargetTableName','TargetDB'
          ]].rename(columns={'id': 'SourceColumnId'})
     resultWithcols['TargetColumnId'] = resultWithcols['SourceColumnId']
     sttChilds = pd.merge(sttChilds, result, left_on=['execId', 'opId'], right_on=['ControlFlowPath', 'Target_id'],
                          how='inner')
     sttChilds = sttChilds[
         ['ControlFlowPath', 'Source_id', 'sourceLayerName', 'SourceObjectType','SourceObjectTypeT', 'SourceTableName','SourceDB', 'SourceColumnId',
          'opId', 'TargetLayerName', 'TargetObjectType','TargetObjectTypeT', 'TargetTableName','TargetDB',
          'TargetColumnId'

          ]].rename(columns={'opId': 'Target_id'})

     # log.info(tabulate(resultWithcols, headers='keys', tablefmt='psql'))
     # log.info(tabulate(sttChilds, headers='keys', tablefmt='psql'))
     # remove rows from resultWithcols that exist on sttChilds
     # sttChildsAgg = sttChilds[['ControlFlowPath', 'Source_id', 'Target_id']].drop_duplicates()



     sttChildsKeys=sttChilds[['ControlFlowPath','Source_id', 'Target_id','SourceColumnId']].rename( columns={'ControlFlowPath':'Map',
                                                                                                    'Source_id':'SourceOp',
                                                                                                    'Target_id':'TargetOp',
                                                                                                    'SourceColumnId':'SourceCol'
                                                                                                    })

     resultWithcols = pd.merge(resultWithcols, sttChildsKeys,
                               left_on=['ControlFlowPath', 'Source_id', 'Target_id', 'SourceColumnId'],
                               right_on=['Map', 'SourceOp', 'TargetOp', 'SourceCol'],
                               how='left')


     resultWithcols = resultWithcols[
         ['ControlFlowPath', 'Source_id', 'sourceLayerName', 'SourceObjectType','SourceObjectTypeT', 'SourceTableName','SourceDB', 'SourceColumnId',
          'Target_id', 'TargetLayerName', 'TargetObjectType','TargetObjectTypeT', 'TargetTableName','TargetDB',
          'TargetColumnId','Map']]

     try:

         resultWithcolsX = resultWithcols[resultWithcols['Map'].isnull()]
         resultWithcolsX = resultWithcolsX[
             ['ControlFlowPath', 'Source_id', 'sourceLayerName', 'SourceObjectType','SourceObjectTypeT', 'SourceTableName','SourceDB', 'SourceColumnId',
              'Target_id', 'TargetLayerName', 'TargetObjectType', 'TargetObjectTypeT', 'TargetTableName','TargetDB',
              'TargetColumnId']]
         #log.info(tabulate(resultWithcolsX, headers='keys', tablefmt='psql'))
        # log.info(tabulate(sttChilds, headers='keys', tablefmt='psql'))
         result = pd.concat([resultWithcolsX, sttChilds])
         #log.info(tabulate(result, headers='keys', tablefmt='psql'))

     except Exception as e:
         log.error(e)
     return result

 #
 def GetTableDf(self,dfNodes,execId):
    list=[]
    for index, row in dfNodes.iterrows():
         opId = row['x_id']
         txt = self.GetLineageDetailedForOperationID(opId, 'operations')

         data = json.loads(txt)
         if data['operation']['_type']=='Read':
             if  data['operation']['properties']['sourceType']=='delta':
                if 'table' in  data['operation']['properties']:
                     table=data['operation']['properties']['table']['identifier']['table']
                     db=data['operation']['properties']['table']['identifier']['database']
                     type = data['operation']['properties']['sourceType']
                     list.append({'opid':opId ,'table':table,'db':db,'type':type})
         elif data['operation']['_type']=='Write':
             if data['operation']['properties']['destinationType'] == 'delta':
                 if 'table' in data['operation']['properties']:
                     identifier=data['operation']['properties']['table']['identifier']
                     table=identifier.split('.')[-1]
                     db = identifier.split('.')[0]
                     type=data['operation']['properties']['destinationType']
                     list.append({'opid': opId, 'table': table, 'db': db, 'type': type})

    df = pd.DataFrame(list)
    return df







 def GetChilds(self, dfNodes, excId):
     listOfdf = []
     for index, row in dfNodes.iterrows():
         opId = row['x_id']
         if opId=="d541121d-21c1-579e-bffe-ae0afbcb7a36:op-4":
             log.info('Test')

         # log.info(f'Get properties for operation{opId}')
         txt = self.GetLineageDetailedForOperationID(opId, 'operations')
         try:
             data = json.loads(txt)['operation']['properties']
             if 'projectList' in data or 'aggregateExpressions' in data:
                     if 'projectList' in data:
                         df = pd.json_normalize(data, record_path=['projectList'])
                     else:
                         df = pd.json_normalize(data, record_path=['aggregateExpressions'])

                     df["execId"]=excId
                     #log.info(tabulate(df, headers='keys', tablefmt='psql'))
                     df["opId"] = opId
                     #log.info(tabulate(df, headers='keys', tablefmt='psql'))
                     if 'child.refId' in df.columns:
                             df = df[df['child.refId'].notnull()]
                             listOfdf.append(df)
                     elif 'child.children' in df.columns:
                         # log.info('Handle children:')
                         df=df[df['child.children'].notnull()]
                         for index, row in df.iterrows():
                             # if index==21:
                                 # log.info('d')
                             dct=row['child.children'][0]

                             data_list=[]
                             myDict=self.extract_data(dct,data_list)
                             if len(myDict)==0:
                                 myDict=dct
                                 myDf = pd.DataFrame(myDict,index=[0, 1,2,3])
                                 if 'refId' in myDf.columns:
                                     myDf=myDf[['name','refId','_typeHint','dataTypeId']].rename(columns={'name': 'child.name', 'refId': 'child.refId','_typeHint':'child._typeHint','dataTypeId':'child.dataTypeId'})
                                     myDf['refId']=myDf['child.refId']
                                 else:
                                     myDf['child.refId'] = row['refId']
                                     myDf['refId'] = row['refId']
                                     myDf = myDf[['name', '_typeHint', 'dataTypeId','child.refId']].rename(columns={'name': 'child.name',
                                                  '_typeHint': 'child._typeHint', 'dataTypeId': 'child.dataTypeId'})




                                 myDf['execId'] = excId
                                 myDf['opId'] = opId
                                 myDf['alias']=row['alias']
                                 myDf['name']=myDf['child.name']
                                 myDf['dataTypeId']=myDf['child.dataTypeId']
                                 myDf['_typeHint'] = myDf['child._typeHint']

                                 myDf = myDf.drop_duplicates()
                                 try:
                                    myDf=myDf[['name','_typeHint','alias','dataTypeId','child._typeHint','child.dataTypeId','child.name','child.refId','refId','execId','opId']]
                                 except Exception as e:
                                  log.error(e)

                                 #log.info(tabulate(myDf, headers='keys', tablefmt='psql'))
                                 listOfdf.append(myDf)

                             else:
                             #col:{'name': 'child.name', 'refid': 'child.refId', })
                                 myDf = pd.DataFrame(myDict)
                                 myDf["child._typeHint"] = myDf["_typeHint"]
                                 myDf["child.dataTypeId"] = myDf["dataTypeId"]
                                 myDf["child.name"] = myDf["name"]
                                 myDf["child.refId"] = myDf["refId"]
                                 myDf["refId"]=row["refId"]
                                 myDf["execId"] = excId
                                 myDf["opId"] = opId
                                 myDf["alias"]=row["alias"]
                                 myDf = myDf.drop_duplicates()

                                 try:
                                     myDf = myDf[
                                         ['name', '_typeHint', 'alias', 'dataTypeId', 'child._typeHint', 'child.dataTypeId',
                                          'child.name', 'child.refId', 'refId', 'execId', 'opId']]
                                 except Exception as e:
                                     log.error(e)
                                 #log.info(tabulate(myDf, headers='keys', tablefmt='psql'))
                                 listOfdf.append(myDf)





         except Exception as e:
                     log.error(e)
     dfColumnsPerOp = pd.concat(listOfdf)
     dfColumnsPerOp = dfColumnsPerOp.drop_duplicates()
     # log.info('end')
     return dfColumnsPerOp

 def extract_data(self,d, data_list):
     for child in d.get("children", []):
         if "_typeHint" in child and child["_typeHint"] == "expr.AttrRef":
             data = {
                 "refId": child["refId"],
                 "name": child["name"],
                 "dataTypeId": child["dataTypeId"],
                 "_typeHint": child["_typeHint"],
             }
             data_list.append(data)
         self.extract_data(child, data_list)
     return data_list

 def GetColsForOps(self, dfNodes, excId):
     listOfdf = []
     for index, row in dfNodes.iterrows():
         opId = row['x_id']
         # log.info(f'Get properties for operation{opId}')
         txt = self.GetLineageDetailedForOperationID(opId, 'operations')
         try:
             data = json.loads(txt)
             data = data['schemas']
             dataDict = {'schemas': data[-1]}
             #log.info(dataDict)
             df = pd.json_normalize(dataDict, record_path='schemas')
             #log.info(tabulate(df, headers='keys', tablefmt='psql'))
             dfSchema = df
             dfSchema["operationId"] = opId
             dfSchema["execId"] = excId
             dfSchema = dfSchema[['id', 'name', 'dataTypeId', 'operationId', 'execId']]
             listOfdf.append(dfSchema)





         except Exception as e:
             log.error(e)
     dfColumnsPerOp = pd.concat(listOfdf)
     #dfColumnsPerOp = dfColumnsPerOp.drop(df.columns[0], axis=1)
     dfColumnsPerOp = dfColumnsPerOp.drop_duplicates()
     # log.info('end')
     return dfColumnsPerOp


 def ExpressionLineage(self, dfAttributes, dfTblLevel, dfderivesFrom, result,dfUses,dfProduces,myRes):

     if (len(dfderivesFrom) > 0):
         result = pd.merge(result, dfderivesFrom, left_on=['ControlFlowPath', 'SourceColumnId'],
                           right_on=['_belongsTo', '_from'], how='inner')
     result = result[['ControlFlowPath', 'Source_id', 'sourceLayerName', 'SourceObjectType', 'SourceTableName',
                      'Target_id', 'TargetLayerName', 'TargetObjectType', 'TargetTableName', 'SourceColumnId']]
     result['TargetColumnId'] = result['SourceColumnId']
     result = result[
         ['ControlFlowPath', 'Source_id', 'sourceLayerName', 'SourceObjectType', 'SourceTableName', 'SourceColumnId',
          'Target_id', 'TargetLayerName', 'TargetObjectType', 'TargetTableName', 'TargetColumnId']]
     dfFinalRow = result
     if (len(dfFinalRow)>0):
         dfFinalRow['SourceColumnId'] = dfFinalRow['SourceColumnId'].apply(lambda x: x.replace('attribute/', ''))
         dfFinalRow['TargetColumnId'] = dfFinalRow['TargetColumnId'].apply(lambda x: x.replace('attribute/', ''))
         if (len(dfderivesFrom) > 0):
             dfderivesFrom['_from'] = dfderivesFrom['_from'].apply(lambda x: x.replace('attribute/', ''))
             # final Row
             dfMiddleRow = pd.merge(dfFinalRow, dfderivesFrom, left_on=['ControlFlowPath', 'SourceColumnId'],
                                    right_on=['_belongsTo', '_from'], how='inner')

             dfMiddleRow = dfMiddleRow[
                 ['ControlFlowPath', 'Source_id', 'sourceLayerName', 'SourceObjectType', 'SourceTableName',
                  'Target_id', 'TargetLayerName', 'TargetObjectType', 'TargetTableName', 'SourceColumnId',
                  '_to']].rename(columns={'SourceColumnId': 'TargetColumnId', '_to': 'SourceColumnId'})

             dfMiddleRowColumn = pd.merge(dfMiddleRow, dfTblLevel, left_on='Source_id',
                                          right_on='Target_id', how='inner')

             dfMiddleRowColumn = dfMiddleRowColumn[
                 ['ControlFlowPath_x', 'Source_id_y', 'sourceLayerName_y', 'SourceObjectType_y',
                  'SourceTableName_y', 'SourceColumnId', 'Source_id_x', 'sourceLayerName_x', 'SourceObjectType_x',
                  'SourceTableName_x', 'TargetColumnId']].rename(columns={'ControlFlowPath_x': 'ControlFlowPath',
                                                                          'Source_id_y': 'Source_id',
                                                                          'sourceLayerName_y': 'sourceLayerName',
                                                                          'SourceObjectType_y': 'SourceObjectType',
                                                                          'SourceTableName_y': 'SourceTableName',
                                                                          'Source_id_x': 'Target_id',
                                                                          'sourceLayerName_x': 'TargetLayerName',
                                                                          'SourceObjectType_x': 'TargetObjectType',
                                                                          'SourceTableName_x':
                                                                              'TargetTableName'
                                                                          })

             dfMiddleRowColumn['SourceColumnId'] = dfMiddleRowColumn['SourceColumnId'].apply(
                 lambda x: x.replace('attribute/', ''))
             dfMiddleRowColumn['TargetColumnId'] = dfMiddleRowColumn['TargetColumnId'].apply(
                 lambda x: x.replace('attribute/', ''))
         ##middle row
         dfAttributes['_id'] = dfAttributes['_id'].apply(lambda x: 'executionPlan/' + x)
         #
         result = pd.merge(dfTblLevel, dfAttributes, left_on='ControlFlowPath', right_on='_id', how='left')
         result = result[
             ['ControlFlowPath', 'Source_id', 'sourceLayerName', 'SourceObjectType', 'SourceTableName', 'Target_id',
              'TargetLayerName', 'TargetObjectType', 'TargetTableName', 'xname', 'xid', 'xdataTypeId']].rename(
             columns={'xname': 'SourceColumn', 'xid': 'SourceColumnId'})
         if (len(dfderivesFrom) > 0):
             dfderivesFrom['_from'] = dfderivesFrom['_from'].apply(lambda x: x.replace('attribute/', ''))
             result = pd.merge(result, dfderivesFrom, left_on='SourceColumnId', right_on='_from', how='left')
             result = result[
                 ['ControlFlowPath', 'Source_id', 'sourceLayerName', 'SourceObjectType', 'SourceTableName', 'Target_id',
                  'TargetLayerName', 'TargetObjectType', 'TargetTableName', 'SourceColumn', 'SourceColumnId', 'xdataTypeId',
                  '_key']]
             result = result[result['_key'].isna()]
         result['TargetColumnId'] = result['SourceColumnId']
         dfRestColumns = result[
             ['ControlFlowPath', 'Source_id', 'sourceLayerName', 'SourceObjectType', 'SourceTableName', 'SourceColumnId',
              'Target_id', 'TargetLayerName', 'TargetObjectType', 'TargetTableName', 'TargetColumnId'
              ]]
     else:
         log.info('In case of ')
         # log.info(tabulate(myRes, headers='keys', tablefmt='psql'))
         #dfUses, dfProduces
         part1 = pd.merge(dfderivesFrom,dfUses , left_on=['_belongsTo', '_to'],
                           right_on=['_belongsTo', '_to'], how='inner')

         part2 = pd.merge(part1, dfProduces, left_on=['_belongsTo', '_from_x'],
                          right_on=['_belongsTo', '_to'], how='inner')



     ####concat all dfs
     if (len(dfderivesFrom) > 0):
         dfAll = pd.concat([dfMiddleRowColumn, dfFinalRow, dfRestColumns])
     else:
         dfAll = dfRestColumns
     return dfAll, result

 def GetUniqueIDForExecutionPlan(self, df):

     try:
         dfSource = df[df['SourceIsObjectData'] == "1"]
         dfSource = dfSource[['ConnectionID','ControlFlowPath', 'SourceDB', 'SourceSchema', 'SourceTableName'
                              ]]
         dfSource = dfSource.drop_duplicates()

         dfTarget = df[df['TargetIsObjectData'] == "1"]
         dfTarget = dfTarget[['ConnectionID','ControlFlowPath', 'TargetDB', 'TargetSchema', 'TargetTableName'
                              ]]
         dfTarget = dfTarget.drop_duplicates()


         total = ""
         for index, row in dfSource.iterrows():
             try:
                 txt = str(row['ConnectionID'])+str(row['ControlFlowPath']) + str(row['SourceDB']) + \
                       str(row['SourceSchema']) + str(row['SourceTableName'])
                 total = total + str(txt)
             except Exception as e:
                 log.error(e)

         for index, row in dfTarget.iterrows():
             try:
                 txt = str(row['ConnectionID'])+str(row['ControlFlowPath']) + \
                       str(row['TargetDB']) + str(row['TargetSchema']) + str(row['TargetTableName'])
                 total = total + str(txt)
             except Exception as e:
                 log.error(e)

         numOfRows = str(len(df))
         input = total + numOfRows
         return hashlib.md5(input.encode('utf_16_le')).hexdigest()
     except Exception as e:
         log.error(e)













# print the value of the consumer
# we run the consumer generator to fetch the message scoming from topic1.

 def WriteJsonToDataFrame(self,text,dataKey,recordPath,meta):
    data=json.loads(text)
    try:
        if( recordPath=='output'):
            result = pd.json_normalize(data)

        elif (dataKey == 'schemas'):

            result = pd.json_normalize(data, record_path=['schemas'], meta=['id', 'name'], meta_prefix='', sep='_',
                                   errors='ignore')

            # result = pd.json_normalize(data, record_path=recordPath, meta=meta,
            #                            record_prefix='x', errors='ignore')

        elif (dataKey!=None):
            result = pd.json_normalize(data[dataKey], record_path=recordPath, meta=meta,
                                   record_prefix='x', errors='ignore')
        else:
            result = pd.json_normalize(data)
    except Exception as e:
            log.error(e)

    #print(tabulate(result, headers='keys', tablefmt='psql'))
    return   result


 def GetAllDataSources(self):

     url = self.arangoDBUrl

     payload = json.dumps({
         "query": "FOR u IN dataSource RETURN u"
     })
     headers = {
         'accept': 'application/json',
         'Content-Type': 'application/json'
     }

     response = requests.request("POST", url, headers=headers, data=payload)
     data = json.loads(response.text)['result']
     return json.dumps(data)




 def GetDocsFromCollection(self,execplanID,collectionName):
     #url = "http://192.168.100.11:8529/_db/spline/_api/cursor"
     url=self.arangoDBUrl

     payload = json.dumps({
         "query": "FOR u IN {collectionName} FILTER u._belongsTo=='executionPlan/{execplanID}' RETURN u".replace("{execplanID}",execplanID).replace("{collectionName}",collectionName)
     })
     headers = {
         'accept': 'application/json',
         'Content-Type': 'application/json'
     }

     response = requests.request("POST", url, headers=headers, data=payload)
     data= json.loads(response.text)['result']
     return json.dumps(data)





 def GetLineageDetailedForExecPlan(self,execplanID,method):


    url = f'{self.consumerUrl}/{method}?execId={execplanID}'

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    log.info(f'execution plan:"{execplanID}')
    return response.text
 def GetLineageDetailedForOperationID(self,opID,method):

#http://192.168.100.11:8080/consumer/operations/b156e991-a538-5052-b513-1d25648906d8:op-2

    url = f'{self.consumerUrl}/{method}/{opID}'

    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)
    # log.info(f'operation id:"{opID}')
    return response.text

class ConsumerExecution:
    def __init__(self):
        #self.topic1 = 'spline-topic'
        self.topic1=config['topic']
        #self.brokers = ['192.168.100.11:9092']
        self.brokers = [config['kafka-broker']]
        #http: // 34.242.14.60 /
        #self.consumerUrl='http://34.242.14.60:8080/consumer'
        self.consumerUrl = config['consumerUrl']


        #self.arangoDBUrl = "http://34.242.14.60:8529/_db/spline/_api/cursor"
        self.arangoDBUrl = config['arangoDBUrl']
        self.orientDB=config['orientDB']
        self.orientCrossName=config['OrientCrossName']
        self.OrientE2E = config['OrientE2E']
        self.group_id='myGroup'
        self.sqlConnectionString=config['sqlDBConnectionString']

        self.p1 = Consumer(self.topic1 ,
                           self.consumerUrl,
                           self.arangoDBUrl,
                           self.group_id,
                           self.brokers,
                           self.orientDB,
                           self.sqlConnectionString,
                           self.orientCrossName,
                           self.OrientE2E
                           )

    def run(self):
        log.info("Start..")
        self.p1.GetMessagesFromTopic()







consumerEx=ConsumerExecution()
consumerEx.run()



