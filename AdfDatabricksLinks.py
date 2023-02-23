import hashlib
import json

import pandas as pd
from tabulate import tabulate


from time import strftime, gmtime

from OrientDBApiCross import OrientDBApiCross
from SqlServerApi import SqlServerApi

from Logger import Logger

with open('Config.json', 'r') as f:
    config = json.load(f)


class LoadMngTable:
    def __init__(self, filePath, sqlApi, lineageflow,lineageobject,schema,orientApiCross):
        self.log = Logger('Adf')
        self.filePath = filePath
        self.sqlApi = sqlApi
        self.lineageflow=lineageflow
        self.lineageobject=lineageobject
        #elf.dfMngTable = pd.read_csv('MngTable/etl_job_param_control.csv')
        self.dfMngTable=pd.read_csv(filePath)
        self.dfAdfPipelines = pd.read_sql_query('SELECT * FROM TI.mng_adf_pipelines', self.sqlApi.cnxn)
        self.schema=schema
        self.orientApiCross=orientApiCross

    def Load(self):
        self.log.info(tabulate(self.dfAdfPipelines, headers='keys', tablefmt='psql'))
        self.log.info(tabulate(self.dfMngTable, headers='keys', tablefmt='psql'))

        dfAdfDatabricks = pd.merge(self.dfMngTable, self.dfAdfPipelines, left_on=['pipeline_name', 'adf_folder'],
                                    right_on=['Name', 'FolderName'], how='inner') \
            .rename(columns={'Id': 'pipelineID'})

        log = Logger('Consumer')(tabulate(dfAdfDatabricks, headers='keys', tablefmt='psql'))


        dfAdfDatabricks=dfAdfDatabricks[["CONNECTION_ID","CONN_LOGIC_NAME","pipelineID","Name","FolderName","Type","adf_folder",'adb_notebook_path']]
        dfAdfDatabricks = dfAdfDatabricks[ dfAdfDatabricks['adb_notebook_path'].notnull()]
        self.linksHandle(dfAdfDatabricks)











        # dfLineageObject
    #
    # def DeleteBefore(self,ConnectionID,ObjectID,orientApiCross):
    #     self.cursor.execute(
    #         f'delete from {self.schema}.{self.lineageobject} where ConnectionID= ? and ObjectID=? '
    #         , (ConnectionID, ObjectID))
    #     self.cursor.commit()
    #
    #     orientApiCross.DeleteVertex(ObjectID)



    def NormalizeSource(self, dfLineageFlowStg):

        try:
            dfLineageObject = pd.DataFrame()

            dfLineageFlowStg=dfLineageFlowStg[
                ["SourceConnectionID","SourceConnectionName","SourceToolName","SourceToolType","SourceObjectType","SourceFolder",'SourceObjectContainer',
                 "SourceObjectID","SourceObjectGUID","SourceObjectName","SourceSchema","SourceDB","SourceServer","SourceIsFake"]]
            dfLineageFlowStg=dfLineageFlowStg.drop_duplicates()

            dfLineageObject["ConnectionID"] = dfLineageFlowStg["SourceConnectionID"]
            dfLineageObject["ConnLogicName"] = dfLineageFlowStg["SourceConnectionName"]
            dfLineageObject["ToolType"] = dfLineageFlowStg["SourceToolType"]
            dfLineageObject["ToolName"] = dfLineageFlowStg["SourceToolName"]
            dfLineageObject["ObjectID"] = dfLineageFlowStg['SourceObjectID']
            dfLineageObject["ObjectGUID"] = dfLineageFlowStg['SourceObjectGUID']
            dfLineageObject["ObjectName"] = dfLineageFlowStg['SourceObjectName']
            dfLineageObject["ObjectType"] = dfLineageFlowStg["SourceObjectType"]
            dfLineageObject["Folder"] = dfLineageFlowStg["SourceFolder"]
            dfLineageObject["ObjectContainer"] = dfLineageFlowStg['SourceObjectContainer']
            dfLineageObject["ProjectName"] = dfLineageFlowStg['SourceConnectionName']
            dfLineageObject["Model"] = dfLineageFlowStg['SourceConnectionName']
            dfLineageObject["DatabaseName"] = ''
            dfLineageObject["SchemaName"] = ''
            dfLineageObject["IsFaked"] = 0
            dfLineageObject["ISDatabase"] = 0
            dfLineageObject["Definition"] = ""
            dfLineageObject["ModuleType"] = dfLineageFlowStg["SourceToolType"]
            dfLineageObject["ViewType"] = ""
            dfLineageObject["OrderInd"] = ""
            dfLineageObject["UpdatedDate"] = str(pd.datetime.datetime.now())
            dfLineageObject["ServerName"] = ""

            dfLineageObject.to_sql("LINEAGEOBJECT_TMP", sqlApi.engine, "TI", if_exists='replace', index=False)
            self.sqlApi.Upsert2Tables("TI.LINEAGEOBJECT_TMP", "TI.LINEAGEOBJECT")
        except Exception as e:
                 self.log.error(e)

    def NormalizeTarget(self, dfLineageFlowStg):
        try:
            dfLineageObject = pd.DataFrame()
            dfLineageFlowStg = dfLineageFlowStg[
                ["TargetConnectionID", "TargetConnectionName", "TargetToolName", "TargetToolType", "TargetObjectType","TargetObjectContainer",
                 "TargetFolder",
                 "TargetObjectID", "TargetObjectGUID", "TargetObjectName", "TargetSchema", "TargetDB", "TargetServer", "TargetIsFake"]]
            dfLineageFlowStg = dfLineageFlowStg.drop_duplicates()

            dfLineageObject["ConnectionID"] = dfLineageFlowStg["TargetConnectionID"]
            dfLineageObject["ConnLogicName"] = dfLineageFlowStg["TargetConnectionName"]
            dfLineageObject["ToolType"] = dfLineageFlowStg["TargetToolType"]
            dfLineageObject["ToolName"] = dfLineageFlowStg["TargetToolName"]
            dfLineageObject["ObjectID"] = dfLineageFlowStg['TargetObjectID']
            dfLineageObject["ObjectGUID"] = dfLineageFlowStg['TargetObjectGUID']
            dfLineageObject["ObjectName"] = dfLineageFlowStg['TargetObjectName']
            dfLineageObject["ObjectType"] = dfLineageFlowStg["TargetObjectType"]
            dfLineageObject["Folder"] = dfLineageFlowStg["TargetFolder"]
            dfLineageObject["ObjectContainer"] = dfLineageFlowStg['TargetObjectContainer']
            dfLineageObject["ProjectName"] = dfLineageFlowStg['TargetConnectionName']
            dfLineageObject["Model"] = dfLineageFlowStg['TargetConnectionName']
            dfLineageObject["DatabaseName"] = ''
            dfLineageObject["SchemaName"] = ''
            dfLineageObject["IsFaked"] = 0
            dfLineageObject["ISDatabase"] = 0
            dfLineageObject["Definition"] = ""
            dfLineageObject["ModuleType"] = dfLineageFlowStg["TargetToolType"]
            dfLineageObject["ViewType"] = ""
            dfLineageObject["OrderInd"] = ""
            dfLineageObject["UpdatedDate"] = str(pd.datetime.datetime.now())
            dfLineageObject["ServerName"] = ""

            dfLineageObject.to_sql("LINEAGEOBJECT_TMP", sqlApi.engine, "TI", if_exists='replace', index=False)
            self.sqlApi.Upsert2Tables("TI.LINEAGEOBJECT_TMP", "TI.LINEAGEOBJECT")
        except Exception as e:
            self.log.error(e)

    def GetPathWithoutLast(self,input, delim):
        ls = input.split(delim)
        ls.pop()
        return delim.join(ls)


    def linksHandle(self,dfAdfDatabricks):
        dfLineageFlowStg = pd.DataFrame()
        try:

            #"CONNECTION_ID","CONN_LOGIC_NAME","pipelineID","Name","FolderName","Type","adf_folder",'adb_notebook_path'



            dfLineageFlowStg["TargetObjectID"] = config["ConnectionID"] +  dfAdfDatabricks["adb_notebook_path"]
            dfLineageFlowStg["TargetConnectionID"] = config["ConnectionID"]
            dfLineageFlowStg["TargetConnectionName"] = 'Databricks'
            dfLineageFlowStg["TargetObjectGUID"] = [hashlib.md5(val.encode('utf_16_le')).hexdigest() for val in    dfLineageFlowStg['TargetObjectID']]
            dfLineageFlowStg["TargetObjectName"] = dfAdfDatabricks["adb_notebook_path"].apply( lambda x:  x.split('/')[-1])
            dfLineageFlowStg["TargetSchema"] = ""
            dfLineageFlowStg["TargetDB"] = ""
            dfLineageFlowStg["TargetServer"] = ""
            dfLineageFlowStg["TargetObjectType"] = "Notebook"
            dfLineageFlowStg["TargetIsFake"] = "0"
            dfLineageFlowStg["LinkType"] = "ImpactAnalysis"
            dfLineageFlowStg["LinkDescription"] = ""
            dfLineageFlowStg["ISSourceInd"] = 0
            dfLineageFlowStg["TargetToolType"] = 'ETL'
            dfLineageFlowStg["TargetToolName"] = 'DATABRICKS'

            dfLineageFlowStg["TargetFolder"] =  dfAdfDatabricks["adb_notebook_path"]
            dfLineageFlowStg["TargetObjectContainer"] = dfLineageFlowStg['TargetFolder'].apply(
                lambda x:  self.GetPathWithoutLast(x,'/') )

            dfLineageFlowStg["UpdatedDate"] = strftime("%Y-%m-%d %H:%M:%S", gmtime())

            dfLineageFlowStg["SourceConnectionID"] = dfAdfDatabricks["CONNECTION_ID"]
            dfLineageFlowStg["SourceConnectionName"] = dfAdfDatabricks["CONN_LOGIC_NAME"]
            dfLineageFlowStg["SourceObjectID"] = dfAdfDatabricks["CONNECTION_ID"] + dfAdfDatabricks["Name"].apply( lambda x: x.upper()) + \
                                                 dfAdfDatabricks["pipelineID"].apply( lambda x: x.upper())+dfAdfDatabricks["Name"].apply( lambda x: x.upper())

            dfLineageFlowStg["SourceObjectID"]=dfLineageFlowStg["SourceObjectID"].apply(lambda x: x.replace(".",""))

            dfLineageFlowStg["SourceObjectGUID"] = [hashlib.md5(val.encode('utf_16_le')).hexdigest() for val in dfLineageFlowStg['SourceObjectID']]
            dfLineageFlowStg["SourceObjectGUID"]=dfLineageFlowStg["SourceObjectGUID"].apply( lambda x: x.upper())

            dfLineageFlowStg["SourceObjectName"] = dfAdfDatabricks["Name"]

            dfLineageFlowStg["SourceSchema"] = ""
            dfLineageFlowStg["SourceDB"] = ""
            dfLineageFlowStg["SourceServer"] = ""
            dfLineageFlowStg["SourceObjectType"] = dfAdfDatabricks["Type"]
            dfLineageFlowStg["SourceIsFake"] = "0"
            dfLineageFlowStg["LinkType"] = "ImpactAnalysis"
            dfLineageFlowStg["LinkDescription"] = ""
            dfLineageFlowStg["SourceceInd"] = 0
            dfLineageFlowStg["SourceToolType"] = 'ETL'
            dfLineageFlowStg["SourceToolName"] = 'ADF'
            dfLineageFlowStg["SourceFolder"] = dfAdfDatabricks["pipelineID"]
            dfLineageFlowStg["SourceObjectContainer"] = dfAdfDatabricks["Name"]



            dfLineageFlowStg["LinkID"] = dfLineageFlowStg["SourceObjectID"] + dfLineageFlowStg["TargetObjectID"]
            dfLineageFlowStg["LinkedGUID"] = [hashlib.md5(val.encode('utf_16_le')).hexdigest() for val in
                                              dfLineageFlowStg['LinkID']]

            dfLineageFlowStg["TargetID"] = dfLineageFlowStg["TargetObjectGUID"]
            dfLineageFlowStg["SourceID"]= dfLineageFlowStg["SourceObjectGUID"]

        except Exception as e:
            self.log.error(e)


        try:

            dfLineageFlow=pd.DataFrame()
            dfLineageFlow=dfLineageFlowStg[["SourceConnectionID"
                                               ,"SourceConnectionName"
                                               ,"SourceToolType"
                                               ,"SourceToolName"
                                               ,"SourceObjectID"
                                               ,"SourceObjectGUID"
                                               ,"SourceObjectName"
                                               ,"SourceObjectType"

                                               ,"TargetConnectionID"
                                              , "TargetConnectionName"
                                              , "TargetToolType"
                                              , "TargetToolName"
                                               ,"TargetObjectID"
                                               ,"TargetObjectGUID"
                                               ,"TargetObjectName"
                                               ,"TargetObjectType"
                                               ,"LinkType"
                                               ,"LinkDescription"
                                               ,"LinkedGUID"
                                               ,"ISSourceInd"
                                               ,"SourceID"
                                               ,"TargetID"
                                               ,"UpdatedDate"]]

            dfLineageFlow.to_sql("LINEAGEFLOW_TMP", self.sqlApi.engine, schema="TI", if_exists='replace', index=False)

        except Exception as e:
            self.log.error(e)



        for index, row in dfLineageFlowStg.iterrows():
            SourceObjectGUID = row["SourceObjectGUID"]
            TargetObjectGUID = row["TargetObjectGUID"]
            try:
                listOfParams1 = [row["SourceConnectionID"],
                                 row["SourceConnectionName"],
                                 row["SourceToolName"],
                                 row["SourceToolType"],
                                 row["SourceObjectGUID"],
                                 row["SourceObjectID"],
                                 row["SourceObjectName"],
                                 row["SourceObjectType"],
                                 row["SourceFolder"],
                                 row["SourceObjectContainer"],
                                 row["SourceObjectName"],
                                 row["SourceObjectName"],
                                 '',
                                 '',
                                 0,
                                 0,
                                 "",
                                  row["SourceToolType"],
                                 "",
                                 "",
                                 row["UpdatedDate"],
                                 ""]
                listOfParams2 = [row["TargetConnectionID"],
                                 row["TargetConnectionName"],
                                 row["TargetToolName"],
                                 row["TargetToolType"],
                                 row["TargetObjectGUID"],
                                 row["TargetObjectID"],
                                 row["TargetObjectName"],
                                 row["TargetObjectType"],
                                 row["TargetFolder"],
                                 row["TargetObjectContainer"],
                                 row["TargetObjectName"],
                                 row["TargetObjectName"],
                                 '',
                                 '',
                                 0,
                                 0,
                                 "",
                                 row["TargetToolType"],
                                 "",
                                 "",
                                 row["UpdatedDate"],
                                 ""]
                exists= self.orientApiCross.CheckVertexByGuid(SourceObjectGUID)
                if exists=="-1":
                    rid1=self.orientApiCross.CheckInDictionary(SourceObjectGUID,listOfParams1,False)
                else:
                    rid1=exists

                rid2 = self.orientApiCross.CheckInDictionary(TargetObjectGUID, listOfParams2, False)
                self.orientApiCross.CheckInDictionaryLinks(rid1,rid2,False,"ImpactAnalysis")

            except Exception as e:
                self.log.error(e)

        self.NormalizeSource(dfLineageFlowStg)
        self.NormalizeTarget(dfLineageFlowStg)
        self.sqlApi.Upsert2TablesLineageFlow("TI.LINEAGEFLOW_TMP", "TI.LINEAGEFLOW")




#
# sqlApi = SqlServerApi(
#             "Data Source=sql.onprem.qa.octopai-corp.local;Initial Catalog=digiceljm_Prod;User ID=oumdb;Password=iAmOum!@")

sqlApi = SqlServerApi(config['sqlDBConnectionString'])
orientApiCross = OrientDBApiCross(config['OrientCrossName'],
                                          config['OrientCrossName'], "vertexCross.json",
                                          "edgesCross.json")

l=LoadMngTable(config['AdfDatabricksMngFile'],sqlApi,'LINEAGEFLOW','LINEAGEOBJCET',"TI",orientApiCross)
l.Load()