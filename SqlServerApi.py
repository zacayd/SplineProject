
import pyodbc
import hashlib
from time import strftime, gmtime
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

#Data Source=sql.onprem.qa.octopai-corp.local;Initial Catalog=digiceljm_Prod;User ID=oumdb;Password=iAmOum!@


from sqlalchemy.connectors import pyodbc
# from sqlalchemy.connectors import pyodbc


from Logger import Logger

log = Logger('Consumer')

class SqlServerApi:
    def __init__(self, connectionString):
        self.tableName="MNG_OCTOPAI_LINEAGE_SOURCE_TO_TARGET"
        self.schema='TI'
        self.server = connectionString.split(';')[0].split('=')[1]
        self.database = connectionString.split(';')[1].split('=')[1]
        self.usr = connectionString.split(';')[2].split('=')[1]
        self.pwd = connectionString.split(';')[3].split('=')[1]




        self.port="1433"

        try:

            # self.cnxn = pyodbc.connect(
            #     'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + self.server + ';DATABASE=' + self.database + ';UID=' + self.usr +\
            #     ';PWD=' + self.pwd + ';TrustServerCertificate=Yes;')
            # self.engine = f"mssql+pyodbc://{self.usr}:{self.pwd}@{self.server}/{self.database}?driver=SQL Server Native Client 11.0?trusted_connection=yes"
            self.engine = create_engine(
                f"mssql+pyodbc://{self.usr}:{self.pwd}@{self.server}:{self.port}/{self.database}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")

            # self.enginex = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + self.server + ';DATABASE=' + self.database + ';UID=' + self.usr +\
            #     ';PWD=' + self.pwd + ';TrustServerCertificate=Yes;'

            # self.cursor = self.cnxn.cursor()
            self.cnxn=self.engine.connect()

        except Exception as e:
            log.error(e)

    def SourceToTargetLoad(self,df):


            try:
                dfDistinct=df[["ConnectionID","ContainerObjectPath","ControlFlowPath","sourceLayerName"]]
                dfDistinct=dfDistinct.drop_duplicates()


                for index, row in dfDistinct.iterrows():
                    ConnectionID=row["ConnectionID"]
                    ContainerObjectPath =  row["ContainerObjectPath"]
                    ControlFlowPath = row["ControlFlowPath"]
                    LayerName= row["sourceLayerName"][:-2]




                    statement="delete from {}.{} where CONNECTION_ID='{}' and CONTROL_FLOW_PATH='{}' and CONTROL_FLOW_PATH='{}' and SOURCE_LAYER_NAME like '{}%" \
                              "'".format(self.schema,self.tableName,ConnectionID,ContainerObjectPath,ControlFlowPath,LayerName)
                    try:
                        log.info(statement)
                        self.cnxn.execute("delete from TI.MNG_OCTOPAI_LINEAGE_SOURCE_TO_TARGET where CONNECTION_ID= ? and CONTAINER_OBJECT_PATH=? and  CONTROL_FLOW_PATH=? "
                                            "and SOURCE_LAYER_NAME like ?",
                                            (ConnectionID,ContainerObjectPath,ControlFlowPath,LayerName+'%'))
                        # self.cnxn.commit_prepared()
                        log.info("finish delete")
                    except Exception as e:
                        log.error(e)

                dfDistinct = df[["ConnectionID", "ContainerObjectPath", "ControlFlowPath", "TargetLayerName"]]
                dfDistinct = dfDistinct.drop_duplicates()

                for index, row in dfDistinct.iterrows():
                    ConnectionID = row["ConnectionID"]
                    ContainerObjectPath = row["ContainerObjectPath"]
                    ControlFlowPath = row["ControlFlowPath"]
                    LayerName = row["TargetLayerName"][:-2]

                    statement = "delete from {}.{} where CONNECTION_ID='{}' and CONTROL_FLOW_PATH='{}' and CONTROL_FLOW_PATH='{}' and TARGET_LAYER_NAME like '{}%" \
                                "'".format(self.schema, self.tableName, ConnectionID, ContainerObjectPath,
                                           ControlFlowPath, LayerName)
                    try:
                        # log.info(statement)
                        log.info(statement)
                        self.cnxn.execute(
                            "delete from TI.MNG_OCTOPAI_LINEAGE_SOURCE_TO_TARGET where CONNECTION_ID= ? and CONTAINER_OBJECT_PATH=? and  CONTROL_FLOW_PATH=? "
                            "and TARGET_LAYER_NAME like ?",
                            (ConnectionID, ContainerObjectPath, ControlFlowPath, LayerName+'%'))
                        # self.cnxn.commit()
                        log.info("finish delete")
                    except Exception as e:
                        log.error(e)





                try:
                    df["SourceProvider"]=''
                    df["TargetProvider"] = ''
                    df["SourcePrecision"] = ''
                    df["SourceScale"] = ''
                    df["TargetPrecision"] = ''
                    df["TargetScale"] = ''
                    df["SourceSql"] = ''
                    df["TargetSql"] = ''
                    df["SourceConnectionKey"] = ''
                    df["TargetConnectionKey"] = ''
                    df["SourcePhysicalTable"] = ''
                    df["TargetPhysicalTable"] = ''
                    df["SourceFilePath"] = ''
                    df["TargetFilePath"] = ''
                    df["Expression"] = ''
                    df["FriendlyExpression"] = ''
                    df["SourceCode"] = 'Pipeline'
                    df["SourceCoordinate"] = ''
                    df["TargetCoordinate"] = ''
                    df["ISColumnOrphand"] = ''
                    df["ContainerObjectDesciption"] = ''
                    df["LinkType"] = ''
                    df["LinkDescription"] = ''
                    df["UpdatedDate"]=strftime("%Y-%m-%d %H:%M:%S", gmtime())

                    df = df.rename(columns={'ConnectionID': 'CONNECTION_ID','ConnLogicName': 'CONN_LOGIC_NAME',
                                                                                 'ContainerObjectName': 'CONTAINER_OBJECT_NAME',
                                                                                 'ContainerObjectPath': 'CONTAINER_OBJECT_PATH',
                                                                                 'ContainerObjectType': 'CONTAINER_OBJECT_TYPE',
                                                                                 'ContainerToolType': 'CONTAINER_TOOL_TYPE',
                                                                                 'ControlFlowName': 'CONTROL_FLOW_NAME',
                                                                                 'ControlFlowPath': 'CONTROL_FLOW_PATH',
                                                                                 'Source_id':'SOURCE_ID',
                                                                                 'sourceLayerName':'SOURCE_LAYER_NAME',
                                                                                 'SourceSchema': 'SOURCE_SCHEMA',
                                                                                 'SourceDB':'SOURCE_DB',
                                                                                 'SourceProvider':'SOURCE_PROVIDER',
                                                                                 'SourceTableName':'SOURCE_TABLE',
                                                                                 'SourceColumn':'SOURCE_COLUMN',
                                                                                 'SourceColumnId':'SOURCE_COLUMN_ID',
                                                                                 'SourceDataType':'SOURCE_DATATYPE',
                                                                                 'SourcePrecision':'SOURCE_PRECISION',
                                                                                 'SourceScale':'SOURCE_SCALE',
                                                                                 'SourceObjectType':'SOURCE_OBJECT_TYPE',
                                                                                 'SourceSql':'SOURCE_SQL',
                                                                                 'SourceConnectionKey':'SOURCE_CONNECTION_KEY',
                                                                                 'SourcePhysicalTable':'SOURCE_PHYSICAL_TABLE',
                                                                                 'SourceFilePath':'SOURCE_FILE_PATH',
                                                                                'Target_id': 'TARGET_ID',
                                                                                'TargetLayerName': 'TARGET_LAYER_NAME',
                                                                                'TargetSchema': 'TARGET_SCHEMA',
                                                                                'TargetDB': 'TARGET_DB',
                                                                                'TargetProvider': 'TARGET_PROVIDER',
                                                                                'TargetTableName': 'TARGET_TABLE',
                                                                                'TargetColumn': 'TARGET_COLUMN',
                                                                                'TargetColumnId': 'TARGET_COLUMN_ID',
                                                                                'TargetDataType': 'TARGET_DATATYPE',
                                                                                'TargetPrecision': 'TARGET_PRECISION',
                                                                                'TargetScale': 'TARGET_SCALE',
                                                                                'TargetObjectType': 'TARGET_OBJECT_TYPE',
                                                                                'TargetSql': 'TARGET_SQL',
                                                                                'TargetPhysicalTable': 'TARGET_PHYSICAL_TABLE',
                                                                                'TargetConnectionKey': 'TARGET_CONNECTION_KEY',
                                                                                'TargetFilePath': 'TARGET_FILE_PATH',
                                                                                'Expression':'EXPRESSION',
                                                                                'FriendlyExpression':'FRIENDLY_EXPRESSION',
                                                                                'SourceCode':'SOURCE_CODE',
                                                                                'SourceIsObjectData':'SOURCE_IS_OBJECT_DATA',
                                                                                'TargetIsObjectData': 'TARGET_IS_OBJECT_DATA',
                                                                                'UpdatedDate': 'UPDATED_DATE',
                                                                                'SourceServer':'SOURCE_SERVER',
                                                                                'TargetServer':'TARGET_SERVER',
                                                                                'SourceCoordinate' :'SOURCE_COORDINATE',
                                                                                'TargetCoordinate': 'TARGET_COORDINATE',
                                                                                'ISColumnOrphand':'IS_COLUMN_ORPHAN',
                                                                                'ContainerObjectDesciption' :'CONTAINER_OBJECT_DESCRIPTION',
                                                                                'LinkType':'LinkType',
                                                                                'LinkDescription': 'LinkDescription'})
                    df=df[["CONNECTION_ID"
                          ,"CONN_LOGIC_NAME"
                          ,"CONTAINER_OBJECT_NAME"
                          ,"CONTAINER_OBJECT_PATH"
                          ,"CONTAINER_OBJECT_TYPE"
                          ,"CONTAINER_TOOL_TYPE"
                          ,"CONTROL_FLOW_NAME"
                          ,"CONTROL_FLOW_PATH"
                          ,"SOURCE_ID"
                          ,"SOURCE_LAYER_NAME"
                          ,"SOURCE_SCHEMA"
                          ,"SOURCE_DB"
                          ,"SOURCE_PROVIDER"
                          ,"SOURCE_TABLE"
                          ,"SOURCE_COLUMN"
                          ,"SOURCE_COLUMN_ID"
                          ,"SOURCE_DATATYPE"
                          ,"SOURCE_PRECISION"
                          ,"SOURCE_SCALE"
                          ,"SOURCE_OBJECT_TYPE"
                          ,"SOURCE_SQL"
                          ,"SOURCE_CONNECTION_KEY"
                          ,"SOURCE_PHYSICAL_TABLE"
                          ,"SOURCE_FILE_PATH"
                          ,"TARGET_ID"
                          ,"TARGET_LAYER_NAME"
                          ,"TARGET_SCHEMA"
                          ,"TARGET_DB"
                          ,"TARGET_PROVIDER"
                          ,"TARGET_TABLE"
                          ,"TARGET_COLUMN"
                          ,"TARGET_COLUMN_ID"
                          ,"TARGET_DATATYPE"
                          ,"TARGET_PRECISION"
                          ,"TARGET_SCALE"
                          ,"TARGET_OBJECT_TYPE"
                          ,"TARGET_SQL"
                          ,"TARGET_CONNECTION_KEY"
                          ,"TARGET_PHYSICAL_TABLE"
                          ,"TARGET_FILE_PATH"
                          ,"EXPRESSION"
                          ,"FRIENDLY_EXPRESSION"
                          ,"SOURCE_CODE"
                          ,"SOURCE_IS_OBJECT_DATA"
                          ,"TARGET_IS_OBJECT_DATA"
                          ,"UPDATED_DATE"
                          ,"SOURCE_SERVER"
                          ,"TARGET_SERVER"
                          ,"SOURCE_COORDINATE"
                          ,"TARGET_COORDINATE"
                          ,"IS_COLUMN_ORPHAN"
                          ,"CONTAINER_OBJECT_DESCRIPTION"
                          ,"LinkType"
                          ,"LinkDescription"]]

                    log.info(f"Start load to table:{self.tableName}")
                    df.to_sql(self.tableName,self.engine,schema=self.schema, if_exists='append', index=False,method='multi')
                    log.info(f"End load to table:{self.tableName}")

                except Exception as e:
                   log.error(e)

            except Exception as e:
                log.error(e)

    def DiscoveryTableLoad(self, df):

        try:
            # connection_string= 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + self.server + ';DATABASE=' + 'oumdb' + ';UID=' + self.usr + \
            #     ';PWD=' + self.pwd + ';TrustServerCertificate=Yes;'

            connection_string=f"mssql+pyodbc://{self.usr}:{self.pwd }@{self.server}:{self.port}/{'oumdb'}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"

            engine = create_engine(connection_string)


            self.cnxnOum = engine.connect()



            result=self.cnxnOum.execute("SELECT * FROM TI.MNG_EXTRACTOR_QUERIES where TOOL_NAME=?","DATABRICKS_POST")


            dfDistinct = df[["ConnectionID", "ContainerObjectPath", "ControlFlowPath","sourceLayerName","SourceIsObjectData"]]
            dfDistinct = dfDistinct.loc[dfDistinct['SourceIsObjectData'] == "1"]

            dfDistinctT = df[["ConnectionID", "ContainerObjectPath", "ControlFlowPath", "TargetLayerName","TargetIsObjectData"]]
            dfDistinctT = dfDistinctT.loc[dfDistinctT['TargetIsObjectData'] == "1"]
            dfDistinct = dfDistinct.drop_duplicates()
            dfDistinctT = dfDistinctT.drop_duplicates()
            myresult = result.fetchall()

            for row in myresult:
                tableName=row[4]
                queryText=row[2]




                for index, row in dfDistinct.iterrows():
                    ConnectionID = row["ConnectionID"]
                    ContainerObjectPath = row["ContainerObjectPath"]
                    ControlFlowPath = row["ControlFlowPath"]
                    LayerName=row["sourceLayerName"][:-2]+'%'
                    if tableName!="TI.MNG_SQLS_AUTO_COMPLETE":
                        try:
                            statement = "delete from {} where CONNECTION_ID='{}' and WORKSPACEPATH='{}' and NOTEBOOKPATH='{}' and LAYER_NAME like {}%'".format(
                                tableName, ConnectionID, ContainerObjectPath, ControlFlowPath,LayerName)


                            log.info(statement)
                            stmt=f'delete from {tableName} where CONNECTION_ID=? and WORKSPACEPATH=? and NOTEBOOKPATH=? and LAYER_NAME like ?'
                            self.cnxn.execute(stmt,ConnectionID,ContainerObjectPath,ControlFlowPath,LayerName)
                            # self.cursor.commit()
                        except Exception as e:
                            log.error(e)
                    try:

                        self.cnxn.execute(queryText,ConnectionID,ContainerObjectPath,ControlFlowPath,LayerName)
                        # log.info(queryText)
                        # self.cursor.commit()
                        # cursor.commit()
                    except Exception as e:
                            log.error(e)


                #targets

                for index, row in dfDistinctT.iterrows():
                    ConnectionID = row["ConnectionID"]
                    ContainerObjectPath = row["ContainerObjectPath"]
                    ControlFlowPath = row["ControlFlowPath"]
                    LayerName = row["TargetLayerName"][:-2]+'%'
                    if tableName != "TI.MNG_SQLS_AUTO_COMPLETE":
                        try:
                            statement = "delete from {} where CONNECTION_ID='{}' and WORKSPACEPATH='{}' and NOTEBOOKPATH='{}' and LAYER_NAME like {}%'".format(
                                tableName, ConnectionID, ContainerObjectPath, ControlFlowPath, LayerName)

                            # log.info(statement)
                            stmt = f'delete from {tableName} where CONNECTION_ID=? and WORKSPACEPATH=? and NOTEBOOKPATH=? and LAYER_NAME like ?'
                            self.cnxn.execute(stmt, ConnectionID, ContainerObjectPath, ControlFlowPath, LayerName)
                            # self.cursor.commit()
                        except Exception as e:
                            log.error(e)
                    try:

                        self.cnxn.execute(queryText, ConnectionID, ContainerObjectPath, ControlFlowPath, LayerName)
                        # log.info(queryText)
                        # self.cursor.commit()

                    except Exception as e:
                        log.error(e)
        except Exception as e:
                     log.error(e)

    def DeleteLineageFlowTableToETL(self,df):
        log.info('Start deleteing from  LineageFlow table..')
        try:

            # table-->ETL
            dfSource = df[
                ['ConnectionID', 'ConnLogicName', 'ContainerObjectName', 'ContainerObjectPath', 'ContainerObjectType',
                 'ContainerToolType', 'ControlFlowName', 'ControlFlowPath', 'SourceObjectType', 'SourceDB',
                 'SourceSchema',
                 'SourceTableName', 'SourceServer', 'SourceObjectType', 'SourceIsObjectData']]
            dfSource = dfSource[dfSource['SourceIsObjectData'] == "1"]

            dfSource = dfSource.drop_duplicates()
            tableName = "LINEAGEFLOW"
            for index, row in dfSource.iterrows():

                try:
                    # type=row['SourceObjectType'].iloc[0]

                    # if type=='FILE':
                    SourceObjectID =row['SourceServer'] + row["SourceDB"] + row["SourceSchema"] + row[
                        "SourceTableName"]
                    # else:
                    #     SourceObjectID = row['SourceServer'] + row["SourceDB"] + row["SourceSchema"] + row[
                    #         "SourceTableName"]

                    SourceObjectID=SourceObjectID.upper()

                    TargetObjectID = row['ConnectionID'] + row["ContainerObjectPath"]
                    TargetObjectID=TargetObjectID.upper()

                    self.cnxn.execute(
                        f'delete from {self.schema}.{tableName} where SourceObjectID= ? and TargetObjectID=? '
                        , SourceObjectID, TargetObjectID)

                    # self.cursor.commit()
                except Exception as e:
                    log.error(e)
            return   dfSource
        except Exception as e:
            log.error(e)

    def DeleteLineageFlowETLToTable(self,df):
        log.info('Start deleteing from  LineageFlow table..')
        try:

            # table-->ETL
            dfSource = df[
                ['ConnectionID', 'ConnLogicName', 'ContainerObjectName', 'ContainerObjectPath', 'ContainerObjectType',
                 'ContainerToolType', 'ControlFlowName', 'ControlFlowPath', 'TargetObjectType', 'TargetDB',
                 'TargetSchema',
                 'TargetTableName', 'TargetServer', 'TargetObjectType', 'TargetIsObjectData']]
            dfSource = dfSource[dfSource['TargetIsObjectData'] == "1"]

            dfSource = dfSource.drop_duplicates()
            tableName = "LINEAGEFLOW"
            for index, row in dfSource.iterrows():

                try:
                    SourceObjectID = row['ConnectionID'] + row['ContainerObjectPath']
                    SourceObjectID=SourceObjectID.upper()
                    # type = row['SourceObjectType'].iloc[0]
                    # if type == "FILE":
                    TargetObjectID = row['TargetServer'] + row["TargetDB"]+row["TargetSchema"]+row["TargetTableName"]
                    # else:
                    #     TargetObjectID = row['TargetServer'] + row["TargetDB"] + row["TargetSchema"] + row["TargetTableName"]

                    TargetObjectID=TargetObjectID.upper()
                    self.cnxn.execute(
                        f'delete from {self.schema}.{tableName} where SourceObjectID= ? and TargetObjectID=? '
                        , SourceObjectID, TargetObjectID)

                    # self.cursor.commit()
                except Exception as e:
                    log.error(e)
            return   dfSource
        except Exception as e:
            log.error(e)

    def LoadLineageFlow(self,df):

        try:
            dfSource=self.DeleteLineageFlowTableToETL(df)

            dfLineageFlow = dfSource[['ConnectionID']]
            dfLineageFlow["ToolName"]=dfSource['ContainerToolType']
            dfLineageFlow["SourceToolType"]='DATABASE'

            dfLineageFlow["SourceObjectID"] = dfSource ['SourceServer'] + dfSource["SourceDB"] + dfSource["SourceSchema"] + dfSource["SourceTableName"]
            dfLineageFlow["SourceObjectID"] = dfLineageFlow["SourceObjectID"].apply(lambda x: x.upper())

            dfLineageFlow["SourceObjectGUID"] =[hashlib.md5(val.encode('utf_16_le')).hexdigest() for val in dfLineageFlow['SourceObjectID']]
                #hashlib.md5(dfLineageFlow["SourceObjectID"].encode('utf-8')).hexdigest()
            #[hashlib.md5(val).hexdigest() for val in ob['ssno']]

            dfLineageFlow["SourceObjectName"]= dfSource["SourceTableName"]
            dfLineageFlow["TargetObjectID"] = dfSource["ConnectionID"] + dfSource["ContainerObjectPath"]
            dfLineageFlow["TargetObjectID"] = dfLineageFlow["TargetObjectID"].apply(lambda x: x.upper())
            dfLineageFlow["TargetObjectGUID"] = [hashlib.md5(val.encode('utf_16_le')).hexdigest() for val in dfLineageFlow['TargetObjectID']]
            dfLineageFlow["TargetObjectName"] = dfSource["ControlFlowName"]
            dfLineageFlow["LinkType"]="DataFlow"
            dfLineageFlow["LinkDescription"] = ""
            dfLineageFlow["LinkID"]=dfLineageFlow["SourceObjectID"]+dfLineageFlow["TargetObjectID"]
            dfLineageFlow["LinkedGUID"]=[hashlib.md5(val.encode('utf_16_le')).hexdigest() for val in dfLineageFlow['LinkID']]

            dfLineageFlow["ISSourceInd"]=0
            dfLineageFlow["TargetToolType"]='ETL'

            dfLineageFlow["SourceID"] = dfLineageFlow["SourceObjectGUID"]
            dfLineageFlow["TargetID"] = dfLineageFlow["TargetObjectGUID"]
            dfLineageFlow["UpdatedDate"]=strftime("%Y-%m-%d %H:%M:%S", gmtime())
            tableName="LINEAGEFLOW"

            log.info("dfLineageFlow:")

            dfLineageFlow = dfLineageFlow[
                ["ConnectionID", "ToolName", "SourceToolType", "SourceObjectID", "SourceObjectGUID",
                 "SourceObjectName", "TargetObjectID", "TargetObjectGUID", "TargetObjectName", "LinkType",
                 "LinkDescription", "LinkedGUID",
                 "ISSourceInd", "TargetToolType", "SourceID", "TargetID", "UpdatedDate"

                 ]]
            # log.info(tabulate(dfLineageFlow, headers='keys', tablefmt='psql'))
            dfLineageFlow.to_sql(tableName, self.engine, schema=self.schema, if_exists='append', index=False,method='multi')


            # ETL-->table

            dfSource = self.DeleteLineageFlowETLToTable(df)
            dfSource = df[
                ['ConnectionID', 'ConnLogicName', 'ContainerObjectName', 'ContainerObjectPath', 'ContainerObjectType',
                 'ContainerToolType', 'ControlFlowName', 'ControlFlowPath', 'TargetDB', 'TargetSchema',
                 'TargetTableName', 'TargetServer', 'TargetObjectType', 'TargetIsObjectData']]
            dfSource = dfSource[dfSource['TargetIsObjectData'] == "1"]


            dfSource = dfSource.drop_duplicates()

            tableName = "LINEAGEFLOW"



            dfLineageFlow = dfSource[['ConnectionID']]
            dfLineageFlow["ToolName"] = dfSource['ContainerToolType']
            dfLineageFlow["SourceToolType"] = 'ETL'
            dfLineageFlow["SourceObjectID"] = dfSource['ConnectionID'] + dfSource['ContainerObjectPath']
            dfLineageFlow["SourceObjectID"]=dfLineageFlow["SourceObjectID"].apply(lambda x: x.upper())

            dfLineageFlow["SourceObjectGUID"] = [hashlib.md5(val.encode('utf_16_le')).hexdigest() for val in dfLineageFlow['SourceObjectID']]
            dfLineageFlow["SourceObjectName"] = dfSource["ControlFlowName"]


            dfLineageFlow["TargetObjectID"] = dfSource ['TargetServer'] + dfSource["TargetDB"] + dfSource["TargetSchema"] + dfSource["TargetTableName"]
            dfLineageFlow["TargetObjectID"] = dfLineageFlow["TargetObjectID"].apply(lambda x: x.upper())
            dfLineageFlow["TargetObjectGUID"] = [hashlib.md5(val.encode('utf_16_le')).hexdigest() for val in dfLineageFlow['TargetObjectID']]
            dfLineageFlow["TargetObjectName"] = dfSource["TargetTableName"]
            dfLineageFlow["LinkType"] = "DataFlow"
            dfLineageFlow["LinkDescription"] = ""
            dfLineageFlow["LinkID"] = dfLineageFlow["SourceObjectID"] + dfLineageFlow["TargetObjectID"]
            dfLineageFlow["LinkedGUID"] = [hashlib.md5(val.encode('utf_16_le')).hexdigest() for val in dfLineageFlow['LinkID']]

            dfLineageFlow["ISSourceInd"] = 0
            dfLineageFlow["TargetToolType"] = 'DATABASE'

            dfLineageFlow["SourceID"] = dfLineageFlow["SourceObjectGUID"]
            dfLineageFlow["TargetID"] = dfLineageFlow["TargetObjectGUID"]
            dfLineageFlow["UpdatedDate"] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            tableName = "LINEAGEFLOW"
            #dfLineageFlow.to_sql(tableName, self.engine, schema=self.schema, if_exists='append', index=False)

            #Megrge RT with final

            log.info("dfLineageFlow:")
            dfLineageFlow=dfLineageFlow[["ConnectionID", "ToolName","SourceToolType","SourceObjectID","SourceObjectGUID",
                                         "SourceObjectName","TargetObjectID","TargetObjectGUID","TargetObjectName","LinkType",
                                         "LinkDescription","LinkedGUID",
                                         "ISSourceInd","TargetToolType","SourceID","TargetID","UpdatedDate"

                                         ]]
            # log.info(tabulate(dfLineageFlow, headers='keys', tablefmt='psql'))
            dfLineageFlow.to_sql(tableName, self.engine, schema=self.schema, if_exists='append', index=False,method='multi')




        except Exception as e:
                log.error(e)



    def InsertStatmentMngLineageUIDatabase(self,tableName,dictCols):

        try:
            insertTxt = f"INSERT INTO {tableName} \
                                  ([CONNECTION_ID]\
                                   ,[CONN_LOGIC_NAME]\
                                   ,[SYS_UPDATE_DATE]\
                                   ,[DATABASE_TYPE]\
                                   ,[DATABASE_NAME]\
                                   ,[SCHEMA_NAME]\
                                   ,[SERVER_NAME]\
                                   ,[OBJECT_NAME]\
                                   ,[OBJECT_TYPE]\
                                   ,[GROUP_OBJECT_TYPE]\
                                   ,[TEXT])\
                                VALUES \
                                      (?,?,?,?,?,?,?,?,?,?,?)"

            self.cnxn.execute(insertTxt,dictCols[0],
                                         dictCols[1],
                                         dictCols[2],
                                         dictCols[3],
                                         dictCols[4],
                                         dictCols[5],
                                         dictCols[6],
                                         dictCols[7],
                                         dictCols[8],
                                         dictCols[9],
                                         dictCols[10])


            # self.cursor.commit()
        except Exception as e:
            log.error(e)

    def InsertStatmentLoadObject(self,tableName,dictCols):

        try:
            insertTxt = f"INSERT INTO {tableName} \
                                  ([ConnectionID]\
                               ,[ConnLogicName]\
                               ,[ToolType]\
                               ,[ToolName]\
                               ,[ObjectGUID]\
                               ,[ObjectID]\
                               ,[ObjectName]\
                               ,[ObjectType]\
                               ,[Folder]\
                               ,[ObjectContainer]\
                               ,[ProjectName]\
                               ,[Model]\
                               ,[DatabaseName]\
                               ,[SchemaName]\
                               ,[IsFaked]\
                               ,[ISDatabase]\
                               ,[Definition]\
                               ,[ModuleType]\
                               ,[ViewType]\
                               ,[OrderInd]\
                               ,[UpdatedDate]\
                               ,[ServerName] )\
                                VALUES \
                                      (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

            self.cnxn.execute(insertTxt,dictCols[0],
                                         dictCols[1],
                                         dictCols[2],
                                         dictCols[3],
                                         dictCols[4],
                                         dictCols[5],
                                         dictCols[6],
                                         dictCols[7],
                                         dictCols[8],
                                         dictCols[9],
                                         dictCols[10],
                                         dictCols[11],
                                        dictCols[12], dictCols[13], dictCols[14], dictCols[15],dictCols[16], dictCols[17],
                                    dictCols[18], dictCols[19],dictCols[20],dictCols[21])

            # self.cursor.commit()
        except Exception as e:
            log.error(e)

    def LoadLineageObject(self,df):

        log.info('Start loading lineageObject table..')
        # self.DeleteLineageFlow(df)
        try:
            dfSource = df[
                ['ConnectionID', 'ConnLogicName', 'ContainerObjectName', 'ContainerObjectPath', 'ContainerObjectType',
                 'ContainerToolType', 'ControlFlowName', 'ControlFlowPath',  'SourceIsObjectData']]
            dfSource = dfSource[dfSource['SourceIsObjectData'] == "1"]

            dfSource=dfSource.drop(['SourceIsObjectData'], axis=1)

            dfSource = dfSource.drop_duplicates()
            # log.info(tabulate(dfSource, headers='keys', tablefmt='psql'))

            dfTarget = df[
                ['ConnectionID', 'ConnLogicName', 'ContainerObjectName', 'ContainerObjectPath', 'ContainerObjectType',
                 'ContainerToolType', 'ControlFlowName', 'ControlFlowPath', 'TargetIsObjectData']]
            dfTarget = dfTarget[dfTarget['TargetIsObjectData'] == "1"]

            dfTarget=dfTarget.drop(['TargetIsObjectData'], axis=1)

            dfTarget=dfTarget.drop_duplicates()
            # log.info(tabulate(dfTarget, headers='keys', tablefmt='psql'))

            frames=[dfSource,dfTarget]
            dfMaps=pd.concat(frames)
            dfMaps=dfMaps.drop_duplicates()

            # log.info(tabulate(dfMaps, headers='keys', tablefmt='psql'))
            tableName='LINEAGEOBJECT'
            ##Delete before
            # for index, row in dfMaps.iterrows():
            #
            #     try:
            #         ObjectID =  row["ConnectionID"]+row["ControlFlowPath"]
            #         ConnectionID=row['ConnectionID']
            #         self.cursor.execute(
            #             f'delete from {self.schema}.{tableName} where ConnectionID= ? and ObjectID=? '
            #             ,( ConnectionID,ObjectID))
            #
            #         self.cursor.commit()
            #     except Exception as e:
            #         print(e)

            # ETL

            dfLineageObject = dfMaps[['ConnectionID']]
            dfLineageObject["ConnLogicName"] = dfMaps["ConnLogicName"]
            dfLineageObject["ToolType"] = 'ETL'
            dfLineageObject["ToolName"] = dfMaps["ContainerToolType"]
            dfLineageObject["ObjectID"] = dfMaps["ConnectionID"] + dfMaps["ContainerObjectPath"]
            dfLineageObject["ObjectID"]=dfLineageObject["ObjectID"].apply( lambda x: x.upper())
            dfLineageObject["ObjectGUID"] =  [hashlib.md5(val.encode('utf_16_le')).hexdigest() for val in dfLineageObject['ObjectID']]
            dfLineageObject["ObjectName"] = dfMaps["ControlFlowPath"]
            dfLineageObject["ObjectType"] = dfMaps["ContainerObjectType"]
            dfLineageObject["Folder"] = dfMaps["ContainerObjectPath"]
            dfLineageObject["ObjectContainer"] = dfMaps["ContainerObjectName"]
            dfLineageObject["ProjectName"] = dfMaps["ConnLogicName"]
            dfLineageObject["Model"] = dfMaps["ConnLogicName"]
            dfLineageObject["DatabaseName"] = ''
            dfLineageObject["SchemaName"] = ''
            dfLineageObject["IsFaked"] = 0
            dfLineageObject["ISDatabase"] = 0
            dfLineageObject["Definition"] = ""
            dfLineageObject["ModuleType"] = "ETL"
            dfLineageObject["ViewType"] = ""
            dfLineageObject["OrderInd"] = ""
            dfLineageObject["UpdatedDate"] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            dfLineageObject["ServerName"]=""
            tableName = "LINEAGEOBJECT"

            self.dfPopAutoComplete(dfLineageObject, "MNG_SQLS_AUTO_COMPLETE")


            dfLineageObject=dfLineageObject[["ConnectionID","ConnLogicName","ToolType","ToolName","ObjectGUID","ObjectID",
                                             "ObjectName","ObjectType","Folder","ObjectContainer","ProjectName","Model",
                                             "DatabaseName","SchemaName","IsFaked","ISDatabase","Definition","ModuleType","ViewType","OrderInd",
                                             "UpdatedDate","ServerName"
                                             ]]

            for i, row in dfLineageObject.iterrows():

                result=self.cnxn.execute("SELECT ObjectGUID FROM TI.LINEAGEOBJECT where ObjectGUID=?",row['ObjectGUID'])
                myresult = result.fetchall()
                if len(myresult) == 0:
                    #dfLineageObject.to_sql(tableName, self.engine, schema=self.schema, if_exists='append', index=False)

                    lst=[row['ConnectionID'],
                         row['ConnLogicName']
                                   ,row['ToolType']
                                   ,row['ToolName']
                                   ,row['ObjectGUID']
                                   ,row ['ObjectID']
                                   ,row['ObjectName']
                                   ,row['ObjectType']
                                   ,row['Folder']
                                   ,row['ObjectContainer']
                                   ,row['ProjectName']
                                   ,row['Model']
                                   ,row['DatabaseName']
                                   ,row['SchemaName']
                                   ,row['IsFaked']
                                   ,row['ISDatabase']
                                   ,row['Definition']
                                   ,row['ModuleType']
                                   ,row['ViewType']
                                   ,row['OrderInd']
                                   ,row['UpdatedDate']
                                   ,row['ServerName']]


                    self.InsertStatmentLoadObject("TI.LINEAGEOBJECT",lst)



            ##Sources
            dfSource = df[
                ['ConnectionID', 'ConnLogicName','SourceServer', 'SourceDB','SourceSchema','SourceTableName','SourceObjectType', 'SourceIsObjectData','ContainerObjectName']]
            dfSource = dfSource[dfSource['SourceIsObjectData'] == "1"]

            dfSource = dfSource.drop_duplicates()


            dfLineageObject = dfSource[['ConnectionID']]
            dfLineageObject["ConnLogicName"] = dfSource["ConnLogicName"]
            dfLineageObject["ToolType"] = 'DATABASE'
            dfLineageObject["ToolName"] = dfSource['SourceDB'].apply(lambda x: 'FILE' if  str(x)=='-1' else 'DATABRICKS')

            dfLineageObject["ObjectID"]=dfSource['SourceServer']+ dfSource["SourceDB"]+dfSource["SourceSchema"] + dfSource["SourceTableName"]
            dfLineageObject["ObjectID"]=dfLineageObject["ObjectID"].apply( lambda x: x.upper())
            dfLineageObject["ObjectGUID"] = [hashlib.md5(val.encode('utf_16_le')).hexdigest() for val in dfLineageObject['ObjectID']]
            dfLineageObject["ObjectName"] = dfSource["SourceTableName"]
            dfLineageObject["ObjectType"] = dfSource["SourceObjectType"]
            dfLineageObject["Folder"] = dfSource["SourceDB"]+'.'+dfSource["SourceSchema"]
            dfLineageObject["ObjectContainer"] =dfSource["SourceDB"]+'.'+dfSource["SourceSchema"]
            dfLineageObject["ProjectName"] = dfSource["SourceDB"]
            dfLineageObject["Model"] = dfSource["SourceDB"]
            dfLineageObject["DatabaseName"] = dfSource["SourceDB"]
            dfLineageObject["SchemaName"] = dfSource["SourceSchema"]
            dfLineageObject["IsFaked"] = 0
            dfLineageObject["ISDatabase"] = 1
            dfLineageObject["Definition"] = ""
            dfLineageObject["ModuleType"] =  np.where(dfSource['SourceDB']=='-1','ETL', 'DATABASE')
            dfLineageObject["ViewType"] = ""
            dfLineageObject["OrderInd"] = ""
            dfLineageObject["UpdatedDate"] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            dfLineageObject["ServerName"]=""
            tableName = "LINEAGEOBJECT"

            dfLineageObject = dfLineageObject[
                ["ConnectionID", "ConnLogicName", "ToolType", "ToolName", "ObjectGUID", "ObjectID",
                 "ObjectName", "ObjectType", "Folder", "ObjectContainer", "ProjectName", "Model",
                 "DatabaseName", "SchemaName", "IsFaked","ISDatabase", "Definition", "ModuleType", "ViewType", "OrderInd",
                 "UpdatedDate",
                 "ServerName"
                 ]]

            for i, row in dfLineageObject.iterrows():

                result=self.cnxn.execute("SELECT ObjectGUID FROM TI.LINEAGEOBJECT where ObjectGUID=?", row['ObjectGUID'])
                myresult = result.fetchall()
                if len(myresult) == 0:
                    # dfLineageObject.to_sql(tableName, self.engine, schema=self.schema, if_exists='append', index=False)

                    lst = [row['ConnectionID'],
                           row['ConnLogicName']
                        , row['ToolType']
                        , row['ToolName']
                        , row['ObjectGUID']
                        , row['ObjectID']
                        , row['ObjectName']
                        , row['ObjectType']
                        , row['Folder']
                        , row['ObjectContainer']
                        , row['ProjectName']
                        , row['Model']
                        , row['DatabaseName']
                        , row['SchemaName']
                        , row['IsFaked']
                        , row['ISDatabase']
                        , row['Definition']
                        , row['ModuleType']
                        , row['ViewType']
                        , row['OrderInd']
                        , row['UpdatedDate']
                        , row['ServerName']]

                    self.InsertStatmentLoadObject("TI.LINEAGEOBJECT", lst)


            # self.cursor.execute("SELECT ObjectGUID FROM TI.LINEAGEOBJECT where ObjectGUID=?",dfLineageObject.iloc[0]['ObjectGUID'])
            #
            # myresult = self.cursor.fetchall()
            # if len(myresult)==0:
            #     dfLineageObject.to_sql(tableName, self.engine, schema=self.schema, if_exists='append', index=False)

            tableName="MNG_LINEAGE_UI_DATABASE"
            self.dfMngLineageUiDatabase(dfLineageObject, tableName)

            ##Targets
            dfTarget = df[
                ['ConnectionID', 'ConnLogicName', 'TargetServer', 'TargetDB', 'TargetSchema', 'TargetTableName','TargetObjectType','ContainerObjectName',
                 'TargetIsObjectData']]
            dfTarget = dfTarget[dfTarget['TargetIsObjectData'] == "1"]
            dfTarget = dfTarget.drop_duplicates()

            dfLineageObject = dfTarget[['ConnectionID']]
            dfLineageObject["ConnLogicName"] = dfTarget["ConnLogicName"]
            dfLineageObject["ToolType"] = 'DATABASE'
            dfLineageObject["ToolName"] = dfTarget['TargetDB'].apply(
                lambda x: 'FILE' if str(x) == '-1' else 'DATABRICKS')


            dfLineageObject["ObjectID"]= dfTarget['TargetServer'] + dfTarget["TargetDB"] + dfTarget["TargetSchema"] + \
                                          dfTarget["TargetTableName"]
            dfLineageObject["ObjectID"] = dfLineageObject["ObjectID"].apply( lambda x: x.upper())
            dfLineageObject["ObjectGUID"] = [hashlib.md5(val.encode('utf_16_le')).hexdigest() for val in dfLineageObject['ObjectID']]
            dfLineageObject["ObjectName"] = dfTarget["TargetTableName"]
            dfLineageObject["ObjectType"] = dfTarget["TargetObjectType"]
            dfLineageObject["Folder"] = dfTarget["TargetDB"] + '.' + dfTarget["TargetSchema"]
            dfLineageObject["ObjectContainer"] = dfTarget["TargetDB"] + '.' + dfTarget["TargetSchema"]
            dfLineageObject["ProjectName"] = dfTarget["TargetDB"]
            dfLineageObject["Model"] = dfTarget["TargetDB"]
            dfLineageObject["DatabaseName"] = dfTarget["TargetDB"]
            dfLineageObject["SchemaName"] = dfTarget["TargetSchema"]
            dfLineageObject["IsFaked"] = 0
            dfLineageObject["ISDatabase"] = 1
            dfLineageObject["Definition"] = ""
            dfLineageObject["ModuleType"] = np.where(dfTarget['TargetDB'] == '-1', 'ETL', 'DATABASE')
            dfLineageObject["ViewType"] = ""
            dfLineageObject["OrderInd"] = ""
            dfLineageObject["UpdatedDate"] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            dfLineageObject["ServerName"]=""
            tableName = "LINEAGEOBJECT"

            dfLineageObject = dfLineageObject[
                ["ConnectionID", "ConnLogicName", "ToolType", "ToolName", "ObjectGUID", "ObjectID",
                 "ObjectName", "ObjectType", "Folder", "ObjectContainer", "ProjectName", "Model",
                 "DatabaseName", "SchemaName", "IsFaked","ISDatabase", "Definition", "ModuleType", "ViewType", "OrderInd",
                 "UpdatedDate",
                 "ServerName"
                 ]]

            result=self.cnxn.execute("SELECT ObjectGUID FROM TI.LINEAGEOBJECT where ObjectGUID=?",
                                dfLineageObject.iloc[0]['ObjectGUID'])
            myresult = result.fetchall()
            if len(myresult) == 0:
                dfLineageObject.to_sql(tableName, self.engine, schema=self.schema, if_exists='append', index=False,method='multi')

            log.info("dfLineageObject:")
            # log.info(tabulate(dfLineageObject, headers='keys', tablefmt='psql'))

            tableName = "MNG_LINEAGE_UI_DATABASE"
            self.dfMngLineageUiDatabase(dfLineageObject, tableName)





        except Exception as e:
            log.error(e)

    def dfMngLineageUiDatabase(self, dfLineageObject, tableName):

        #InsertStatmentLoadObject

        try:
            dfMNG_LINEAGE_UI_DATABASE = pd.DataFrame()
            dfMNG_LINEAGE_UI_DATABASE["CONNECTION_ID"] = dfLineageObject["ConnectionID"]
            dfMNG_LINEAGE_UI_DATABASE["CONN_LOGIC_NAME"] = dfLineageObject["ConnLogicName"]
            dfMNG_LINEAGE_UI_DATABASE["SYS_UPDATE_DATE"] = dfLineageObject["UpdatedDate"]
            dfMNG_LINEAGE_UI_DATABASE["DATABASE_TYPE"] = dfLineageObject["ObjectType"].apply(
                lambda x: 'FILE' if str(x) == 'FILE' else 'DATABRICKS')
            dfMNG_LINEAGE_UI_DATABASE["DATABASE_NAME"] = dfLineageObject["DatabaseName"]
            dfMNG_LINEAGE_UI_DATABASE["SCHEMA_NAME"] = dfLineageObject["SchemaName"]
            dfMNG_LINEAGE_UI_DATABASE["SERVER_NAME"] = dfLineageObject["ServerName"]
            dfMNG_LINEAGE_UI_DATABASE["OBJECT_NAME"] = dfLineageObject["ObjectName"]
            dfMNG_LINEAGE_UI_DATABASE["OBJECT_TYPE"] = dfLineageObject["ObjectType"]
            dfMNG_LINEAGE_UI_DATABASE["GROUP_OBJECT_TYPE"] = dfLineageObject["ObjectType"]
            dfMNG_LINEAGE_UI_DATABASE["TEXT"] = ''

            for i, row in dfMNG_LINEAGE_UI_DATABASE.iterrows():

                result=self.cnxn.execute(f"SELECT *  FROM {self.schema }.{tableName} where  CONNECTION_ID=? and DATABASE_NAME=? and "
                                    f"SCHEMA_NAME=? and OBJECT_NAME=?",
                                    row['CONNECTION_ID'],
                                    row['DATABASE_NAME'],
                                    row['SCHEMA_NAME'],
                                    row['OBJECT_NAME'])


                myresult = result.fetchall()

                if len(myresult) == 0:
                    dictCols=[row['CONNECTION_ID'],row['CONN_LOGIC_NAME'],row['SYS_UPDATE_DATE'],row['DATABASE_TYPE'],row['DATABASE_NAME'],
                              row['SCHEMA_NAME'],row['SERVER_NAME'],row['OBJECT_NAME'],row['OBJECT_TYPE'],row['GROUP_OBJECT_TYPE'],row['TEXT']]

                    self.InsertStatmentMngLineageUIDatabase('TI.MNG_LINEAGE_UI_DATABASE',dictCols)

                    # dfMNG_LINEAGE_UI_DATABASE.to_sql(tableName, self.engine, schema=self.schema, if_exists='append', index=False)



        except Exception as e:
            log.error(e)

    def dfPopAutoComplete(self, dfLineageObject, tableName):
        try:
            dfAutoComplete = pd.DataFrame()
            dfAutoComplete["CONNECTION_ID"] = dfLineageObject['ConnectionID']
            dfAutoComplete["CONN_LOGIC_NAME"] = dfLineageObject['ConnLogicName']
            dfAutoComplete["SYS_UPDATE_DATE"] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            dfAutoComplete["TABLE_NAME"] = dfLineageObject["ObjectName"]
            dfAutoComplete["TABLE_TYPE"] = dfLineageObject["ObjectType"]
            dfAutoComplete["TOOL_TYPE"] = dfLineageObject["ToolType"]
            dfAutoComplete["UPDATED_DATE"] = strftime("%Y-%m-%d %H:%M:%S", gmtime())

            result=self.cnxn.execute("SELECT CONNECTION_ID FROM TI.MNG_SQLS_AUTO_COMPLETE where TABLE_NAME=? and CONNECTION_ID=?",
                                    dfAutoComplete.iloc[0]['TABLE_NAME'], dfAutoComplete.iloc[0]['CONNECTION_ID'])

            myresult = result.fetchall()

            if len(myresult) == 0:
                dfAutoComplete.to_sql(tableName, self.engine, schema=self.schema, if_exists='append', index=False,method='multi')
        except Exception as e:
                log.error(e)



    def Upsert2Tables(self,sourceTable,targetTable):

        try:

            statement=f"MERGE {targetTable} AS TARGET \n \
                       USING {sourceTable} AS SOURCE  \n \
                      ON (TARGET.ObjectGUID = SOURCE.ObjectGUID) \n  \
                      --When records are matched, update the records if there is any change \n \
                      WHEN MATCHED AND TARGET.ObjectName <> SOURCE.ObjectName  \n \
                      THEN UPDATE SET TARGET.ObjectName = SOURCE.ObjectName  \n \
                      --When no records are matched, insert the incoming records from source table to target table \n \
                       WHEN NOT MATCHED BY TARGET \n \
                       THEN INSERT ([ConnectionID],\n \
                       [ConnLogicName],[ToolType], \n \
                       [ToolName],[ObjectGUID], \n \
                       [ObjectID],[ObjectName], \n \
                       [ObjectType],[Folder],\n \
                       [ObjectContainer], \n \
                       [ProjectName], \n \
                       [Model] \n \
                       ,[DatabaseName] \n \
                       ,[SchemaName] \n \
                       ,[IsFaked] \n \
                       ,[ISDatabase] \n \
                       ,[Definition] \n \
                       ,[ModuleType] \n \
                       ,[ViewType] \n \
                       ,[OrderInd] \n \
                       ,[UpdatedDate] \n \
                       ,[ServerName]) VALUES (\n \
                        SOURCE.[ConnectionID] \n\
                       ,SOURCE.[ConnLogicName] \n\
                       ,SOURCE.[ToolType] \n\
                       ,SOURCE.[ToolName] \n\
                       ,SOURCE.[ObjectGUID] \n\
                       ,SOURCE.[ObjectID] \n\
                       ,SOURCE.[ObjectName] \n\
                       ,SOURCE.[ObjectType] \n \
                       ,SOURCE.[Folder] \n \
                       ,SOURCE.[ObjectContainer] \n \
                       ,SOURCE.[ProjectName] \n \
                       ,SOURCE.[Model] \n \
                       ,SOURCE.[DatabaseName] \n \
                       ,SOURCE.[SchemaName] \n \
                       ,SOURCE.[IsFaked] \n \
                       ,SOURCE.[ISDatabase]\n \
                       ,SOURCE.[Definition] \n \
                       ,SOURCE.[ModuleType] \n\
                       ,SOURCE.[ViewType] \n \
                       ,SOURCE.[OrderInd] \n\
                       ,cast (SOURCE.[UpdatedDate] as datetime2) \n \
                       ,SOURCE.[ServerName]);"


            # log.info(statement)
            self.cnxn.execute(statement)

            # self.cursor.commit()
        except Exception as e:
                     log.error(e)


    def Upsert2TablesLineageFlow(self,sourceTable,targetTable):

        try:

            statement=f"MERGE {targetTable} AS TARGET  \n  \
                       USING {sourceTable} AS SOURCE  \n \
                      ON (TARGET.SourceObjectGUID = SOURCE.SourceObjectGUID  AND TARGET.TargetObjectGUID = SOURCE.TargetObjectGUID ) \n  \
                      --When records are matched, update the records if there is any change \n\
                      WHEN MATCHED AND TARGET.SourceObjectName <> SOURCE.SourceObjectName AND  TARGET.TargetObjectName <> SOURCE.TargetObjectName \n  \
                      THEN UPDATE SET TARGET.SourceObjectName = SOURCE.SourceObjectName,TARGET.TargetObjectName = SOURCE.TargetObjectName \n \
                      --When no records are matched, insert the incoming records from source table to target table  \n \
                      WHEN NOT MATCHED BY TARGET  \n \
                      THEN INSERT  ([ConnectionID] \n\
                               ,[ToolName] \n \
                               ,[SourceToolType] \n \
                               ,[SourceObjectID] \n\
                               ,[SourceObjectGUID] \n\
                               ,[SourceObjectName] \n\
                               ,[TargetObjectID] \n\
                               ,[TargetObjectGUID] \n\
                               ,[TargetObjectName] \n\
                               ,[LinkType] \n\
                               ,[LinkDescription] \n\
                               ,[LinkedGUID] \n\
                               ,[ISSourceInd] \n\
                               ,[TargetToolType] \n\
                               ,[SourceID] \n\
                               ,[TargetID] \n \
                               ,[UpdatedDate]) \n \
                    VALUES ( SOURCE.[SourceConnectionID],  \n \
                                SOURCE.[SourceToolName] , \n \
                                SOURCE.[SourceToolType] , \n \
                               SOURCE.[SourceObjectID], \n \
                               SOURCE.[SourceObjectGUID], \n\
                               SOURCE.[SourceObjectName], \n\
                               SOURCE.[TargetObjectID], \n\
                               SOURCE.[TargetObjectGUID], \n\
                               SOURCE.[TargetObjectName], \n\
                               SOURCE.[LinkType], \n\
                               SOURCE.[LinkDescription],\n\
                               SOURCE.[LinkedGUID], \n\
                               SOURCE.[ISSourceInd], \n\
                               SOURCE.[TargetToolType], \n\
                               SOURCE.[SourceID], \n\
                               SOURCE.[TargetID], \n\
                               cast (SOURCE.[UpdatedDate] as datetime2));"



            # log.info(statement)
            self.cnxn.execute(statement)
            # self.cursor.commit()
        except Exception as e:
                     log.error(e)

    def DropTableIfExists(self,tableName,schemaName):
        try:
            stmt=f'IF  EXISTS(SELECT * FROM  INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME =" {tableName}" AND TABLE_SCHEMA =" {schemaName}") DROP TABLE {tableName}.{schemaName};'
            self.cnxn.execute(stmt)
            # self.cursor.commit()
        except Exception as e:
            log.error(e)


