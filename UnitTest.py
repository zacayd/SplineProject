import sqlalchemy

#"sqlDBConnectionString": "Data Source=sql.us.octopai-corp.local;Initial Catalog=DigicelJM_Prod;User ID=oum-prod-us;Password=Dv45WDnqD6dV",
from sqlalchemy import create_engine

user='oum-prod-us'
pwd='Dv45WDnqD6dV'
port="1433"
database="DigicelJM_Prod"
server='sql.us.octopai-corp.local'
connection_string=f"mssql+pyodbc://{user}:{pwd}@{server}:{port}/{database}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"

connection_string = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + self.server + ';DATABASE=' + 'oumdb' + ';UID=' + self.usr + \
                    ';PWD=' + self.pwd + ';TrustServerCertificate=Yes;'
engine = create_engine(connection_string)


try:
    print(create_engine(connection_string).connect().connect())



    engine = create_engine(connection_string)

    # Test the connection by running a simple query
    result = engine.execute('SELECT 1')
    print(result.scalar())

except Exception as e:
    print(e)