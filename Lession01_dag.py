from datetime import datetime, timedelta
import pendulum
import pandas as pd
import airflow 
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
#from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator

DAG_ID = "Delete_Summary_Order"
# run with time zone
local_tz = pendulum.timezone("Asia/Vientiane")
#### lay parameter -> ariable
deletedate = Variable.get("ETL_Order_Deleted", deserialize_json=False)

if not deletedate:
    deletedate =  datetime.now().strftime('%Y-%m-%d')
else:
    deletedate = deletedate

default_args = {
    'owner' :'admin',
    'retries':1,
    'retry_delay': timedelta(minutes=5),
    'email': ["noinq@outlook.com"],
    #'email_on_failure': True,
    # 'email_on_retry': True,
}

dag = DAG(
    DAG_ID,
    start_date = datetime(2023, 9 ,12, tzinfo = local_tz),
    schedule_interval = '18 16 * * *',
    tags=["ETLdata"],
    #default_args = default_args,
) 

#### dag start
Dag_Start = DummyOperator(
    task_id='Start' + deletedate ,
    dag=dag
)



def Deleted_BalanceCustomerTbl_Hook():
        mssql_hook = MsSqlHook(mssql_conn_id="sqlserver_connect_source", schema="DEMO")

        strsql = f" DELETE BalanceCustomerTbl where ValueDate ='{deletedate}'" 
        print(strsql)
        mssql_hook.run(strsql)
        
        return ""
def Deleted_StockCustomerTbl_Hook():
        mssql_hook = MsSqlHook(mssql_conn_id="sqlserver_connect_source", schema="DEMO")

        strsql = f" DELETE StockCustomerTbl where ValueDate ='{deletedate}'" 
        print(strsql)
        mssql_hook.run(strsql)
        
        return ""

def Deleted_SummaryCustomerTbl_Hook():
        mssql_hook = MsSqlHook(mssql_conn_id="sqlserver_connect_source", schema="DEMO")

        strsql = f" DELETE SummaryCustomerTbl where ValueDate ='{deletedate}'" 
        print(strsql)
        mssql_hook.run(strsql)
        
        return ""
#### dag deleted 
Dag_Deleted_BalanceCustomer = PythonOperator(
    task_id='Deleted_BalanceCustomer',
    python_callable= Deleted_BalanceCustomerTbl_Hook,
    dag = dag
    )

Dag_Deleted_StockCustomer = PythonOperator(
    task_id='Deleted_StockCustomerTbl',
    python_callable= Deleted_StockCustomerTbl_Hook,
    dag = dag
    )

Dag_Deleted_SummaryCustomer = PythonOperator(
    task_id='Deleted_SummaryCustomer',
    python_callable= Deleted_SummaryCustomerTbl_Hook,
    dag = dag
    )    
#model dag 3    
Dag_Start >> Dag_Deleted_BalanceCustomer
Dag_Start >> Dag_Deleted_StockCustomer
Dag_Start >> Dag_Deleted_SummaryCustomer