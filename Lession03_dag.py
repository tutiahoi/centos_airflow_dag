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


DAG_ID = "Insert_data"
# run with time zone
local_tz = pendulum.timezone("Asia/Vientiane")
#### lay parameter -> ariable
fromdate = Variable.get("ETL_Order_fromdate", deserialize_json=False)

if not fromdate:
    fromdate =  datetime.now().strftime('%Y-%m-%d')
else:
    fromdate = fromdate

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
    schedule_interval = '20 16 * * *',
    tags=["ETLdata"],
    #default_args = default_args,
) 
#as dag:
        
 
    
#@dag.task(task_id="Select_OrderTbl")
def Select_OrderTbl_Hook():
        mssql_hook = MsSqlHook(mssql_conn_id="sqlserver_connect_source", schema="DEMO")

        sqlstr = f"select top 1 AccountID from OrderTbl where ValueDate ='{fromdate}'" 
        
        df = mssql_hook.get_pandas_df(sql = sqlstr)

        if len(df.index) > 0:
             return 1
        else:
            return 0
        

def Insert_SummaryCustomerTbl_Hook():
            mssql_hook = MsSqlHook(mssql_conn_id="sqlserver_connect_source", schema="DEMO")

            sqlstr = "select Format(ValueDate,'yyyy-MM-dd') as ValueDate, AccountID , InstrumentID, Type," + \
                " Sum(SoLuong) as SoLuong, avg(Gia) as Gia, Sum(Tien) as Tien, Sum(Phi) as Phi, Sum(Thue) as Thue" + \
                f" from OrderTbl where ValueDate ='{fromdate}'" +  \
                " group by ValueDate, AccountID , InstrumentID, Type "
        
            df = mssql_hook.get_pandas_df(sql = sqlstr)
            
            target_fields = ["ValueDate", "AccountID", "InstrumentID", "Type", "SoLuong", "Gia", "Tien", "Phi", "Thue"]
            mssql_hook.insert_rows(table="SummaryCustomerTbl", 
                                rows =df.values.tolist(), 
                                target_fields=target_fields,
                                commit_every=2
                                )  
            return "Insert SummaryCustomerTbl"
#### mail 
send_email_error = EmailOperator( 
    task_id='send_email', 
    to='noinq@outlook.com', 
    subject='Notification dags', 
    html_content="<h3>Insert SummaryCustomerTbl error!</h3>"
)

#### dag start
Dag_Start = DummyOperator(
    task_id='Start_' + fromdate ,
    dag=dag
)
#### dag check exists data order
Dag_Notexistsdata = DummyOperator(
    task_id='Notexistsdata',
    dag=dag
)
#### dag insert 
Dag_Insert_SummaryCustomerTbl = PythonOperator(
    task_id='Insert_SummaryCustomerTbl',
    python_callable= Insert_SummaryCustomerTbl_Hook,
    dag = dag
    )


def choose_branch():
    a= Select_OrderTbl_Hook()
    print("can in " + fromdate + " row " + str(a))
    if a > 0:
        return ['Insert_SummaryCustomerTbl']
    else:
        return ['Notexistsdata']

choose_best_model = BranchPythonOperator(
    task_id='choose_best_model',
    python_callable=choose_branch
)
#### exce job tinh ton cuoi tk
def CalculatorStockCustomer():
    mssql_hook = MsSqlHook(mssql_conn_id="sqlserver_connect_source", schema="DEMO") 
    strsql = " Exec dbo.SP_CalculatorStockCustomer '{0}',{1}".format(fromdate,'null')    
    mssql_hook.run(strsql)
    print(strsql)
    return ""

#### Dag ton cuoi ngay tk
Dag_CalculatorStockCustomer = PythonOperator(
    task_id='CalculatorStockCustomer',
    python_callable= CalculatorStockCustomer,
    dag = dag
    )

#### exec job tinh du cuoi ngay tk
def CalculatorBalanceCustomer():
    mssql_hook = MsSqlHook(mssql_conn_id="sqlserver_connect_source", schema="DEMO") 
    strsql = " Exec dbo.SP_CalculatorBalanceCustomer '{0}',{1}".format(fromdate,'null')    
    mssql_hook.run(strsql)
    print(strsql)
    return ""
#### Dag du cuoi ngay tk
Dag_CalculatorBalanceCustomer = PythonOperator(
    task_id='CalculatorBalanceCustomer',
    python_callable= CalculatorBalanceCustomer,
    dag = dag
    )
     
Dag_Start >> choose_best_model >> [Dag_Notexistsdata, Dag_Insert_SummaryCustomerTbl] 
Dag_Insert_SummaryCustomerTbl >> [Dag_CalculatorStockCustomer,Dag_CalculatorBalanceCustomer]
