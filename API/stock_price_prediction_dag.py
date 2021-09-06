from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

## Projects
import pandas as pd
import numpy as np
import joblib
import os
import time
import datetime
from pytz import timezone
import configparser as cp
from sqlalchemy import create_engine
import mysql.connector
import yfinance as yf


## Airflow setup
yf_args={'owner':'airflow',
         'depends_on_past':False,
         'start_date':datetime.datetime(2021,9,1),
         'retries':3,
         'retry_delay':timedelta(minutes=5)
        }


## db connection ##
config=cp.ConfigParser()
config.read('/home/ubuntu/certi/db_login.txt')
db_config=config['ivan_db']
# conn -1
engine=create_engine('mysql+mysqlconnector://{0:s}:{1:s}@{2:s}/{3:s}'.format(db_config['userid'],
                                                                             db_config['pwd'],
                                                                             db_config['hostname'],
                                                                             'STOCK_PRED'))
# conn -2
mydb=mysql.connector.connect(host=db_config['hostname'].replace(':3306',''),
                             user=db_config['userid'],
                             password=db_config['pwd'],
                             database='STOCK_PRED')
cursor=mydb.cursor()

###############
## Functions 
###############
def fetch_stockprice_all(stock_exchange=['NASDAQ','NYSE'],start_vals=None,n_sample=None):
    stock_list=[]
    ## Loading tickers ##
    all_tickers=pd.read_csv('/home/ubuntu/projects/Stock_Price_Prediction/data/NYSE_NASDAQ_Aug4_2021.csv')
    
    ## params ##
    if start_vals==None:
        period_vals='max'
    else:
        period_vals=None
    if n_sample==None:
        all_tickers=all_tickers
    else:
        all_tickers=all_tickers.sample(n=n_sample)
    
    ## fetching data ##
    count=1
    
    for s in stock_exchange:
        for i in all_tickers.loc[all_tickers.SE==s,'Symbol'].tolist():
            if count%50==0:
                time.sleep(3)
                print('Wait every 50 queries; Progress: {:.2f}%'.format(count/all_tickers.shape[0]*100))
            
            df=yf.download(tickers=i,
                           periods=period_vals,
                           start=start_vals,
                           interval='1d',
                           groupby='ticker',
                           auto_adjust=True,
                           prepose=False,
                           threads=True,
                           proxy=None
                          ).reset_index()
            df['SE']=s
            df['Stock']=i
            stock_list.append(df)
            
            count+=1
    return pd.concat(stock_list)


###############
## Major 
###############
def update_all_stocks_2():
    
    utc_tz=datetime.datetime.now()
    # input new data into temp 
    # do minus
    # insert new data
    ## db connection
#    config=cp.ConfigParser()
#    config.read('/home/ubuntu/certi/db_login.txt')
#    db_config=config['ivan_db']
    # conn -1
#    engine=create_engine('mysql+mysqlconnector://{0:s}:{1:s}@{2:s}/{3:s}'.format(db_config['userid'],
#                                                                                   db_config['pwd'],
#                                                                                   db_config['hostname'],
#                                                                                 'STOCK_PRED'))
    # conn -2
#    mydb=mysql.connector.connect(host=db_config['hostname'].replace(':3306',''),
#                                 user=db_config['userid'],
#                                 password=db_config['pwd'],
#                                 database='STOCK_PRED')
#    cursor=mydb.cursor()
    
    ## Get the start date:
    hist_df=pd.read_sql("SELECT * FROM ALL_STOCK_HIST ORDER BY Date DESC LIMIT 1", con=engine)
    hist_last_day=hist_df.loc[0,'Date']
    print('Latest data in the database: {:s}'.format(hist_last_day.strftime(format='%Y-%m-%d')))
    
    if utc_tz.astimezone(timezone('America/New_York')).date()>hist_last_day:
        if utc_tz.astimezone(timezone('America/New_York')).weekday()+1<=5:
            ## fetch the latest data
            latest_data=fetch_stockprice_all(start_vals=hist_last_day+timedelta(days=1))
            
            if (latest_data.shape[0]/latest_data.Date.nunique())>=5000:
                latest_data.loc[:,'REFRESH_DATE']=datetime.datetime.now()
                latest_data=latest_data.loc[:,['Date','Open','High','Low','Close','Volume','SE','Stock','REFRESH_DATE']]
        
                # additional checking
                latest_data_2=latest_data.loc[latest_data.Date>hist_last_day,:].reset_index(drop=True)
    
                ## Load to temp table 
                latest_data_2.to_sql(name='ALL_STOCK_LATEST', # ALL_STOCK_LATEST
                                     con=engine,
                                     if_exists='replace',
                                     index=False,
                                     chunksize=1000)
                print('ALL_STOCK_LATEST is updated with data as of {:s}, rows updated: {:,.0f}'.format(latest_data_2.Date.max().strftime(format='%Y-%m-%d'),
                                                                                                       latest_data_2.shape[0])
                     )
    
                cursor.execute("""INSERT INTO ALL_STOCK_HIST 
                                  SELECT *
                                  FROM ALL_STOCK_LATEST 
                                  COMMIT
                                """)
                mydb.commit()
                print('ALL_STOCK_HIST is updated with data as of {:s}'.format(latest_data_2.Date.max().strftime(format='%Y-%m-%d')))
            
            else:
                print('Stock data not available; there are {:,.0f} NA records'.format(latest_data.Open.isnull().sum()))
        
        else:
            print('Not weekday')
    
    else:
        print('ALL_STOCK_HIST is already up-to-date, latest data: {:s}'.format(hist_last_day.strftime(format='%Y-%m-%d')))
    


###############
## Task 
###############
with DAG('Stock_Price_Prediction',
         default_args=yf_args,
         schedule_interval='20 0,2,22 * * *',
         catchup=False
        ) as dag:
    t_data_load=PythonOperator(task_id='Yahoo_Finance_Data_Pull',python_callable=update_all_stocks_2)

    
    t_data_load

# use with: https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html
