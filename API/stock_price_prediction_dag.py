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
import pytz


import statsmodels.formula.api as smf
import math



## Airflow setup
yf_args={'owner':'airflow',
         'depends_on_past':False,
         'start_date':datetime.datetime(2021,9,1),
         'retries':1,
         #'retry_delay':timedelta(minutes=5)
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


# load all_tickers
all_tickers=pd.read_csv('/home/ubuntu/projects/Stock_Price_Prediction/data/NYSE_NASDAQ_Aug4_2021.csv')




###############
## Functions 
###############
def fetch_stockprice_all(stock_exchange=['NASDAQ','NYSE'],start_vals=None,n_sample=None):
    stock_list=[]
    ## Loading tickers ##
    all_tickers=pd.read_csv('/home/ubuntu/projects/Stock_Price_Prediction/data/NYSE_NASDAQ_Aug4_2021.csv')
    
    ## params ##
    #if start_vals==None:
    #    period_vals='max'
    #else:
    #    period_vals=None
    if n_sample==None:
        all_tickers=all_tickers
    else:
        all_tickers=all_tickers.sample(n=n_sample)
    
    ## fetching data ##
    count=1
    
    for s in stock_exchange:
        for i in all_tickers.loc[all_tickers.SE==s,'Symbol'].tolist():
            if count%50==0:
            #   time.sleep(3)
            #   print('Wait every 50 queries; Progress: {:.2f}%'.format(count/all_tickers.shape[0]*100))
                print('Progress: {:.2f}%'.format(count/all_tickers.shape[0]*100))

            df=yf.download(tickers=i,
                           periods='max',
                           start=start_vals,
                           interval='1d',
                           groupby='ticker',
                           #auto_adjust=True,
                           prepose=False,
                           threads=True,
                           proxy=None
                          ).reset_index()
            df['SE']=s
            df['Stock']=i
            stock_list.append(df)
            
            count+=1
    return pd.concat(stock_list)




## Regression ## 
def linear_reg_analysis_for(df):
    lr_model=smf.ols('Close ~ DATE_ORDER',data=df).fit()
    #lr_model=sm.OLS(x.Close,x.DATE_ORDER).fit()
    
    model_result={#'Stock':df.Stock[0],
                  'R_squared':[lr_model.rsquared],
                  'Coef':[lr_model.params[1]],
                  'P_values':[lr_model.pvalues[1]],
                  
                  'Start_Date':df['Date'].min(),
                  'End_Date':df['Date'].max(),
                  'Num_records':[df.shape[0]],
                  'Num_records_dist':[df.Date.nunique()]  
                 }



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
            joblib.dump(latest_data,'/home/ubuntu/projects/Stock_Price_Prediction/data/latest_stock_info.pkl')
           
           ## Pull the data as long as they are available
            if latest_data.shape[0]>0: # (latest_data.shape[0]/latest_data.Date.nunique())>=5000
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
    
## Stock Attributes ##
def pull_stock_attri(stocks=None,n_sample=None):
    if stocks is None:
        stocks=all_tickers.Symbol.unique()
    if (n_sample is not None) & (np.where(n_sample is None,0,n_sample)<len(stocks)):
        stocks=sample(set(stocks),n_sample)
        
    
    col_attri=[]
    count=0
    
    for i in stocks:
        try:
            stock_attri=yf.Ticker('{}'.format(i))
            result={'Stock':i,
                    'Exchange':stock_attri.info['exchange'],
                    'Shortname':stock_attri.info['shortName'],
                    'Longname':stock_attri.info['longName'],
                    'Currentprice':stock_attri.info['currentPrice'],
                    'Targetlowprice':stock_attri.info['targetLowPrice'],
                    'Targetmeanprice':stock_attri.info['targetMeanPrice'],
                    'Targetmedianprice':stock_attri.info['targetMedianPrice'],
                    'Targethighprice':stock_attri.info['targetHighPrice'],
                    'recommendation':stock_attri.info['recommendationKey'],
                    'Date_UTC':datetime.datetime.now()
                   }
            col_attri.append(result)
            count+=1
            if count%25==0:
                print('{:} records downloaded, {:.1f}%'.format(count,
                                                               count/len(stocks)*100
                                                             ))
        
            if count%500==0:
                time.sleep(1)
        
        except KeyError:
            continue
        
        except AttributeError:
            continue

        except Exception as ex:
            print(ex)

    col_attri_df=pd.DataFrame(col_attri)
    print('{} stocks are updated with data as of {}'.format(col_attri_df.Stock.nunique(),
                                                            col_attri_df.Date_UTC.max().strftime('%Y-%m-%d %H:%M')
                                                           ))
    
    ## Saving
    joblib.dump(col_attri_df,'/home/ubuntu/projects/Stock_Price_Prediction/data/stock_attris.pkl')
    
        
def load_stock_attri():
    col_attri_df=joblib.load('/home/ubuntu/projects/Stock_Price_Prediction/data/stock_attris.pkl')
    
    
    col_attri_df.to_sql(name='STOCK_ATTRIBUTES',
                        con=engine,
                        if_exists='replace',
                        index=False,
                        chunksize=1000)
    print('Data is updated: {:}'.format(col_attri_df.Date_UTC.min().strftime('%Y-%m-%d %H:%M:%S')))


## Recommendations ##
## Recommendations
def pull_stock_recommendatins(stocks=None,n_sample=None):
    if stocks is None:
        stocks=all_tickers.Symbol.unique()
    if (n_sample is not None) & (np.where(n_sample is None,0,n_sample)<len(stocks)):
        stocks=sample(set(stocks),n_sample)

    df_col=[]
    count=0

    for i in stocks:
        try:
            stock_info=yf.Ticker('{}'.format(i))
            df_recommends=stock_info.recommendations#.reset_index(drop=True)
            if df_recommends.empty:
                continue
            else:
                df_recommends.reset_index(drop=False,inplace=True)
                df_recommends.loc[:,'Stock']=i
                df_col.append(df_recommends)
                count+=1
            
            if count%25==0:
                print('{:} records downloaded, {:.1f}%'.format(count,
                                                               count/len(stocks)*100)
                     )
            if count%500==0:
                time.sleep(1) ## added Jan.10
        except AttributeError:
            continue

        except Exception as ex:
            print(ex)
            pass

    df_col=pd.concat(df_col)
    df_col.rename(columns={'Date':'DATE',
                           'Firm':'FIRM',
                           'To Grade':'TO_GRADE',
                           'From Grade':'FROM_GRADE',
                           'Action':'ACTION',
                           'Stock':'STOCK'},inplace=True)
    df_col.loc[:,'REFRESH_DATE']=datetime.datetime.now()

    ## saving
    joblib.dump(df_col,'/home/ubuntu/projects/Stock_Price_Prediction/data/stock_anayst_recommendations.pkl')
    print('Stcok recommendations are downloaded: {:,.0f} stocks, {:,.0f} records'.format(df_col.STOCK.nunique(),
                                                                                         df_col.shape[0]
                                                                                     ))

## Institutional holders
def pull_stock_insti_holders(stocks=None,n_sample=None):
    if stocks is None:
        stocks=all_tickers.Symbol.unique()
    if (n_sample is not None) & (np.where(n_sample is None,0,n_sample)<len(stocks)):
        stocks=samplei(set(stocks),n_sample)

    df_col=[]
    count=0

    for i in stocks:
        try:
            stock_info=yf.Ticker(i)
            df_insti_hold=stock_info.institutional_holders.reset_index(drop=True)
            
            #df_insti_hold.loc[:,'Stock']=i # df_insti_hold['Stock']=i
            #df_col.append(df_insti_hold)
            #count+=1
            if df_insti_hold.empty:
                continue
            else: 
                df_insti_hold.loc[:,'Stock']=i
                df_col.append(df_insti_hold)
                count+=1

            if count%25==0:
                print('{:} records downloaded, {:.1f}%'.format(count,
                                                               count/len(stocks)*100
                                                              ))
           # if count==5775:
           #     time.sleep(10) ## added Jan.10
        except AttributeError:
            continue

        except Exception:
            pass

    df_col=pd.concat(df_col)
    df_col.dropna(how='all',axis=1,inplace=True)
    df_col.rename(columns={'Holder':'HOLDER',
                           'Shares':'SHARES',
                           'Date Reported':'DATE_REPORTED',
                           '% Out':'PERC_OUT',
                           'Stock':'STOCK'
                          },inplace=True)
    df_col.loc[:,'REFRESH_DATE']=datetime.datetime.now()

    ## saving
    joblib.dump(df_col,'/home/ubuntu/projects/Stock_Price_Prediction/data/stock_institutional_holders.pkl')
    print('Stock institutional holders info has been downloaded: {:,.0f}, {:,.0f} records'.format(df_col.STOCK.nunique(),
                                                                                                  df_col.shape[0]
                                                                                                 ))


def pull_stock_insti_holders_test():
    df_col_2=[]
    count=0
    stocks=all_tickers.Symbol.unique()
    for i in stocks:
        try:
            stock_info=yf.Ticker(i)
            df_insti_hold=stock_info.institutional_holders.reset_index(drop=True)
            result={i:df_insti_hold}
            df_col_2.append(result)
            
            count+=1
            
            if count==5775:
                time.sleep(10)
                
            if count%50==0:
                print('{:} records downloaded, {:.1f}%'.format(count,
                                                               count/len(stocks)*100
                                                              ))
        except AttributeError:
            continue
    joblib.dump(df_col_2,'/home/ubuntu/data/stock_price_pred/df_insti_hold.pkl')





## Loading
def load_Recommends_InstiHolders():
    ## Recommendations ##
    df_recommends=joblib.load('/home/ubuntu/projects/Stock_Price_Prediction/data/stock_anayst_recommendations.pkl')
    print(df_recommends.shape)

    # load
    df_recommends.to_sql(name='STOCK_RECOMMENDS',
                         con=engine,
                         if_exists='replace',
                         index=False,
                         chunksize=1000
                        )
    print('STOCK_RECOMMENDS is updated: {:,.0f} records, {:}'.format(df_recommends.shape[0],
                                                                     df_recommends.REFRESH_DATE.min().strftime('%Y-%m-%d %H:%M:%S')
                                                                   ))

    ## Institutional Holders ##
    df_insti_hold=joblib.load('/home/ubuntu/projects/Stock_Price_Prediction/data/stock_institutional_holders.pkl')
    print(df_insti_hold.shape)

    # load
    df_insti_hold.to_sql(name='STOCK_INSTI_HOLDERS',
                         con=engine,
                         if_exists='replace',
                         index=False,
                         chunksize=1000
                        )
    print('STOCK_INSTI_HOLDERS is updated: {:,.0f} records, {:}'.format(df_insti_hold.shape[0],
                                                                        df_insti_hold.REFRESH_DATE.min().strftime('%Y-%m-%d %H:%M:%S')
                                                                       ))





## Implemment ##
## Data Cleaning
def stock_data_cleaning():
    ## 1 Data Loading ##
    # 1.1 Stock data - last 60 days
    df=pd.read_sql("""SELECT *
                      FROM STOCK_PRED.ALL_STOCK_HIST
                      WHERE DATE>=CURDATE()-INTERVAL 60 DAY
                             """,
                   con=engine)
    print(df.shape)
    print(df.Stock.nunique())
    print(df['Date'].max(),df['Date'].min())

    if df.loc[df.Date==df.Date.max(),:].shape[0]>5000:
        ## 2 Data Cleaning ##
        # 2.1 Remove NAs
        df_1=df.dropna(axis=0,how='any')

        # 2.2 Remove accounts with Negative Stock price
        negative_stocks=df_1.loc[df_1.Close<0,'Stock'].unique()
        df_1=df_1.loc[~df_1.Stock.isin(negative_stocks),:]

        # 2.3 Keep active stocks
        active_stocks=df_1.loc[df_1.Date==df_1.Date.max(),'Stock'].to_list()
        df_2=df_1.loc[df_1.Stock.isin(active_stocks),:].reset_index(drop=True)

        # 2.4 Add DATE_ORDER
        df_2.loc[:,'DATE_ORDER']=df_2.groupby('Stock').Date.transform(lambda x:x.rank(method='first',ascending=True))

        stock_mapping=pd.read_sql("""SELECT *
                                     FROM STOCK_PRED.NYSE_NASDAQ_TICKERS
                                  """,con=engine)

        # 2.4 Merging
        df_3=pd.merge(df_2,
                      stock_mapping.loc[:,['Symbol','Name','Country','IPOYear','Sector','Industry']],
                      how='left',
                      left_on='Stock',
                      right_on='Symbol'
                     )
        df_3.drop(['Symbol'],axis=1,inplace=True)
        df_3.sort_values(by=['Stock','Date'],ascending=True,inplace=True)

        ## 3. Saving ##
        joblib.dump(df_3,'/home/ubuntu/data/stock_price_pred/ALL_STOCK_L60D.pkl')
        
        print('{:,.0f} records saved, data as of {:s}'.format(df_3.shape[0],
                                                              df_3.Date.max().strftime('%Y-%m-%d')
                                                             ))

    else:
        print('Data issue: latest data {:s} only as {:,.0f} rows'.format(df.Date.max().strftime('%Y-%m-%d'),
                                                                         df.loc[df.Date==df.Date.max(),:].shape[0]
                                                                        ))





## Applying and Loading
def impl_linear_reg():
    ## 1 Data Loading & Preprocessing ##
    df_3=joblib.load('/home/ubuntu/data/stock_price_pred/ALL_STOCK_L60D.pkl')
    print(df_3.shape)
    print(df_3.Date.max())

    stock_mapping=pd.read_sql("""SELECT *
                                 FROM STOCK_PRED.NYSE_NASDAQ_TICKERS
                              """,con=engine)


    ## 2 Modeling Implementing ##
    linear_reg_sum=df_3.groupby(['Stock']).apply(linear_reg_analysis_for).reset_index(drop=False)
    print(linear_reg_sum.shape)

    ## 3. Processing ##
    # 3.1 Add new columns
    linear_reg_sum.loc[:,'WT_Coef']=linear_reg_sum.R_squared*linear_reg_sum.Coef
    linear_reg_sum.loc[:,'Model_date']=datetime.now(tz=pytz.utc).astimezone(timezone('US/Pacific'))

    # 3.2 Additional tables
    stock_strt_end_price=df_3.groupby('Stock').agg(start_price=('Close','first'),
                                                   end_price=('Close','last')
                                                  ).reset_index(drop=False)
    linear_reg_sum_2=pd.merge(linear_reg_sum,
                              stock_strt_end_price,
                              how='left',
                              on='Stock'
                             ).assign(growth_rate=lambda x:(x.end_price-x.start_price)/x.start_price)

    linear_reg_sum_2=pd.merge(linear_reg_sum_2,
                              stock_mapping.loc[:,['Symbol','Name','Industry','SE']],
                              how='left',
                              left_on='Stock',
                              right_on='Symbol')

    linear_reg_sum_2.drop('level_1',axis=1,inplace=True)

    # 3.3 Reorder columns
    linear_reg_sum_2=linear_reg_sum_2.loc[:,['Model_date',
                                             'Stock','Name','Industry',
                                             'R_squared','Coef','P_values','WT_Coef',
                                             'Start_Date','End_Date','start_price','end_price',
                                             'Num_records_dist','growth_rate']]

    print(linear_reg_sum_2.shape)


    ## 4. Load to MySQL ##
    linear_reg_sum_2.to_sql(name='LINEAR_REG_L40',
                            con=engine,
                            if_exists='append',
                            index=False,
                            chunksize=1000)

    print('{0:,.0f} records created; max stock price date: {1:s}; model implemented date: {2:s}'.format(linear_reg_sum_2.shape[0],
                                                                                                        linear_reg_sum_2.End_Date.max().strftime('%Y-%m-%d'),
                                                                                                        linear_reg_sum_2.Model_date.min().strftime('%Y-%m-%d %H:%M:%S')
                                                                                                       ))




###############
## Task 
###############
with DAG('Stock_Price_Prediction',
         default_args=yf_args,
         schedule_interval='30 1 * * 2-6',
         catchup=False
        ) as dag:
    yf_data_load=PythonOperator(task_id='Yahoo_Finance_Data_Pull',python_callable=update_all_stocks_2)
    # attributes
    Pull_stock_attri=PythonOperator(task_id='Pull_stock_attributes',python_callable=pull_stock_attri)
    Load_stock_attri=PythonOperator(task_id='Load_stock_attributes',python_callable=load_stock_attri)
    # stock recommendation
    Pull_stock_recommendations=PythonOperator(task_id='Pull_stock_recommendations',python_callable=pull_stock_recommendatins)
    # institutional holders
    Pull_stock_insti_holders=PythonOperator(task_id='Pusll_stock_institutional_holders',python_callable=pull_stock_insti_holders)
    #Pull_stock_insti_holders_test=PythonOperator(task_id='pull_stock_insti_holders_test',python_callable=pull_stock_insti_holders_test)
    # Load recomenedation & institutional holders
    Load_recommendations_InstiHolders=PythonOperator(task_id='Load_recommendations_institutional_holders',python_callable=load_Recommends_InstiHolders)
    #######################
    ## Linear Regression ##
    Clean_stock_data=PythonOperator(task_id='Clean_stock_data',python_callable=stock_data_cleaning)
    Apply_linear_model=PythonOperator(task_id='Apply_linear_model',python_callable=impl_linear_reg)

yf_data_load >> [Pull_stock_attri, Pull_stock_recommendations, Pull_stock_insti_holders, Clean_stock_data]
Clean_stock_data >> Apply_linear_model
#yf_data_load>> Pull_stock_attri >> [Pull_stock_recommendations, Pull_stock_insti_holders]
Pull_stock_attri>> Load_stock_attri
[Pull_stock_recommendations, Pull_stock_insti_holders]>>Load_recommendations_InstiHolders
#Load_recommendations_InstiHolders>> pull_stock_insti_holders_test
# use with: https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html
