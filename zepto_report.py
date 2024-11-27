import redshift_connector
from datetime import datetime, date,timedelta
import time
import pandas as pd
from urllib.parse import unquote, quote
import numpy as np
import os
import boto3
from sqlalchemy import create_engine
from SecretManager import *
from test_ezpto import iam_role
host = str(df_redshift[2])
port = int(df_redshift[3])
user = str(df_redshift[0])
password= str(df_redshift[1])

to_date=datetime.now().date()
date_2=to_date-timedelta(days=2) #install
date_3=to_date-timedelta(days=3)  #event
print(to_date)
conn = redshift_connector.connect(
        host=host,
        database='mfilterit_db',
        port=port,
        user=user,
        password=password
    )


cursor = conn.cursor()
df_event=pd.read_sql(f"select installed_app,event_publisher_name,event_sub_publisher_name,app_version,event_date,click_time,inserted_date,install_publisher_name,install_sub_publisher_name,install_fraud_sub_category,event_fraud_sub_category,adid,device_id,campaign_id,event_type,event_datetime,transaction_id,click_id,conversion_date,site_event_name,conversion_time,install_country,country,order_no,user_id,city,af_revenue,partner_campaign_name,reserved_4,contributing_publisher from mfilterit_app_event_schema.efa_report where inserted_date ='"+str(date_3)+"' and installed_app in ('com.zeptoconsumerapp','com.zeptoconsumerapp.ios') and event_publisher_name not in (select distinct publisher_name from mfilterit_app_install_schema.whitelisted_pubs where installed_app='com.zeptoconsumerapp' and inserted_date = '"+str(date_3)+"' );",con=conn)
print("installed_App",df_event['inserted_date'])

df_install=pd.read_sql(f"select publisher_id,publisher_name,sub_publisher_name,adid,inserted_date,installed_app,vendor_id,device_id,fraud_sub_category,campaign_id,transaction_id,user_id,device_carrier,conversion_make,conversion_model,click_id,click_time,conversion_time,app_version,conversion_ip,conversion_type,session_referrer,os_type,country,os_version,region,city,partner_campaign_name,contributing_publisher from mfilterit_app_install_schema.mf_report where inserted_date ='"+str(date_2)+"' and installed_app in ('com.zeptoconsumerapp','com.zeptoconsumerapp.ios') and publisher_name not in (select distinct publisher_name from mfilterit_app_install_schema.whitelisted_pubs where installed_app='com.zeptoconsumerapp' and inserted_date='"+str(date_2)+"' );",con=conn)
print("event",df_install['inserted_date'])

class parquet_file():
    def creation(df,file_name,data_type,date):
        print(">>>>>>>>>>>>>>>>",len(df))
        df_install_chunk=np.array_split(df,1)
        len_1=int(len(df)/1000000)+1
 #       print(len_1)
        a=0
        b=1000000
        j=0
        for i in range(len_1):
            file_name=str(file_name)+"_"+str(j)
            df.iloc[a:b].to_parquet("/s3storage_mfappdata/internal_uses/zepto/"+file_name+".parquet",index=False)
            os.chmod("/s3storage_mfappdata/internal_uses/zepto/"+file_name+".parquet",0o777)
            time.sleep(300)
            a+=1000000
            b+=1000000
            j+=1
            kaye_path=str(data_type)+"/date="+str(date)+"/"+str(file_name)+".parquet"
            print(kaye_path)
            try:
                with open("/s3storage_mfappdata/internal_uses/zepto/"+file_name+".parquet",'rb') as data:
                    iam_role(data,"prod-zepto-mfilterit",kaye_path)
            except:
                from alert_mail import send_alert
                subject='Status of zepto parquet file'
                emails=['sonu.rathore@mfilterit.com','sunil.kashyap@mfilterit.com','labhit.agarwal@mfilterit.com','ojasvi.rana@mfilterit.com','durgesh@mfilterit.com']
                body_containt='Please check zepto parquet files are not loading on client bucket. file_name:- '+str(file_name)
                send_alert(subject,emails,body_containt)

install_file_name='zepto_install_'+str(date_2)
event_file_name='zepto_event_'+str(date_3)
date_install=date_2.strftime("%Y%m%d")
date_event=date_3.strftime("%Y%m%d")
#print(date_install)
#print(date_event)
parquet_file.creation(df_install,install_file_name,"Install",date_install)
parquet_file.creation(df_event,event_file_name,"Event",date_event)
#path=''
#with open('zepto_install_2024-01-130.parquet', 'rb') as data:
    #op = s3.upload_fileobj(data, "prod-zepto-mfilterit", "test.txt")
#    iam_role(data,"prod-zepto-mfilterit","Install/date=20240113/zepto_install_2024-01-130.parquet")
#path=""
#s3_client=boto3.client('s3','AKIAZGM55GEVUIGB6XQQ','2U0sWu6HMZ8FAaMylGxqKg9gUkRWwYyw3qfSFV2w')
#s3_client.upload_file("/s3_storage/sonu_new/"+file_name+".parquet",'prod-zepto-mfilterit',file_name+".parquet")
#iam_role(df_install,install_file_name,"Install",date_install)
