1. Python code

Business Objective: Data ingested by the data provider in S3 should be processed.
If there are any missing products and address_code in Datalake, email should be triggered to respective stakeholders.
Once, the data is shared by stakeholders, the data should be automatically ingested in s3 and the data should be reprocessed.

Technical Objective: 
AWS access keys are shared with data providers to ingest the data in S3.
Data is processed once the file is placed by the data provider in s3 using AWS lambda service.
Automated the process of data ingestion sent via mail to datalake. 
If there is any missing information in the data provided, the email will be automatically triggered to the user.

Pseudo Code: Pylint compliance check is done before deploying the code into Production Environment.
try and Exception blocks are added in the original code.

'''INFO
Pylint compliance: **/10
RUNS @******.
'''

#Third party imports
import boto3
import pandas as pd
import csv
import numpy as np
import io
from datetime import datetime
from dateutil.relativedelta import relativedelta
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

#Variable declarations
s3 = boto3.client('s3')
s3resource = boto3.resource('s3')
csv_buffer = io.StringIO()

srcBucket = 'Bucket_name'
rawDatapath = 's3_file_path'
filename = 'ABC_data.xlsx'
COUNTRY_CD = filename[0:2]
filelookuppath = 'S3_folder_path/LOOKUP_DATA.csv'
Brlookuppath = 'S3_folder_path/Address_code'+COUNTRY_CD+'.csv'
Prdlookuppath = 'S3_folder_path/Product_name'+COUNTRY_CD+'.csv'

prd_cols = ['CODE', 'STATUS', 'COUNTRY']
Br_cols = ['CODE', 'FEED_NAME']

destBucket = 'Bucket_name'
destPath = 's3_file_path'

def validation(x, prdlist, brlist):
	msg = ''
	if pd.isna(x['PROD']):
		msg = msg+'PROD is null;'
	else:
		if x['PROD'] not in prdlist:
			msg = msg+x['PROD']+' is not available in Product table;'
	if pd.isna(x['COUNTRY_NAME']):
		msg = msg+'COUNTRY_NAME is null;'
	if pd.isna(x['PROVIDER_NAME']):
		msg = msg+'PROVIDER_NAME is null;'
	if pd.isna(x['DATE']):
		msg = msg+'DATE is null;'
	if pd.isna(x['POSTAL_CODE']):
		if pd.isna(x['ACC_ID']):
			msg = msg+'POSTAL_CODE and ACC_ID is null;'
	else:
		if x['POSTAL_CODE'] not in brlist:
			msg = msg+x['POSTAL_CODE']+' is not available in Brick table;'
	if msg == '':
		return np.nan
	else:
		return msg

#reading the lookup data from datalake
lookup_obj = s3.get_object(Bucket=srcBucket, Key=filelookuppath)
lookup_string = lookup_obj['Body'].read()
lookup_df = pd.read_csv(io.BytesIO(lookup_string), quotechar = '"', sep=',', error_bad_lines=False, dtype=str).loc[lambda df: df['FILE_NAME'].isin([filename]), :]

d = lookup_df.set_index('DEFAULT_COLUMN')['DEFAULT_VALUE'].to_dict()

brick_obj= s3.get_object(Bucket=srcBucket, Key=Brlookuppath)
brick_string = brick_obj['Body'].read()
brick_df = pd.read_csv(io.BytesIO(brick_string), quotechar='"', sep=',', error_bad_lines=False, dtype=str, usecols=Br_cols).loc[lambda df: df['FEED_NAME'].isin([d['DATA_SOURCE']]), :]

brk_list = list(brick_df['CODE'].drop_duplicates())

prd_obj = s3.get_object(Bucket=srcBucket, Key=Prdlookuppath)
prd_string = prd_obj['Body'].read()
prd_df = pd.read_csv(io.BytesIO(prd_string), quotechar='"', sep=',', error_bad_lines=False, dtype=str, usecols=prd_cols).loc[lambda df: df['COUNTRY'].isin([d['COUNTRY_NAME']]), :]

prd_list = list(prd_df['CODE'].drop_duplicates())

#Reading csv content sent by the data provider and processing the data as per stakeholder requirement
sales_obj = s3resource.Object(bucket_name=srcBucket, key=rawDatapath+filename)
sales_string = io.BytesIO(sales_obj.get()["Body"].read())
sales_df = pd.read_excel(sales_string, encoding='utf-8',sheet_name=2)
sales_df.columns = ['POSTAL_CODE', 'PROD', 'MONTH_ID', 'UNIT', 'VAL']
sales_df['MONTH_ID'] = sales_df['MONTH_ID'].str.replace('OKT', 'OCT').str.replace('MAI', 'MAY').str.replace('MRZ', 'MAR').str.replace('DEZ', 'DEC')
sales_df['MONTH_ID'] = sales_df.apply(lambda x: datetime.strptime(x.MONTH_ID.capitalize(), "%b %Y").strftime("%Y%m"), axis=1)

Maxmonth = max(sales_df['MONTH_ID'])
Maxdate = datetime.strptime(Maxmonth, '%Y%m')
Minmonth = datetime.strftime(Maxdate-relativedelta(years=2), '%Y%m')

#recent 2 years data is retained
sales_df = sales_df[(sales_df['MONTH_ID']>Minmonth) & (sales_df['MONTH_ID']<=Maxmonth)]

sales_df['UNIT'].fillna(0, inplace=True)
sales_df['VAL'].fillna(0, inplace=True)

sales_df['UNIT']=sales_df.UNIT.astype('float64')
sales_df['VAL']=sales_df.VAL.astype('float64')

sales_final = sales_df.groupby(['POSTAL_CODE','PROD','MONTH_ID'], as_index=False).agg({"VAL":sum,"UNIT":sum})

sales_final['POSTAL_CODE'] = sales_final['POSTAL_CODE'].str.split(' ').str[0]
sales_final ['ORIGIN'] = ''
sales_final['COUNTRY_NAME'] = d['COUNTRY_NAME']
sales_final['DATE'] = sales_final['MONTH_ID']+'01'
sales_final['COUNTRY_CD'] = d['COUNTRY_CD']
sales_final['PROVIDER_NAME'] = d['PROVIDER_NAME']
sales_final['DATA_SOURCE'] = d['DATA_SOURCE']
sales_final['CURCY'] = d['CURCY']

Final_df = sales_final[['COUNTRY_NAME','COUNTRY_CD','PROVIDER_NAME','DATA_SOURCE','ORIGIN','POSTAL_CODE','PROD','CURCY','DATE','MONTH_ID','VAL','UNIT']]

Final_df['VAL_RESULT'] = Final_df.apply(lambda x: validation(x, prd_list, brk_list), axis=1)

Error_df = Final_df[~Final_df['VAL_RESULT'].isna()].reset_index(inplace = False)
Error_df = Error_df[['VAL_RESULT']].drop_duplicates()

#Email Trigger or data ingestion process
if Error_df.empty:
    print("No error")
    Final_df = Final_df.drop(['VAL_RESULT'], axis=1)
    DestFile = 'XYZ_'+COUNTRY_CD+'_'+d['DATA_SOURCE']+'_SALESDATA_'+datetime.strftime(datetime.now(),'%Y%m%d')+'.txt'
    Final_df.to_csv(csv_buffer, sep=',', quotechar = '"', quoting=csv.QUOTE_ALL, index = False, encoding = 'utf-8')
    s3resource.Object(destBucket, destPath+str(DestFile)).put(Body=csv_buffer.getvalue())
else:
    ErrorFile = 'XYZ_'+d['DATA_SOURCE']+'_VALIDATION_ERROR_'+datetime.strftime(datetime.now(),'%Y%m%d')+'.csv'
	Error_df.to_csv(csv_buffer, sep=',', quotechar = '"', quoting=csv.QUOTE_ALL, index = False, encoding = 'utf-8')
    s3resource.Object(destBucket, destPath+str('error_file_path')).put(Body=csv_buffer.getvalue())
    to_emails = ['mail_id']
    cc_emails = ['mail_id']
    ses = boto3.client('ses',region_name='us-east-1')
    msg = MIMEMultipart()
    msg['Subject'] = 'ABC '+COUNTRY_CD+' DATALOAD VALIDATION ERROR-'+Maxmonth
    msg['From'] = "mail_id"
    msg['To'] = to_emails[0]
    msg['Cc'] = ','.join(cc_emails)
    # what a recipient sees if they don't use an email reader
    msg.preamble = 'Multipart message.\n'
    # the message body
    part = MIMEText('Hi Team,\n\nPlease find attached the validation errors for '+d['NAME']+'. Kindly check and share the information to be loaded into Datalake.\n\nBest Regards,\nTeam xyz')
    msg.attach(part)
    # the attachment
    part = MIMEApplication(open('mail_id'+ErrorFile, 'rb').read())
    part.add_header('Content-Disposition', 'attachment', filename=ErrorFile)
    msg.attach(part)
    result = ses.send_raw_email(Source=msg['From'],Destinations=to_emails,RawMessage={'Data': msg.as_string()})