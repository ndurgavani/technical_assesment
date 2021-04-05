2. Python and SQL/HQL combination

Business Objective: Data transformation as per the data modelling shared by BI Team.

Technical Objective: 
Data is processed using Python and HQL/SQL.
Data is stored as Internal table and External table.

Pseudo Code: Pylint compliance check is done before deploying the code into Production Environment
try and Exception blocks are added in the original code

'''INFO
LINT compliance : **/10
RUNS @******.
'''
#Third party imports
import subprocess

#Declaring the variables
SCHEMA = 'schema_name' 
SQL_FILE = 'file_name.hql'
INI_CMD = "beeline -u jdbc:hiveconnection{0} -n username -w \
       password -f EMR_code_path/{1}".\
       format(SCHEMA, SQL_FILE)

#---------------INVOCATION OF WH_PROD_INITIAL_RUN.SQL------------------##Addded to capture logs
try:
    STATUS, OUTPUT = subprocess.getstatusoutput(INI_CMD)
    if STATUS != 0:
        logging.info('Refresh Failed: Start of Error Message')
        logging.exception(str(OUTPUT))
        logging.info('Refresh Failed: End of Error Message')
    else:
        logging.info('Tables Refresh started')
        logging.exception(str(OUTPUT))
        logging.info('Tables Refresh Completed')
except Exception as e:
    logging.info('Refresh Failed: Start of Exception Message')
    logging.exception(str(e))
    logging.info('Refresh Failed: End of Exception Message')

#file_name.hql
--table_name: External table
drop table if exists table_name;
create external table table_name
(
new_column1 STRING,
new_column2 STRING,
new_column3 STRING,
new_column4 STRING,
new_column5 STRING,
new_column6 STRING
)
ROW FORMAT 
DELIMITED FIELDS TERMINATED BY '\t'
NULL DEFINED AS ''
stored as textfile
location 's3://bucket_name/file_name/';
INSERT OVERWRITE TABLE novo_io_external_data.table_name
SELECT 
case when t1.column_11 is null AND substr(t1.column_13, 1, 2)='AB' then 'COMPLETE' 
when t1.column_11 is null AND substr(t1.column_13, 1, 2)='CD' then 'PENDING' 
else t1.column_11 end as new_column,
concat_ws('#','I',t1.column_1,t1.column_1,t1.column_2,t1.column_3)  as new_column1,
"abc" as new_column2,
t1.column_4 as new_column3,
upper(trim(split(split(t1.column_5,'/')[7],'_')[1]))  as new_column4,
concat_ws('#',t2.column_2,'test') as new_column5,
replace(replace(ltrim(replace(replace(replace(t1.column_10,'-',0),'R',0),'0',' ')),' ',0),'13B','0') as new_column6,
FROM 
novo_io_external_data.table_1 t1
LEFT OUTER JOIN novo_io_external_data.table_2 t2 on (upper(trim(split(split(t1.column_5,'/')[7],'_')[1]))=ds.column_4) and (ds.column_3="test_data");	
--table_name1: Internal table
drop table if exists novo_io_eu_oce_export.table_name1;
create table if not exists novo_io_eu_oce_export.table_name1
as
SELECT distinct
co.new_column1 as date_id,
co.Id as Coaching_ID,
co.new_column5 as new_column5, 
exp_col as new_column10,
co.new_column4 as new_column4,
co.new_column7 as new_column7
FROM (
select a.Id,
from_unixtime(unix_timestamp(regexp_replace(a.column1, 'T',' ')), 'ddMMyyyy') as new_column1,
split(substr(a.column2, 2, length(column2) - 2), '\073') as exploded_column,
c.column5 as new_column5,
b.column7 as new_column4,
b.column4 as new_column7
from novo_io_eu_oce_export.table1 a join novo_io_eu_oce_export.table2 b
on(a.id=b.column3)
join novo_io_eu_oce_export.table3 c on (c.column1=a.OCE__User__c)) CO LATERAL VIEW 
explode(exploded_column) adTable AS exp_col
where new_column5<>exp_col;
