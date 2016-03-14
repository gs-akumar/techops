#!/usr/bin/python
#######################################
#############################################
# Created By : Anmol Kumar
# Objective : Redshift and S3 handlers
# call usage : python s3putfile.py <bucketname> <filetoput> 
#################################################################
####################################################################
import sys, os, datetime
from redshiftconf import *
from techopsmonitor import * 

######################### Execution Steps at  main Program ############

#                                         1. extract tenant details from mongoDB, generate a csv file
#                                         2. put csv file to S3 bucket
#                                         3. truncate table tenant details
#                                         4. Load csv file to tenant details at reporting cluster
#                                         5.extract 1 day incremental data from redshift servers at STG table
#                                         (a) RS3
#                                         (b) RS4
#                                         (c) RS5
#                                         (d) RS6
#                                         6. extract incremental data to csv from STG table
#                                         7. put csv files to S3 bucket
#                                         8. Load csv files to STG table at reporting cluster with redshift server ID (i.e. rs3, rs4 etc.)
#                                         9. lookup for tenant details for relevant sql queries, populate tenant Id and tenant Name
#                                         10. load to final base fact table (base_fact_querylogs) at reporting cluster
#                                         11. Derive top slow running queries for each tenant, cluster level, date wise
#                                         12. no of query growth on each cluster
#                                         13. table size with sorting details ... vacuum automate

#################### Update Tenant Incremental Data ##############################################
dvalue = str(datetime.date.today())
print " redshift monitoring is starting for process-date %s\n"%(dvalue)
tenantfilename='tenant_table_details-' + dvalue  + '.csv'
tenantstgtable='rsm_stg_tenant_detail'
tenanttable='rsm_tenant_detail'

# 1. extract tenant details from mongoDB, generate a csv file
for x in range(1, 5):
    USERNAME = eval('MUSERNAME' + str(x))
    PASSWORD = eval('MPASSWORD' + str(x))
    SERVERNAME = eval('MSERVERNAME' + str(x)) 
    PORT = eval('MPORT' + str(x)) 
    DBNAME = eval('MDBNAME' + str(x))
    FILENAME='mongo' + str(x) + '_' + tenantfilename 
    print "Connecting .....\n ***********\t***********\t%s\t%s\t%s\t" % (SERVERNAME, PORT , DBNAME)
    generatetlist(USERNAME,PASSWORD,SERVERNAME,PORT,DBNAME,MUSERNAME1,MPASSWORD1,MSERVERNAME1,MPORT1,MDBNAME1,FILENAME)
# 2. put csv file to S3 bucket
    putfile(FILENAME)
# 3. truncate stage table tenant details
    sqlquerytext='truncate table ' +  tenantstgtable + ';'
    rsexecute(TECHHOST,TECHPORT,TECHDB,TECHUSER,TECHPASSWORD,sqlquerytext,0,"")
# 4. Load csv file to tenant details at reporting cluster
    sqlquerytxt="copy " + tenantstgtable + "(tenantid,tenantname,tablename) from 's3://" + BUCKETNAME  + "/" + TENANTLOGDIR  + "/" + FILENAME  + "' credentials 'aws_access_key_id=" + AWS_ACCESS_KEY_ID  + ";aws_secret_access_key=" + AWS_SECRET_ACCESS_KEY + "' delimiter ',' REMOVEQUOTES ;"
    rsexecute(TECHHOST,TECHPORT,TECHDB,TECHUSER,TECHPASSWORD,sqlquerytxt,0,"")
# 5. (ii) insert all new records to tenant details
    sqlquerytxt="insert into " + tenanttable  + " select * from " + tenantstgtable  + " where (tenantid,tablename) not in (select tenantid,tablename from " + tenanttable + ");"
    rsexecute(TECHHOST,TECHPORT,TECHDB,TECHUSER,TECHPASSWORD,sqlquerytxt,0,"")


#################### Extract daily incremental data from STL tables from each redshift server ##########################

# 5. Incremental data load :  1 day data from query logs from stl tables

for y in range(1, 8):
#for y in 1,3,4,5,6:
    cname='gs-prod-rs' + str(y)
    filename='stllogsincr' + dvalue  + '.csv'
    qstgtable='rsm_stg_querylogdetail'
    qtable='rsmquerylogdetail_b90b9bc677ec45a091e4047f9c686eee'
    rawspath='s3://gs-techops-prod/redshift-logs/querylogs/' + cname 
    rfilepath=rawspath + '/' + cname + '-' + filename
    rzipfilepath=rfilepath + '000.gz'
    
    RSHOST = eval('RSHOST' + str(y))
    RSPORT = eval('RSPORT' + str(y))
    RSDB = eval('RSDB' + str(y))
    RSUSER = eval('RSUSER' + str(y))
    RSPASSWORD = eval('RSPASSWORD' + str(y))

#    rsexecute(RSHOST,RSPORT,RSDB,RSUSER,RSPASSWORD,'DROP TABLE rsm_stg_querylogdetail',0,"")
### Create table if not exists for new listed servers
    SQLQUERYDDL="create table IF NOT EXISTS " + qstgtable + " (query_id integer, pid            integer , querytxt       character varying(4000)  ,querytype      character varying(20)    , label          character varying(30)    , tenantid       character varying(100)   , tenantname     character varying(250)   , executiondate  date                     , queuetime      bigint                   , exectime       bigint                   , createddate     date default current_date , createdby      character varying(50)    default user);"
    rsexecute(RSHOST,RSPORT,RSDB,RSUSER,RSPASSWORD,SQLQUERYDDL,0,"")

####### truncate temp table
    SQLQUERY="truncate table " + qstgtable + ";"
    rsexecute(RSHOST,RSPORT,RSDB,RSUSER,RSPASSWORD,SQLQUERY,0,"")
# change to current_date -1 filter
    SQLQUERY="insert into " + qstgtable  + " select s.query as query_id,s.pid,trim(s.querytxt),case when querytxt iLike '%select%' then 'SELECT' when querytxt iLike '%update%' then 'UPDATE' when querytxt iLike '%delete%' then 'DELETE' when querytxt iLike '%insert%' then 'INSERT' END  as querytype,s.label,-1 as tenantId,'demo' as tenantName,starttime as executionDate,t.total_queue_time as queueTime,total_exec_time as execTime,current_date  as createdDate,user as createdBy  from stl_query as s inner join  stl_wlm_query as t on s.query=t.query;"
          ## where cast(s.starttime as date) <= current_date - 1;"
    rsexecute(RSHOST,RSPORT,RSDB,RSUSER,RSPASSWORD,SQLQUERY,0,"")
### Unload incremental data to s3 bucket
    RSSQLQUERY="unload('select query_id,pid,querytxt,querytype,label,tenantid,tenantname,executiondate,queuetime,exectime,''" + cname +  "'' as servername,createddate,createdby from " + qstgtable  + ";') to '" + rfilepath  + "' credentials 'aws_access_key_id=" + AWS_ACCESS_KEY_ID  + ";aws_secret_access_key=" + AWS_SECRET_ACCESS_KEY + "' gzip delimiter as '\t'  null as '' parallel off ALLOWOVERWRITE;"
    rsexecute(RSHOST,RSPORT,RSDB,RSUSER,RSPASSWORD,RSSQLQUERY,0,"")
### Truncate stage table in Target Redshift
    RSSQLQUERY="truncate table " + qstgtable  + ";"
    rsexecute(TECHHOST,TECHPORT,TECHDB,TECHUSER,TECHPASSWORD,RSSQLQUERY,0,"")
### load incremental data from s3 to redshift (target) stage table
    RSSQLQUERY="copy " + qstgtable + " from '" + rzipfilepath  + "' credentials 'aws_access_key_id=" + AWS_ACCESS_KEY_ID  + ";aws_secret_access_key=" + AWS_SECRET_ACCESS_KEY + "' gzip delimiter '\t' REMOVEQUOTES ;"
    rsexecute(TECHHOST,TECHPORT,TECHDB,TECHUSER,TECHPASSWORD,RSSQLQUERY,0,"")
#### Load new data from stage to final fact log table #########
    RSSQLQUERY="insert into " + qtable  + " select * from " + qstgtable  + " where (query_id,servername,executiondate) not in (select gsd36640,gsd7386,gsd49773 from " + qtable + ");"
    rsexecute(TECHHOST,TECHPORT,TECHDB,TECHUSER,TECHPASSWORD,RSSQLQUERY,0,"")



################## End Of Process ####################################
print "Redshift log reporting process completed for " + dvalue
#####################################################################
