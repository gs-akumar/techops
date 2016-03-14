import psycopg2
import pprint
import csv
import time
import pymongo
import json
from pymongo import MongoClient
import boto
import sys
import os
from boto.s3.key import Key
from redshiftconf import *



def putfile(trgfilename):
    print "%s - S3 bucket connecting .... "%(BUCKETNAME)
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket(BUCKETNAME)
    key = trgfilename
    fn = LOCAL_PATH + '/' + trgfilename
    k = Key(bucket)
    k.key = os.path.join(TENANTLOGDIR,key)
    print k.key
    k.set_contents_from_filename(fn)
    k.make_public()
    print 'file uploaded to  S3 bucket %s\n'%(trgfilename)

def delfile(trgfilename):
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket(BUCKETNAME)
    key = trgfilename
    bucket.delete_key(key)
    print 'file deleted from S3 bucket %s\n'%(trgfilename)

def generatetlist(username, password, servername, port, dbname, tusername, tpassword, tservername, tport, tdbname,filename):
    connstr = 'mongodb://' + username + ':' + password + '@' + servername + ':' + port + '/' + dbname
    tconnstr = 'mongodb://' + tusername + ':' + tpassword + '@' + tservername + ':' + tport + '/' + tdbname
    sqlfile = open(filename, 'w')
    tconnection = MongoClient(tconnstr)
    connection = MongoClient(connstr)
    tconndbi = tconnection[tdbname]
    conndbi = connection[dbname]
    db = conndbi['collectionmaster']
    tdb = tconndbi['tenantmaster']
    print 'Process started at ' + time.strftime('%Y-%m-%d %X')
    results = db.find({'deleted': False,
     'CollectionDetails.dataStoreType': 'REDSHIFT',
     'CollectionDetails.assetType': 'ANALYTICS'}, {'TenantId': 1,
     'CollectionDetails.dbCollectionName': 1}).sort('TenantId', pymongo.ASCENDING)
    tid = ''
    ct=1
    for record in results:
        for (k, v,) in record.items():
            if k == 'TenantId':
                tid = '"' + v + '"'
                tresults = tdb.find({'TenantId': v,
                 'deleted': False}, {'TenantName': 1})
                for rec in tresults:
                    for (n, m,) in rec.items():
                        #print "%s\t%s"%(n,m)
                        if n == 'TenantName':
                            tid = tid + ',"' + m + '"'
                            print "table number :: %s" % ct
                            ct = ct + 1
            if k == 'CollectionDetails':
                for (j, l,) in v.items():
                    cstr = l
        sqlfile.write(tid + ',"' + cstr + '"\n')
    sqlfile.close()
    connection.close()
    tconnection.close()


def rsexecute(hostname, portno, dbname, username, pswd, sqlquery, repflag, outfile):
    conn_string = "host='" + hostname + "' port=" + portno + " dbname='" + dbname + "' user='" + username + "' password='" + pswd + "'"
    print 'Connecting to database .................\n%s/%s\n' % (hostname, dbname)
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    print 'Database Server Connected!\n %s \n' % sqlquery
    print 'SQL Execution started at ' + time.strftime('%Y-%m-%d %X')
    cursor.execute(sqlquery)
    conn.commit()
    if repflag == 1:
        records = cursor.fetchall()
        pprint.pprint(records)
    if outfile != '':
        records = cursor.fetchall()
        result = open(outfile, 'wb')
        writer = csv.writer(result, dialect='excel')
        writer.writerows(records)
        result.close
    print 'query execution Completed at ' + time.strftime('%Y-%m-%d %X')


