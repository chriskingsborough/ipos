#!/usr/bin/env python
import json
from datetime import datetime
import pymysql
import logging
import traceback
from os import environ

host=environ.get('HOST')
port=environ.get('PORT')
dbuser=environ.get('USER')
password=environ.get('PASSWORD')
database=environ.get('DATABASE')

query="""
    select
        symbol,
        company,
        lead_managers,
        shares_millions,
        price_low,
        price_high,
        expected_to_trade
    from upcoming_ipos
    where expected_to_trade >= cast(NOW() as date)
    order by expected_to_trade;
"""

logger=logging.getLogger()
logger.setLevel(logging.INFO)

def make_connection():
    return pymysql.connect(host=host, user=dbuser, passwd=password,
        port=int(port), db=database, autocommit=True)

def log_err(errmsg):
    logger.error(errmsg)
    return {"body": errmsg , "headers": {}, "statusCode": 400,
        "isBase64Encoded":"false"}

logger.info("Cold start complete.") 

def handler(event,context):

    try:
        cnx = make_connection()
        cursor=cnx.cursor(pymysql.cursors.DictCursor)
        
        try:
            cursor.execute(query)
        except:
            return log_err ("ERROR: Cannot execute cursor.\n{}".format(
                traceback.format_exc()) )

        try:
            results_list=[]
            for result in cursor:
                result['expected_to_trade'] = datetime.strftime(
                    result['expected_to_trade'], 
                    '%Y-%m-%d'    
                )
                results_list.append(result)
            print(results_list)
            cursor.close()

        except:
            return log_err ("ERROR: Cannot retrieve query data.\n{}".format(
                traceback.format_exc()))


        return {"body": json.dumps(results_list), "headers": {}, "statusCode": 200,
        "isBase64Encoded":"false"}

    
    except:
        return log_err("ERROR: Cannot connect to database from handler.\n{}".format(
            traceback.format_exc()))


    finally:
        try:
            cnx.close()
        except: 
            pass 

if __name__== "__main__":
    handler(None,None)