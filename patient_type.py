import json
from datetime import timedelta
import pandas as pd
import pymysql
from sqlalchemy import create_engine


def lambda_handler(event, context):
    db_connection_str = 'mysql+pymysql://admin:SherpaPw123@sherpa-database.cj5ympuvmpqg.us-east-2.rds.amazonaws.com:' \
                        '3306/Sherpa'
    db_connection = create_engine(db_connection_str)
    startDate = event['queryStringParameters']['startdate']
    endDate = event['queryStringParameters']['enddate']
    sqlQuery = 'call get_patient_type(\'{}\', \'{}\')'.format(startDate, endDate)
    Surgeries = pd.read_sql(sqlQuery, con=db_connection_str)
    Surgeries['Inpatient_Percent'] = (Surgeries['Inpatient_Percent'].round(decimals=0)).astype(int)
    Surgeries['Outpatient_Percent'] = (Surgeries['Outpatient_Percent'].round(decimals=0)).astype(int)
    Surgeries['Emerg_Percent'] = (Surgeries['Emerg_Percent'].round(decimals=0)).astype(int)
    Surgeries = Surgeries.to_dict('records')
    return {
        'statusCode': 200,
        'headers': {

            'Access-Control-Allow-Headers': 'Content-Type',

            'Access-Control-Allow-Origin': '*',

            'Access-Control-Allow-Methods': 'OPTIONS,GET'

        },
        'body': json.dumps(Surgeries)

    }
