import json
from datetime import timedelta
import pandas as pd
import pymysql
from sqlalchemy import create_engine


def lambda_handler(event, context):
    startdate = event['queryStringParameters']['startdate']
    enddate = event['queryStringParameters']['enddate']
    overview = event['queryStringParameters']['overview']
    add = event['queryStringParameters']['addition']
    comparison = "No Comparison"
    if add == "true":
        comparison = pd.DataFrame.from_dict(json.loads(event['queryStringParameters']['comparelist']))
        print(comparison)

    print(startdate)
    print(enddate)
    db_connection_str = 'mysql+pymysql://admin:SherpaPw123@sherpa-database.cj5ympuvmpqg.us-east-2.rds.amazonaws.com:' \
                        '3306/Sherpa'
    db_connection = create_engine(db_connection_str)
    sqlQuery = 'Select Distinct `Actual Room` from `OR-Data`'
    rooms = pd.read_sql(sqlQuery, con=db_connection_str)
    rooms = rooms[rooms['Actual Room'] != 'RM3']
    FinalOut = pd.DataFrame(
        columns=['Day', 'Service', 'TurnAroundTime', 'AllowableTaT', 'StartTime', 'EndTime', 'BlockUtilization',
                 'NownCount'])
    for index, room in rooms.iterrows():
        curRoom = room['Actual Room']
        sqlQuery = "SELECT `Surgery Date`, `Service`, `Patient In Room Time`, `Patient Out of Room Time`, `Admission Type` " \
                   "from `OR-Data` WHERE `Surgery Date`>= %s and `Surgery Date` <= %s and " \
                   "time(`Scheduled Start Time`) between '08:00:00' and '15:30:00' and ((time(`Patient In Room Time`) < '15:30:00' " \
                   "and time(`Patient Out of Room Time`) > '08:00:00') or `Service` ='NOWN') " \
                   "and dayofweek(`Surgery Date`) in (2,3,4,5,6) and `Actual Room` = %s " \
                   "and !(`Service`!='NOWN' and " \
                   "(`Patient In Room Time`= '00:00:00' or `Patient Out of Room Time` = '00:00:00'))" \
                   "ORDER BY `Service`='NOWN' desc, `Patient In Room Time`"

        information = pd.read_sql(sqlQuery, con=db_connection_str, params=(startdate, enddate, curRoom))
        information.set_index(['Surgery Date'], inplace=True)
        Output = pd.DataFrame(
            columns=['Day', 'Service', 'TurnAroundTime', 'AllowableTaT', 'StartTime', 'EndTime', 'BlockUtilization',
                     'NownCount', 'UnderUtilTime', 'Extra Surgeries'])

        for date, new_df in information.groupby(level=0):
            new_df = new_df.reset_index()
            curTaT, avgTaT, startMin, endMin, BlockUtilization, startPoint = 0, 0, 0, 0, 0, 0
            contElect, contOPTH = False, False
            curService = ""
            splitStart = 0

            while startPoint < len(new_df) and new_df.iloc[startPoint]['Service'] == 'NOWN':
                startPoint += 1

            for index, surgery in new_df.iloc[startPoint:].iterrows():

                inTime = surgery['Patient In Room Time']
                outTime = surgery['Patient Out of Room Time']
                if inTime == new_df.iloc[startPoint]['Patient In Room Time']:

                    startTime = inTime.hour * 60 + inTime.minute
                    BaseStartTime = 480  # 8:00 to minutes from 0:00
                    if startTime < BaseStartTime:
                        startMin = 0
                    else:
                        startMin = startTime - BaseStartTime

                if outTime == new_df.iloc[-1]['Patient Out of Room Time']:

                    endTime = outTime.hour * 60 + outTime.minute
                    BaseEndTime = 930  # 3:30 to minutes from 0:00
                    if endTime > BaseEndTime:
                        endMin = 0
                    else:
                        endMin = BaseEndTime - endTime

                else:
                    new_start = new_df.iloc[index + 1]['Patient In Room Time']
                    tempTaT = int((new_start - outTime).total_seconds() / 60)
                    tempavgTaT = 0
                    if curRoom == 'RM3':
                        tempavgTaT = 10
                    elif surgery['Service'] == 'OPTH':
                        tempavgTaT = 5
                        contOPTH = True
                    else:
                        tempavgTaT = 12

                    if tempTaT > tempavgTaT:
                        curTaT += tempTaT
                    elif tempTaT < 0:
                        timeChange = timedelta(minutes=tempavgTaT)
                        outTime = new_start - timeChange
                        curTaT += tempavgTaT

                    else:
                        timeChange = timedelta(minutes=tempavgTaT - tempTaT)
                        outTime = outTime - timeChange
                        curTaT += tempavgTaT

                    avgTaT += tempavgTaT

                if inTime.hour < 8:
                    inTime = inTime.replace(hour=8, minute=0)
                if outTime.hour > 15 or (outTime.hour == 15 and outTime.minute > 30):
                    outTime = outTime.replace(hour=15, minute=30)

                BlockUtilization += (outTime - inTime).total_seconds() / 60

                if surgery['Admission Type'] == 'ELECTIVE':
                    if curService == "":
                        curService = surgery['Service']
                    else:
                        splitStart = index
                        # upload the old values to the Block Utilization,
                    contElect = True
            if contElect == True:
                UnderUtilTime = 450 - avgTaT - BlockUtilization
                if UnderUtilTime > 450:
                    print(avgTaT)
                    print(BlockUtilization)
                    print(date)
                    print(curTaT)
                if contOPTH == True:
                    minSurgTime = 12
                else:
                    minSurgTime = 30

                extraSurgery = int(UnderUtilTime / minSurgTime)

                outputS = pd.Series(
                    [date, curService, curTaT, avgTaT, startMin, endMin, BlockUtilization, startPoint,
                     UnderUtilTime, extraSurgery], index=Output.columns)
                Output = Output.append(outputS, ignore_index=True)
            # else:
            # print(date)
        # ['TurnAroundTime', 'AllowableTaT', 'StartTime', 'EndTime', 'BlockUtilization', 'NownCount']

        if overview:
            FinalOut = FinalOut.append(Output, ignore_index=True, sort=False)
            # print(ExecOut)
    if overview:
        db_connection.dispose()
        if FinalOut.empty:

            FinalOut = pd.DataFrame(
                columns=['Service', 'Utilized', 'TotalTime', 'Unutilized', ])
            FinalOut = FinalOut.to_dict('records')
            Info = {}
            Info['Service'] = 'All Services'
            Info['Utilize'] = 0
            Info['Unutilized'] = 0
            Info['TotalTime'] = 0
            Info['Neither'] = 100
            responseBody = {
                'SumGraph': [Info],
                'GraphData': FinalOut,

            }
            return {
                'statusCode': 200,
                'headers': {

                    'Access-Control-Allow-Headers': 'Content-Type',

                    'Access-Control-Allow-Origin': '*',

                    'Access-Control-Allow-Methods': 'OPTIONS,GET'

                },
                'body': json.dumps(responseBody)
            }
        else:
            FinalOut['TotalTime'] = 450
            ServOut = FinalOut.groupby('Service').agg(
                {'TurnAroundTime': 'sum', 'AllowableTaT': 'sum', 'StartTime': 'sum', 'EndTime': 'sum',
                 'NownCount': 'sum',
                 'BlockUtilization': 'sum', 'UnderUtilTime': 'sum', 'Extra Surgeries': 'sum', 'TotalTime': 'sum'})

            FullSum = ServOut.sum()
            ServOut['BlockUtilization'] = (
                (((ServOut['BlockUtilization'] + ServOut['AllowableTaT']) / ServOut['TotalTime']) * 100).round(
                    decimals=0)).astype(int)
            FullSum['BlockUtilization'] = (
                (((FullSum['BlockUtilization'] + FullSum['AllowableTaT']) / FullSum['TotalTime']) * 100).round(
                    decimals=0)).astype(int)
            ServOut['TotalTime'] = ((ServOut['TotalTime'] / 60).round(decimals=0)).astype(int)

            # print('Average Utilization: ' + str(round(FullUtilize, 2)))
            # print('Total Time Lost: ' + str(round(FullSum['UnderUtilTime'] / 60, 2)))
            # print('Total Extra Surgeries Possible: ' + str(FullSum['Extra Surgeries']))
            # print('Total Turn Around Time Overage Hours: ' + str(
            #    round((FullSum['TurnAroundTime'] - FullSum['AllowableTaT']) / 60, 2)))
            # print('Total Late Start Hours: ' + str(round(FullSum['StartTime'] / 60, 2)))
            # print('Total Early End Hours: ' + str(round(FullSum['EndTime'] / 60, 2)))
            # print('Total Nown Surgeries: ' + str(FullSum['NownCount']))

            GraphData = ServOut.reset_index()
            GraphData = GraphData.filter(['Service', 'BlockUtilization', 'TotalTime'], axis=1)
            GraphData['BlockUtilization'] = (GraphData['BlockUtilization'].round(decimals=0)).astype(int)
            GraphData['Unutilized'] = 100 - GraphData['BlockUtilization']
            info = {}
            info['Service'] = 'All Services'
            info['Utilize'] = FullSum['BlockUtilization']
            info['Unutilized'] = 100 - FullSum['BlockUtilization']
            info['TotalTime'] = str(round(GraphData['TotalTime'].sum(), 0))
            info['Neither'] = 0
            RespAvg = [info]
            GraphData['TotalTime'] = GraphData['TotalTime'].astype(str)
            GraphData = GraphData.rename(columns={'BlockUtilization': 'Utilized'})
            GraphData = GraphData.to_dict('records')


            responseBody = {
                'SumGraph': RespAvg,
                'GraphData': GraphData,
                'Utilize': round(FullSum['BlockUtilization'], 0),
                'OverageTaT': round((FullSum['TurnAroundTime'] - FullSum['AllowableTaT']) / 60, 0),
                'FCSDelay': round(FullSum['StartTime'] / 60, 0),
                'EarlyEnd': round(FullSum['EndTime'] / 60, 0),
                'Unutilized': round(FullSum['UnderUtilTime'] / 60, 0),
                'UnutilizedDollars': round(FullSum['UnderUtilTime']*15, 0)
            }
            print(type(RespAvg))
            print(type(GraphData))
            print(type(responseBody))
            print(responseBody)
            print(FullSum['AllowableTaT'])
            return {
                'statusCode': 200,
                'headers': {

                    'Access-Control-Allow-Headers': 'Content-Type',

                    'Access-Control-Allow-Origin': '*',

                    'Access-Control-Allow-Methods': 'OPTIONS,GET'

                },
                'body': json.dumps(responseBody)

            }
    db_connection.dispose()
    return {
        'statusCode': 200,
        'body': json.dumps('Overview not selected')
    }
