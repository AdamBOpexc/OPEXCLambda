# This is a sample Python script.
import calendar
import datetime
from datetime import timedelta

import pandas as pd
import pymysql
from sqlalchemy import create_engine


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

def utilization(startdate, enddate, executive, service):
    db_connection_str = 'mysql+pymysql://admin:SherpaPw123@sherpa-database.cj5ympuvmpqg.us-east-2.rds.amazonaws.com:' \
                        '3306/Sherpa'
    print('start')
    print(startdate)
    print('end')
    print(enddate)
    db_connection = create_engine(db_connection_str)
    sqlQuery = 'Select Distinct `Actual Room` from `OR-Data`'
    rooms = pd.read_sql(sqlQuery, con=db_connection_str)
    rooms = rooms[rooms['Actual Room'] != 'RM4']
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

                BlockUtilization += int((outTime - inTime).total_seconds() / 60)

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

                BlockUtilization = (BlockUtilization+avgTaT) / 450 * 100
                outputS = pd.Series([date, curService, curTaT, avgTaT, startMin, endMin, BlockUtilization, startPoint,
                                     UnderUtilTime, extraSurgery], index=Output.columns)
                Output = Output.append(outputS, ignore_index=True)
            else:
                print(date)
        print('-------------------------------------------------------------------------------------------------------')
        print('Information for: ' + curRoom + '\n')
        print('Information Broken Up By Day')
        print('Day:\t\tService\t\tMinutes Over Allowable TAT:\tLate Start Minutes:'
              '\tEarly End Minutes:\tNOWN Count:\tBlock Utilization:\tUnder Util Time:\tExtra Surgeries Possible')
        # ['TurnAroundTime', 'AllowableTaT', 'StartTime', 'EndTime', 'BlockUtilization', 'NownCount']

        for index, row in Output.iterrows():
            curTurnAround = row['TurnAroundTime'] - row['AllowableTaT']
            print('{:10s}\t\t{:10s}\t\t{:3.0f}\t\t\t\t{:3.0f}\t\t\t{:3.0f}\t\t{:1.0f}\t\t{:5.2f}'
                  '\t\t\t{:3.0f}\t\t\t{:2.0f}'.format(
                str(row['Day'].date()), row['Service'], curTurnAround, row['StartTime'], row['EndTime'],
                row['NownCount'], row['BlockUtilization'], row['UnderUtilTime'], row['Extra Surgeries']))
        print('\n\nInformation Over Full Period: ' + str(startdate.date()) + ' to ' + str(enddate.date()))
        FullUtilize = Output['BlockUtilization'].median()
        FullSum = Output.sum()
        print('Average Utilization: ' + str(round(FullUtilize, 2)))
        print('Total Time Lost: ' + str(round(FullSum['UnderUtilTime'] / 60, 2)))
        print('Total Extra Surgeries Possible: ' + str(FullSum['Extra Surgeries']))
        print('Total Turn Around Time Overage Hours: ' + str(
            round((FullSum['TurnAroundTime'] - FullSum['AllowableTaT']) / 60, 2)))
        print('Total Late Start Hours: ' + str(round(FullSum['StartTime'] / 60, 2)))
        print('Total Early End Hours: ' + str(round(FullSum['EndTime'] / 60, 2)))
        print('Total Nown Surgeries: ' + str(FullSum['NownCount']))

        if executive or service:
            FinalOut = FinalOut.append(Output, ignore_index=True, sort=False)
            # print(ExecOut)
    if service:
        print('_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_')
        ServOut = FinalOut.groupby('Service').agg(
            {'TurnAroundTime': 'sum', 'AllowableTaT': 'sum', 'StartTime': 'sum', 'EndTime': 'sum', 'NownCount': 'sum',
             'BlockUtilization': 'mean', 'UnderUtilTime': 'sum', 'Extra Surgeries': 'sum'})

        GraphData = ServOut.reset_index()
        GraphData = GraphData.filter(['Service', 'BlockUtilization'], axis=1)
        GraphData['Unutilized'] = 100 - GraphData['BlockUtilization']
        GraphData = GraphData.rename(columns={'BlockUtilization':'Utilized'})
        GraphData = GraphData.to_dict('records')
        print('Service:\t\tMinutes Over Allowable TAT:\tLate Start Minutes:'
              '\tEarly End Minutes:\tNOWN Count:\tBlock Utilization:\tUnder Util Time:\tExtra Surgeries Possible')


        for index, row in ServOut.iterrows():
            curTurnAround = row['AllowableTaT']
            print(
                '{:10s}\t\t{:3.0f}\t\t\t\t{:3.0f}\t\t\t{:3.0f}\t\t\t{:1.0f}\t\t{:5.2f}\t\t\t{:3.0f}\t\t\t{:2.0f}'.format(
                    index, curTurnAround, row['StartTime'], row['EndTime'], row['NownCount'],
                    row['BlockUtilization'], row['UnderUtilTime'], row['Extra Surgeries']))
        print('\n\nAll Information Over Full Period: ' + str(startdate.date()) + ' to ' + str(enddate.date()))
        FullUtilize = ServOut['BlockUtilization'].median()
        FullSum = ServOut.sum()

        print('Average Utilization: ' + str(round(FullUtilize, 2)))
        print('Total Time Lost: ' + str(round(FullSum['UnderUtilTime'] / 60, 2)))
        print('Total Extra Surgeries Possible: ' + str(FullSum['Extra Surgeries']))
        print('Total Turn Around Time Overage Hours: ' + str(
            round((FullSum['TurnAroundTime'] - FullSum['AllowableTaT']) / 60, 2)))
        print('Total Late Start Hours: ' + str(round(FullSum['StartTime'] / 60, 2)))
        print('Total Early End Hours: ' + str(round(FullSum['EndTime'] / 60, 2)))
        print('Total Nown Surgeries: ' + str(FullSum['NownCount']))

    if executive:
        print('****************************************************')
        print('\n\nExecutive Information: ' + str(startdate.date()) + ' to ' + str(enddate.date()))
        print('Day:\t\tMinutes Over Allowable TAT:\tLate Start Minutes:'
              '\tEarly End Minutes:\tNOWN Count:\tBlock Utilization:\tUnder Util Time:\tExtra Surgeries Possible')
        ExecOut = FinalOut.groupby('Day').agg(
            {'TurnAroundTime': 'sum', 'AllowableTaT': 'sum', 'StartTime': 'sum', 'EndTime': 'sum', 'NownCount': 'sum',
             'BlockUtilization': 'mean', 'UnderUtilTime': 'sum', 'Extra Surgeries': 'sum'})
        for index, row in ExecOut.iterrows():
            # print(index)
            # print(row)
            curTurnAround = row['TurnAroundTime'] - row['AllowableTaT']
            print(
                '{:10s}\t\t{:3.0f}\t\t\t\t{:3.0f}\t\t\t{:3.0f}\t\t{:1.0f}\t\t{:5.2f}\t\t\t{:3.0f}\t\t\t{:2.0f}'.format(
                    str(index.date()), curTurnAround, row['StartTime'], row['EndTime'], row['NownCount'],
                    row['BlockUtilization'], row['UnderUtilTime'], row['Extra Surgeries']))
        print('\n\nAll Information Over Full Period: ' + str(startdate.date()) + ' to ' + str(enddate.date()))
        FullUtilize = ExecOut['BlockUtilization'].median()
        FullSum = ExecOut.sum()
        print('Average Utilization: ' + str(round(FullUtilize, 2)))
        print('Total Time Lost: ' + str(round(FullSum['UnderUtilTime'] / 60, 2)))
        print('Total Extra Surgeries Possible: ' + str(FullSum['Extra Surgeries']))
        print('Total Turn Around Time Overage Hours: ' + str(
            round((FullSum['TurnAroundTime'] - FullSum['AllowableTaT']) / 60, 2)))
        print('Total Late Start Hours: ' + str(round(FullSum['StartTime'] / 60, 2)))
        print('Total Early End Hours: ' + str(round(FullSum['EndTime'] / 60, 2)))
        print('Total Nown Surgeries: ' + str(FullSum['NownCount']))

    db_connection.dispose()


def simple_statistics(startdate, enddate):
    db = pymysql.Connect(host='sherpa-database.cj5ympuvmpqg.us-east-2.rds.amazonaws.com', user='admin',
                         password='SherpaPw123', db='Sherpa')
    cursor = db.cursor()
    sqlQuery = "Select `Service`, Count(*) from `OR-Data` WHERE `Surgery Date`>= %s and `Surgery Date` <= %s" \
               "Group By `Service`", (startdate, enddate)
    cursor.execute(*sqlQuery)
    values = cursor.fetchall()
    total = 0
    NOWN = 0
    result = 'Service:\tCount:'
    for value in values:
        result += '\n' + str(value[0]) + '\t\t' + str(value[1])
        if value[0] != 'NOWN':
            total += int(value[1])
        else:
            NOWN += int(value[1])
    sqlQuery = "SELECT Count(*) from `OR-Data` WHERE `Surgery Date`>= %s and `Surgery Date` <= %s " \
               "and `Patient Type` ='INPATIENT' and 'Service'!= 'NOWN'", (startdate, enddate)
    cursor.execute(*sqlQuery)
    values = cursor.fetchone()
    percInpatient = round(int(values[0]) / total * 100, 2)
    print('-----------------------------------------------------------------------------------------------------\n\n')
    print("All Surgeries over the Period:")
    print("Total Surgeries: " + str(total))
    print("Total NOWN Surgeries: " + str(NOWN))
    print("Inpatients: " + str(percInpatient) + "%")
    print("Outpatients: " + str(round((100 - percInpatient), 2)) + "%")
    print("Total Surgeries Broken up by service")
    print(result)
    print('-----------------------------------------------------------------------------------------------------\n\n')
    print('Surgeries Performed During Elective Hours:')
    sqlQuery = "SELECT `Service`, `Admission Type`, Count(*) from `OR-Data` where `Surgery Date` >= %s " \
               "and `Surgery Date` <=%s and time(`Scheduled Start Time`) between '08:00:00' and '15:30:00' " \
               "and dayofweek(`Surgery Date`) in (2,3,4,5,6) and `Service`!= 'NOWN' " \
               "Group By `Service`, `Admission Type` != 'ELECTIVE'" \
               " Order By `Service`, `Admission Type`", (startdate, enddate)
    cursor.execute(*sqlQuery)
    values = cursor.fetchall()
    total = 0

    result = 'Service:\tAdmission Type:\t\tCount:'
    for value in values:
        if value[1] is None:
            result += '\n' + value[0] + '\t\tNo Entry\t\t' + str(value[2])
        elif value[1] != 'ELECTIVE':
            result += '\n' + value[0] + '\t\tNon Elective\t\t' + str(value[2])
        else:
            result += '\n' + value[0] + '\t\tElective\t\t' + str(value[2])
        total += int(value[2])
    sqlQuery = "SELECT count(*) from `OR-Data` where `Surgery Date` >= %s " \
               "and `Surgery Date` <=%s and time(`Scheduled Start Time`) between '08:00:00' and '15:30:00' " \
               "and dayofweek(`Surgery Date`) in (2,3,4,5,6) and `Service`!= 'NOWN' and `Patient Type` = 'INPATIENT'" \
        , (startdate, enddate)
    cursor.execute(*sqlQuery)
    values = cursor.fetchone()
    percInpatient = round(int(values[0]) / total * 100, 2)
    print("Total Surgeries: " + str(total))
    print("Inpatients: " + str(percInpatient) + "%")
    print("Outpatients: " + str(round((100 - percInpatient), 2)) + "%")
    print("Total Surgeries Broken up by service:\n")
    print(result)
    print('-----------------------------------------------------------------------------------------------------\n\n')
    print('Surgeries Outside Elective Hours:')
    sqlQuery = "SELECT `Service`, `Admission Type`, Count(*) from `OR-Data` where `Surgery Date` >= %s " \
               "and `Surgery Date` <=%s and ((!(time(`Scheduled Start Time`) between '08:00:00' and '15:30:00') " \
               "and dayofweek(`Surgery Date`) in (2,3,4,5,6)) or dayofweek(`Surgery Date`) in (1,7)) " \
               "and `Service`!= 'NOWN' GROUP By `Service`, `Admission Type`!= 'ELECTIVE'" \
               " ORDER  By `Service`, `Admission Type`", (startdate, enddate)
    cursor.execute(*sqlQuery)
    values = cursor.fetchall()
    total = 0
    result = 'Service:\tAdmission Type:\t\tCount:'
    for value in values:
        if value[1] is None:
            result += '\n' + value[0] + '\t\tNo Entry\t\t' + str(value[2])
        elif value[1] != 'ELECTIVE':
            result += '\n' + value[0] + '\t\tNon Elective\t\t' + str(value[2])
        else:
            result += '\n' + value[0] + '\t\tElective\t\t' + str(value[2])
        total += int(value[2])

    sqlQuery = "SELECT Count(*) from `OR-Data` where `Surgery Date` >= %s " \
               "and `Surgery Date` <=%s and ((!(time(`Scheduled Start Time`) between '08:00:00' and '15:30:00') " \
               "and dayofweek(`Surgery Date`) in (2,3,4,5,6)) or dayofweek(`Surgery Date`) in (1,7)) " \
               "and `Service`!= 'NOWN' and `Patient Type` = 'INPATIENT'", (startdate, enddate)
    cursor.execute(*sqlQuery)
    values = cursor.fetchone()
    percInpatient = round(int(values[0]) / total * 100, 2)
    print("Total Surgeries: " + str(total))
    print("Inpatients: " + str(percInpatient) + "%")
    print("Outpatients: " + str(round((100 - percInpatient), 2)) + "%")
    print("Total Surgeries Broken up by service:\n")
    print(result)
    print('-----------------------------------------------------------------------------------------------------\n\n')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('Choose Simple Statistics (0) or Utilization (1):')
    Utilization = input()
    print('Choose the date range type you will enter:\nA: Year\nB: Month\nC: Week Range\nD: YTD')
    dateType = input()
    dateStart = None
    dateEnd = None
    if str(dateType).upper() == 'A':
        print('Enter the Years, separated by a space: ')
        tempDate = input()
        dateinfo = tempDate.split()
        dateStart = datetime.datetime.strptime(dateinfo[0] + "04" + "01", "%Y%m%d")
        dateEnd = datetime.datetime.strptime(dateinfo[1] + "3" + "31", "%Y%m%d")

    elif str(dateType).upper() == 'B':
        print('Enter the Month and Year, separated by a space: ')
        tempDate = input()
        dateinfo = tempDate.split()
        monthnum = datetime.datetime.strptime(dateinfo[0], "%B").month
        lastday = calendar.monthrange(int(dateinfo[1]), monthnum)[1]

        dateStart = datetime.datetime.strptime('01' + str(monthnum) + dateinfo[1], '%d%m%Y')
        dateEnd = datetime.datetime.strptime(str(lastday) + str(monthnum) + dateinfo[1], '%d%m%Y')

    elif str(dateType).upper() == 'C':
        print('Enter The First Day: ')
        tempDate = input()
        dateStart = datetime.datetime.strptime(tempDate, '%d/%m/%y')
        print('Enter The Second Day')
        tempDate = input()
        dateEnd = datetime.datetime.strptime(tempDate, '%d/%m/%y')

    elif str(dateType).upper() == 'D':
        dateEnd = datetime.datetime.now()
        if dateEnd.month > 4 or (dateEnd.month == 4 and dateEnd.day > 1):
            dateStart = datetime.datetime.strptime('01' + '04' + str(dateEnd.year), '%d%m%Y')
        else:
            dateStart = datetime.datetime.strptime('01' + '04' + str(dateEnd.year), '%d%m%Y')
    else:
        dateType = ''

    if dateType != '':
        if Utilization == '0':
            simple_statistics(dateStart, dateEnd)
        elif Utilization == '1':
            print('Executive View? (Yes for yes, anything else = no)')
            exec = input()
            if exec.upper() == 'YES':
                exec = True
            else:
                exec = False
            print('View By Service? (Yes for yes, anything else = no)')
            service = input()
            if service.upper() == 'YES':
                service = True
            else:
                service = False
            utilization(dateStart, dateEnd, exec, service)
