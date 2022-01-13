#!/usr/bin/env python3
# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


from datetime import datetime
import connection
import pandas as pd

def main():
    # TODO: Connect to MYSQL with your USERNAME and PASSWORD, by replacing them bellow:
    conn = connection.connect('USERNAME', 'PASSWORD')

    # Create the tables.
    sqldata = "Create table data(VAERS_ID int(6), RECVDATE date null, STATE varchar(7) null, AGE_YRS float null, " \
              "CAGE_YR float null, CAGE_MO float null, SEX varchar(7) null, RPT_DATE date null, " \
              "SYMPTOM_TEXT longtext null, DIED varchar(7) null, DATEDIED date null, L_THREAT varchar(7) null, " \
              "ER_VISIT varchar(7) null, HOSPITAL varchar(7) null, HOSPDAYS int(3) null, X_STAY varchar(7) null, " \
              "DISABLE varchar(7) null, RECOVD varchar(7) null, VAX_DATE date null, ONSET_DATE date null, " \
              "NUMDAYS int(5) null, LAB_DATA longtext null, V_ADMINBY varchar(7) null, V_FUNDBY varchar(7) null, " \
              "OTHER_MEDS varchar(240) null, CUR_ILL longtext null, HISTORY longtext null, " \
              "PRIOR_VAX varchar(128) null, SPLTTYPE varchar(25) null, FORM_VERS int(1) null, TODAYS_DATE date null, " \
              "BIRTH_DEFECT varchar(7) null, OFC_VISIT varchar(7) null, ER_ED_VISIT varchar(7) null, " \
              "ALLERGIES longtext null)"

    sqlvax = "Create table vax(VAERS_ID int(6) null, VAX_TYPE varchar(15) null, VAX_MANU varchar(40) null, " \
             "VAX_LOT varchar(15) null, VAX_DOSE_SERIES varchar(7) null, VAX_ROUTE varchar(7) null, " \
             "VAX_SITE varchar(7) null, VAX_NAME varchar(100) null)"

    sqlsymptoms = "Create table symptoms(VAERS_ID int(6), SYMPTOM1 varchar(100) null, SYMPTOMVERSION1 float null, " \
                  "SYMPTOM2 varchar(100) null, SYMPTOMVERSION2 float null, SYMPTOM3 varchar(100) null, " \
                  "SYMPTOMVERSION3 float null, SYMPTOM4 varchar(100) null, SYMPTOMVERSION4 float null, " \
                  "SYMPTOM5 varchar(100) null, SYMPTOMVERSION5 float null)"

    # Execute the table creation queries and commit them.
    cursor = conn.cursor()
    cursor.execute(sqldata)
    cursor.execute(sqlvax)
    cursor.execute(sqlsymptoms)
    conn.commit()

    # Create the base names for each of the csv types.
    vbasename = 'VAERSVAX.csv'
    sbasename = 'VAERSSYMPTOMS.csv'
    dbasename = 'VAERSDATA.csv'

    # Now process the data for each year into the database 'vax' table.
    for x in range(1990, 2022):
        vname = str(x) + vbasename
        print(vname)
        # Read the data and place it into a dataframe. NOTE THE ENCODING.
        vdata = pd.read_csv(vname, encoding="Windows-1252", delimiter=",")
        vdataframe = pd.DataFrame(data=vdata)

        # Manage all the empty values.
        for v in vdataframe.values:
            for i in range(0, 8):
                if pd.isna(v[i]) and isinstance(v[i], str):
                    v[i] = None
                elif pd.isna(v[i]) and isinstance(v[i], float):
                    v[i] = None
                elif pd.isna(v[i]) and isinstance(v[i], int):
                    v[i] = None

            # Insert them into the database table.
            cursor = conn.cursor()
            sql = 'INSERT INTO vax (VAERS_ID, VAX_TYPE, VAX_MANU, VAX_LOT, VAX_DOSE_SERIES, VAX_ROUTE, VAX_SITE, ' \
                  'VAX_NAME) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
            val = (v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7])
            cursor.execute(sql, val)

        conn.commit()

    # Now process the data for each year into the database 'symptoms' table.
    for x in range(1990, 2022):
        sname = str(x) + sbasename
        print(sname)
        # Read the data and place it into a dataframe. NOTE THE ENCODING.
        sdata = pd.read_csv(sname, encoding="Windows-1252", delimiter=",")
        sdataframe = pd.DataFrame(data=sdata)

        # Manage all the empty values.
        for s in sdataframe.values:
            for i in range(0, 11):
                if pd.isna(s[i]) and isinstance(s[i], str):
                    s[i] = None
                elif pd.isna(s[i]) and isinstance(s[i], float):
                    s[i] = None
                elif pd.isna(s[i]) and isinstance(s[i], int):
                    s[i] = None

            # Insert them into the database table.
            cursor = conn.cursor()
            sql = 'INSERT INTO symptoms (VAERS_ID, SYMPTOM1, SYMPTOMVERSION1, SYMPTOM2, SYMPTOMVERSION2, ' \
                  'SYMPTOM3, SYMPTOMVERSION3, SYMPTOM4, SYMPTOMVERSION4, SYMPTOM5, SYMPTOMVERSION5) ' \
                  'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            val = (s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8], s[9], s[10])
            cursor.execute(sql, val)

        conn.commit()

    # Now process the data for each year into the database 'data' table.
    for x in range(1990, 2022):
        dname = str(x) + dbasename

        # Read the data and place it into a dataframe. NOTE THE ENCODING.
        ddata = pd.read_csv(dname, encoding="Windows-1252", delimiter=",")
        ddataframe = pd.DataFrame(data=ddata)

        # Using the dataframe, we want to first work with the RECVDATE column to make sure it is formatted as a date.
        ddataframe["RECVDATE"] = pd.to_datetime(ddataframe["RECVDATE"], format="%m/%d/%Y")
        ddataframe["RECVDATE"] = ddataframe["RECVDATE"].dt.strftime("%Y-%m-%d")

        # Using the dataframe, we want to first work with the RPT_DATE column to make sure it is formatted as a date.
        ddataframe["RPT_DATE"] = pd.to_datetime(ddataframe["RPT_DATE"], format="%m/%d/%Y")
        ddataframe["RPT_DATE"] = ddataframe["RPT_DATE"].dt.strftime("%Y-%m-%d")

        # Using the dataframe, we want to first work with the DATEDIED column to make sure it is formatted as a date.
        ddataframe["DATEDIED"] = pd.to_datetime(ddataframe["DATEDIED"], format="%m/%d/%Y")
        ddataframe["DATEDIED"] = ddataframe["DATEDIED"].dt.strftime("%Y-%m-%d")

        # Using the dataframe, we want to first work with the VAX_DATE column to make sure it is formatted as a date.
        ddataframe["VAX_DATE"] = pd.to_datetime(ddataframe["VAX_DATE"], format="%m/%d/%Y")
        ddataframe["VAX_DATE"] = ddataframe["VAX_DATE"].dt.strftime("%Y-%m-%d")

        # Using the dataframe, we want to first work with the TODAYS_DATE column to make sure it is formatted as a date.
        ddataframe["TODAYS_DATE"] = pd.to_datetime(ddataframe["TODAYS_DATE"], format="%m/%d/%Y")
        ddataframe["TODAYS_DATE"] = ddataframe["TODAYS_DATE"].dt.strftime("%Y-%m-%d")

        # Manage all the empty values.
        for d in ddataframe.values:
            # First manage empty dates.
            dates = [1, 7, 10, 18, 19, 30]
            for i in dates:
                if pd.isna(d[i]):
                    d[i] = None

                if isinstance(d[i], datetime) or pd.isna(d[i]):
                    pass
                elif isinstance(d[i], str) and d[i][2] == "/" or isinstance(d[i], str) and d[i][1] == "/":
                    d[i] = pd.to_datetime(d[i], format="%m/%d/%Y")
                    d[i] = d[i].strftime("%Y-%m-%d")

            # Now manage empty data.
            for i in range(0, 35):
                if i not in dates:
                    if pd.isna(d[i]):
                        if i == 4:
                            d[i] = None
                        elif i == 5:
                            d[i] = None
                        elif isinstance(d[i], str) and len(d[i]) < 1:
                            d[i] = None
                        elif isinstance(d[i], float):
                            d[i] = None
                        elif isinstance(d[i], int):
                            d[i] = None
                        else:
                            pass

            # Insert them into the database table.
            cursor = conn.cursor()
            sql = 'INSERT INTO data (VAERS_ID, RECVDATE, STATE, AGE_YRS, CAGE_YR, CAGE_MO, SEX, RPT_DATE, ' \
                  'SYMPTOM_TEXT, DIED, DATEDIED, L_THREAT, ER_VISIT, HOSPITAL, HOSPDAYS, X_STAY, DISABLE, ' \
                  'RECOVD, VAX_DATE, ONSET_DATE, NUMDAYS, LAB_DATA, V_ADMINBY, V_FUNDBY, OTHER_MEDS, CUR_ILL, ' \
                  'HISTORY, PRIOR_VAX, SPLTTYPE, FORM_VERS, TODAYS_DATE, BIRTH_DEFECT, OFC_VISIT, ER_ED_VISIT, ' \
                  'ALLERGIES) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ' \
                  '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            val = (d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10], d[11], d[12], d[13], d[14],
                   d[15], d[16], d[17], d[18], d[19], d[20], d[21], d[22], d[23], d[24], d[25], d[26], d[27], d[28],
                   d[29], d[30], d[31], d[32], d[33], d[34])
            cursor.execute(sql, val)

        print(f'{dname}')

        conn.commit() # Do the commits all at once.


if __name__ == '__main__':
    main()
