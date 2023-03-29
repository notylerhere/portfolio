# import packages
import sqlite3
import pymongo
import pandas as pd
import os


# ISMG 6020 Assignment 2 (Final Submission)
# Created by Tyler Watson 
# Spring 2023
# 2/27/2023
# All code is original! 

# change the working directory
os.chdir(r'## hidden ##')


# connect to the SQLite database and create a cursor object
def connect_to_sqlite():
    print('Making the connection to the SQLite database...')
    conn = sqlite3.connect('CO_Labor.sqlite')
    conn.text_factory = str
    cur = conn.cursor()
    return cur, conn

# drop the tables if they already exist
def drop_tables(cur):
    print('\n')
    print('Dropping any existing tables...')
    cur.executescript('''DROP TABLE IF EXISTS CO_Labor; DROP TABLE IF EXISTS Address''')

# create the table for the CO_Labor data and populate it with the data from the MongoDB collection
def create_co_labor_table(cur): 
    print('\n')
    print('Creating the CO_Labor table...')

    cur.executescript('''CREATE TABLE IF NOT EXISTS CO_Labor (
        id TEXT PRIMARY KEY, 
        areaname Text, 
        areatype INTEGER,
        area INTEGER,
        periodtype INTEGER,
        period INTEGER,
        pertypdesc Text,
        ownership INTEGER,
        ownertitle Text,
        prelim INTEGER,
        firms INTEGER,
        estab INTEGER,
        mnth1emp INTEGER,
        mnth2emp INTEGER,
        mnth3emp INTEGER,
        topempav INTEGER,
        taxwage INTEGER,
        contrib INTEGER,
        suppress INTEGER,
        statename Text,
        stateabbrv Text,
        stfips INTEGER,
        avgemp INTEGER,
        totwage INTEGER,
        avgwkwage INTEGER,
        periodyear INTEGER,
        indcode INTEGER,
        indcodety INTEGER,
        codetitle TEXT)''')

# connect to the MongoDB database
def connect_to_mongodb():
    print('\n')
    print('Making the connection to MongoDB...')
    client = pymongo.MongoClient("# hidden #")
    # select the database and collection names
    db = client['co_labor']
    collection = db['co_labor_20230227']
    return collection

# loop through the collection and insert the data into the SQLite table
def insert_data_into_sqlite_table(cur, conn, collection):
    print('\n')
    print('Inserting data into SQLite table')
    count = 0
    for record in collection.find():
        count += 1
        id = str(record['_id'])
        areaname = str(record['areaname'])
        areatype = int(record['areatype'])
        area = str(record['area'])
        periodtype = int(record['periodtype'])
        period = int(record['period'])
        pertypdesc = str(record['pertypdesc'])
        ownership = int(record['ownership'])
        ownertitle = str(record['ownertitle'])
        prelim = int(record['prelim'])
        firms = int(record['firms'])
        estab = int(record['estab'])
        mnth1emp = int(record['mnth1emp'])
        mnth2emp = int(record['mnth2emp'])
        mnth3emp = int(record['mnth3emp'])
        topempav = int(record['topempav'])
        taxwage = int(record['taxwage'])
        contrib = int(record['contrib'])
        suppress = int(record['suppress'])
        statename = str(record['statename'])
        stateabbrv = str(record['stateabbrv'])
        stfips = int(record['stfips'])
        avgemp = int(record['avgemp'])
        totwage = int(record['totwage'])
        avgwkwage = int(record['avgwkwage'])
        periodyear = int(record['periodyear'])
        indcode = int(record['indcode'])
        codetitle = str(record['codetitle'])
        try:
            indcodety = record['indcodety']
        except KeyError:
            indcodety = ''


        cur.execute("INSERT INTO CO_Labor VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (id, areaname, areatype, area, periodtype, period, pertypdesc, ownership, ownertitle, prelim, firms, estab, mnth1emp, mnth2emp, mnth3emp, topempav, taxwage, contrib, suppress, statename, stateabbrv, stfips, avgemp, totwage, avgwkwage, periodyear, indcode, codetitle, indcodety))
        
        if count % 50 == 0:
            print(f"    {count} records have been inserted...")

    conn.commit()

    print(f'Successfully inserted {count} records into SQLite table!')


def create_dataframe_from_sqlite_table(conn):
    print('\n')
    print('Creating a dataframe from the CO_Labor table...')
    # create a dataframe from the CO_Labor table
    co_labor_df = pd.read_sql_query("SELECT * FROM CO_Labor", conn)
    print('Viewing the first 10 rows of the CO_Labor table')
    print('\n')
    print(co_labor_df.head(10))
    return co_labor_df

# create a dataframe from the CO_Labor table
def main():
    cur, conn = connect_to_sqlite()
    drop_tables(cur)
    create_co_labor_table(cur)
    collection = connect_to_mongodb()
    insert_data_into_sqlite_table(cur, conn, collection)
    co_labor_df = create_dataframe_from_sqlite_table(conn)
    conn.close()
    return co_labor_df

# view the first 10 rows of the CO_Labor table 
if __name__ == "__main__":
    main()