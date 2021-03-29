import pymysql
import pandas as pd
from datetime import datetime as dtm




global dbName
global hostIP
global hostport
global username
global password
global dbConn
global dbError
global dbOpen
global cur
import pandas as pd
global schema_name


dbConn = 0
dbError = True
dbOpen = False


class DbException(Exception):
    pass

class InsertException(Exception):
    pass

class UpdateException(Exception):
    pass

# DATABASE OPERATION FUNCTIONS
#==============================

def open_database(host, port, user_name, password, schema_):
    global dbConn
    global dbError
    global dbOpen
    global cur
    global schema_name
    schema_name = schema_
    if not dbOpen:
        #if True:
        try:  
            dbConn = pymysql.connect(host=host, port=port, user=user_name, passwd=password, db= schema_)
            cur = dbConn.cursor()
            dbError = False
            dbOpen = True
        except Exception as e:
        	print ('Error: DB: Couldnot open Database: ', sys.exc_info()[0])
        	dbError = True
        	dbOpen = False
        	raise DbException from e
        pass
    pass
pass


def insert_person(PrimeMinister, Rank, Party, StartDate, EndDate, DOB):
    global dbConn
    global dbError
    global cur
    global schema_name
    if not dbError:
        try:
        #if True:
            #cur = dbConn.cursor()
            #cur.execute

            if pd.isna(DOB):
                cur.execute("insert into "+schema_name+".Persons(`PrimeMinister`,`Rank` ,`Party`,`StartDate`,`EndDate`) values('" \
            + str(PrimeMinister) + "','" + str(Rank) + "','" + str(Party) + "','" + str(StartDate) + "','" + str(EndDate) + "')")
            
            else:
                cur.execute("insert into "+schema_name+".Persons(`PrimeMinister`,`Rank` ,`Party`,`StartDate`,`EndDate`,`DOB`) values('" \
            + str(PrimeMinister) + "','" + str(Rank) + "','" + str(Party) + "','" + str(StartDate) + "','" + str(EndDate) + "','" + str(DOB) +"')")

            dbConn.commit()
        except Exception as e:
            print ('Error: DB: Insert Raw data: ', sys.exc_info()[0])
            dbError = True
            raise InsertException from e

def insert_monarch(Monarch_, StartDate_, EndDate_ ):
    global dbConn
    global dbError
    global cur
    global schema_name
    if not dbError:
        try:
        #if True:
            #cur = dbConn.cursor()
            #cur.execute
            
            
            cur.execute("INSERT INTO `C00265741`.`Monarchs`(`Monarch`,`StartDate`,`EndDate`) VALUES( '"+ str(Monarch_) + "','"+ str(StartDate_) + "','" + str(EndDate_) + "');")
            dbConn.commit()
        except Exception as e:
            print ('Error: DB: Insert Raw data: ', sys.exc_info()[0])
            dbError = True
            raise InsertException from e



def db_cmd_execute(strCommand):
    global dbConn
    global dbError
    global cur
    if not dbError:
        try:
            #cur = dbConn.cursor()
            cur.execute(strCommand)
            #dbConn.commit()
            return(cur.fetchall())
        except Exception as e:
            print ('Error: DB: db_cmd_execute: ', sys.exc_info()[0])
            dbError = True
            raise DbException from e
            return(None)

def db_commit():
    global dbConn
    global dbOpen
    global dbError
    try:
         dbConn.commit()
    except Exception as e:
        print ('Error: DB: Closing Database: ', sys.exc_info()[0])
        dbError = True
        raise DbException from e

    
def close_database():
    global dbConn
    global dbOpen
    global dbError
    try:
        dbConn.close()
        dbOpen = False
    except Exception as e:
        print ('Error: DB: Closing Database: ', sys.exc_info()[0])
        dbError = True
        raise DbException from e
        
  
  

def IsError():
    global dbError
    return (dbError)
pass
    

    


def DataBase_connectionCheck():
    try:
        open_database()

        close_database()
    except Exception as e:

        print("Database connectivity error!!!")
        print("Unexpected error:", e.value) 
        raise DbException from e 
pass

