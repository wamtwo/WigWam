import os
import time
import numpy as np
from config import WWSettings
from sqlalchemy import create_engine, Table, select, MetaData, insert, delete, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.functions import now, count
from sqlalchemy.sql import and_, or_
import datetime

#engine = create_engine("mssql+pyodbc://" + username + ":" + password + "@wigwam.database.windows.net/BzApiDB?driver=SQL+Server+Native+Client+11.0")
engine = create_engine(WWSettings.engineString)

metadata = MetaData()

def writeCharNames(target_table, namelist): #writing every single entry with commit into DB. Low performance, high error tolerance.
    if "'" in target_table: target_table = target_table.replace("'", "-")
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    countrejected = 0
    starttime = time.clock()
    print("Writing to Database Table \"" + target_table + "\"")


    with engine.connect() as connection:
        for index, tuple in enumerate(namelist):
            stmt = insert(table).values(Char_Name=tuple[1], Server_Name=tuple[0])
            try:
                #engine.execute(stmt)
                connection.execute(stmt)
            except IntegrityError as ierror:
                #trans.rollback()
                countrejected += 1
                print("Integrity Error for {}".format(tuple[1]))
                #raise
            print(str(index) + "/" + str(len(namelist)), end="")
            print("\r", end="")
    
    elapsed = (time.clock() - starttime)
    print("Done. \t --- \t %.2f seconds." %elapsed, end="\n\n")
    return "{} entries given. {} entries rejected.".format(len(namelist), countrejected)


def writeCharNamesAtOnce(target_table, namelist, verbosity=False, chunks=10): # writing all entries at once. Higher performance, low/no error tolerance
    if chunks > 1000 or chunks < 1: return "Chunksize error: Chunksize can only be between 1 and 1000"
    if verbosity == False and chunks not in [1,10]: print("Disregarding chunk size for verbosity=false")
    print("Writing to Database Table \"" + target_table + "\"")

    if "'" in target_table: target_table = target_table.replace("'", "-")

    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    entries = []
    countrejected = 0
    starttime = time.clock()

    comparedata = getCharNames(target_table)
    compareset = set()

    for row in comparedata:
        compareset.add((row[0].lower(), row[1].lower()))
    
    namelist_cleaned = []
    for tuple in namelist:
        if (tuple[0].lower(), tuple[1].lower()) not in compareset: namelist_cleaned.append(tuple)
        else: countrejected += 1

    ierrlist = []
  
    with engine.connect() as connection:

        if verbosity == True and len(namelist_cleaned) <= chunks: print("Too few entries for selected chunk size. Setting chunk to 1.")
        if verbosity == True and len(namelist_cleaned) >= chunks:
            splitlist = np.array_split(namelist_cleaned,chunks)

            i = 0
            for list in splitlist:
                for tuple in list:
                    entries.append({"Char_Name":tuple[1], "Server_Name":tuple[0]})
                stmt = insert(table)

                if len(entries) > 0:
                    with connection.execution_options(autocommit=False).begin() as trans:
                        try:
                            connection.execute(stmt, entries)
                            trans.commit()
                        except IntegrityError as ier:
                            trans.rollback()
                            print("Integrity Error - attempting to write chunk entry for entry at the end of the process")
                            ierrlist = ierrlist + [(x[0], x[1]) for x in list]
                            #raise
                i += 100 / chunks    
                print(" -> {:.1f}% completed".format(i), end="")
                if i < 100: print("\r", end="")
                else: print("")
                entries = []
        else:
            for tuple in namelist_cleaned:
                entries.append({"Char_Name":tuple[1], "Server_Name":tuple[0]})
            stmt = insert(table)

            if len(entries) > 0:
                with connection.begin() as trans:
                    try:
                        connection.execute(stmt, entries)
                        trans.commit()
                    except IntegrityError as ier:
                        print("Integrity Error")
                        ierrlist = namelist_cleaned
                        trans.rollback()
                        #raise
                print("Completed!")
   
    elapsed = (time.clock() - starttime)
    #print("Done. \t --- \t %.2f seconds." %elapsed, end="\n\n")

    if len(ierrlist) > 0:
        print("{} integrity Error candidates. Writing entries for entry.".format(len(ierrlist)))
        print(writeCharNames(target_table, ierrlist))

    return "Done. {} Entries given. Rejections: {} \t --- \t {:.2f} seconds".format(len(namelist), countrejected, elapsed)




def getCharNames(target_table, amount=0):  #fetches charnames+realms from staging tables (e.g. Server_Malfurion)
    if "'" in target_table: target_table = target_table.replace("'", "-")
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    stmt = select([table.columns.Server_Name, table.columns.Char_Name, table.columns.transferred_on])
    if amount > 0: stmt = stmt.limit(amount)
    with engine.connect() as connection:
        result = connection.execute(stmt).fetchall()
    if len(result) == 0: return[("Empty", "Empty")]
    resultlist = []
    for row in result:
        resultlist.append((row[0], row[1], row[2]))
    
    return resultlist


def getServerbyID(id): #fetches Servername by ID from Serverlist
    table = Table("serverlist", metadata, autoload=True, autoload_with=engine)
    stmt = select([table.columns.Servername, table.columns.scanned_on, table.columns.Skip]).where(table.columns.Server_ID == id)
    with engine.connect() as connection:
        result = connection.execute(stmt).fetchall()
    if len(result) == 0: return ("Error", "No Server for id {}".format(id))
    else: return (result[0]["Servername"], result[0]["scanned_on"], result[0]["Skip"])


def updateServerScanbyID(id): #updates the "scanned on" row in serverlist
    table = Table("serverlist", metadata, autoload=True, autoload_with=engine)
    stmt = update(table).values(scanned_on = now())
    stmt = stmt.where(table.columns.Server_ID == id)
    with engine.connect() as connection:
        result = connection.execute(stmt)
    return "Updated Scan-Date for Server_ID {}".format(id)

def getNumberofServers():   #return all Server_IDs from serverlist
    table = Table("serverlist", metadata, autoload=True, autoload_with=engine)
    stmt = select([table.columns.Server_ID]).order_by(table.columns.Server_ID.asc())
    with engine.connect() as connection:
        result = connection.execute(stmt).fetchall()
    return [row["Server_ID"] for row in result]

def getUntransferredChars(target_table, chunksize=100, fail_thresh=1): #returns all chars from a staging table that have no "transferred_on" value and a failcount that is below the selected threshold
    if "'" in target_table: target_table = target_table.replace("'", "-")
    if chunksize > 10000 or chunksize < 1:
        print("Error, select chunksize between 1 and 1000.")
        return []
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    stmt = select([table.columns.Server_Name, table.columns.Char_Name, table.columns.transferred_on, table.columns.failed_attempts])
    stmt = stmt.where(and_(table.columns.transferred_on.is_(None), or_(table.columns.failed_attempts < fail_thresh, table.columns.failed_attempts.is_(None))))
    stmt = stmt.limit(chunksize)
    with engine.connect() as connection:
        result = connection.execute(stmt).fetchall()
    return [(row["Server_Name"], row["Char_Name"]) for row in result]

def transferChartogeneral(name, realm, server_id, pclass, lvl, faction, race): #writes a char into the main table (player_general) - deprecated
    table = Table("player_general", metadata, autoload=True, autoload_with=engine)
    values = {"Name": name, "Server_Name":realm, "Server_ID":server_id, "Class": pclass, "lvl":lvl, "Faction":faction, "Race":race,}
    stmt = insert(table)

    with engine.connect() as connection:
        try: 
            connection.execute(stmt, values)
            return True
        except: return False

    return "done."

def setCharasTransferred(target_table, chartuple): #sets the "transferred_on" date for a single char in a staging table - deprecated
    if "'" in target_table: target_table = target_table.replace("'", "-")
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    stmt = table.select().where(and_(table.columns.Char_Name == chartuple[1], table.columns.Server_Name == chartuple[0]))
    stmt2 = table.update().where(and_(table.columns.Char_Name == chartuple[1], table.columns.Server_Name == chartuple[0])).values(transferred_on = now())
    
    
    with engine.connect() as connection:
        try:
            result = connection.execute(stmt).fetchall() #FOR DEBUGGING! Checks if the transferred char is found in the database. 
            if len(result) == 1: connection.execute(stmt2) #marks the char as transferred if it was found in DB
            if len(result) > 1:                             #FOR DEBUGGING
                print("Duplicate values found while updating") 
            if len(result) == 0:                            #FOR DEBUGGING
                print(f"Error while updating: Entry for {chartuple[0]}, {chartuple[1]} not found.")
                return False
            return True
        except:
            return False
    return False


def bulkSetCharasTransferred(target_table, chartuplelist): #set the "transferred_on" date for a list of chars in a staging table
    if "'" in target_table: target_table = target_table.replace("'", "-")
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    errorlist = 0

    
    with engine.connect() as connection:

        for index, chartuple in enumerate(chartuplelist): #iterating over the list after the connection was established
            print("Updating entry {}/{}".format(index+1, len(chartuplelist)), end="")
            print("\r", end="")
            stmt = table.select().where(and_(table.columns.Char_Name == chartuple[1], table.columns.Server_Name == chartuple[0]))
            stmt2 = table.update().where(and_(table.columns.Char_Name == chartuple[1], table.columns.Server_Name == chartuple[0])).values(transferred_on = now())
            try:
                result = connection.execute(stmt).fetchall() #FOR DEBUGGING! Checks if the transferred char is found in the database.
                if len(result) == 1: connection.execute(stmt2) #marks the char as transferred if it was found in DB
                if len(result) > 1:                             #FOR DEBUGGING!
                    print("Duplicate values found while updating")  
                    errorlist += 1
                if len(result) == 0:                            #FOR DEBUGGING!
                    print(f"Error while updating: Entry for {chartuple[0]}, {chartuple[1]} not found. Removing Entry from player_general.")
                    errorlist += 1
                    print("Removed: {}".format(removeCharfromGeneral(chartuple[1], chartuple[0])))
            except Exception as exc:
                print(f"unhandled exception! \n {exc}")
                raise
    return errorlist


def removeCharfromGeneral(charname, server_name): #removes a char from main table
    table = Table("player_general", metadata, autoload=True, autoload_with=engine)
    stmt = table.select().where(and_(table.columns.Name == charname, table.columns.Server_Name == server_name))
    stmt2 = table.delete().where(and_(table.columns.Name == charname, table.columns.Server_Name == server_name))
    with engine.connect() as connection:
        try:
            result = connection.execute(stmt).fetchall()  #FOR DEBUGGING! Checks if the char is in the DB before removing
            if len(result) == 1:
                connection.execute(stmt2)
                return True
            if len(result) > 1:                 #FOR DEBUGGING!
                print("Duplicate values found while deleting! ENTRIES NOT DELETED!")
                return False
            if len(result) <= 0:                   #FOR DEBUGGING!
                print(f"Error while deleting: Entry for {server_name}, {charname} not found.")
                return False
        except:
            return False

    return False


def removeCharfromTable(target_table, chartuple):   #removes a char from a staging table
    if "'" in target_table: target_table = target_table.replace("'", "-")
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    stmt = delete(table)
    stmt = stmt.where(and_(table.columns.Char_Name == chartuple[1], table.columns.Server_Name == chartuple[0]))

    with engine.connect() as connection:
        try:
            result = connection.execute(stmt)
            return True
        except:
            return False

    return False

def bulktransferChartoGeneral(infodictlist, id):    #writes a list of chars into the main table
    print("Writing to Database Table \"player_general\"")
    table = Table("player_general", metadata, autoload=True, autoload_with=engine)
    values = []

    for infodict in infodictlist:
        values.append({"Name": infodict["name"].lower(), "Server_Name":infodict["realm"].lower(), "Server_ID":id, "Class": infodict["class"],
                      "lvl":infodict["level"], "Faction":infodict["faction"], "Race":infodict["race"]})

    stmt = insert(table)

    with engine.connect() as connection:
        with connection.execution_options(autocommit=False).begin() as trans:
            try: 
                connection.execute(stmt, values)
                trans.commit()
                return True
            except:                     #Rollback on exception so no "untracked" entries will be writting into DB
                print("Exception occurred")
                trans.rollback()
                return False

    return False


def getCountsServerTable(target_table): #returns the entry count + count of untransferred chars from a staging table
    if "'" in target_table: target_table = target_table.replace("'", "-")
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    stmt2 = select([count()]).select_from(table)
    stmt2 = stmt2.where(table.columns.transferred_on.is_(None))
    stmt = select([count()]).select_from(table)

    with engine.connect() as connection:
        result = connection.execute(stmt).fetchall()
        result2 = connection.execute(stmt2).fetchall()
    return (result[0]["count_1"], result2[0]["count_1"])


def increaseFailCount(target_table, chartuple): #increases the failed_attempts for a char in a staging table
    if "'" in target_table: target_table = target_table.replace("'", "-")
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    stmt = table.select().where(and_(table.columns.Char_Name == chartuple[1], table.columns.Server_Name == chartuple[0]))
    
    
    with engine.connect() as connection:
        try:
            result = connection.execute(stmt).fetchall()  #FOR DEBUGGING! Checks if char exists in DB before updating failcount
            if len(result) == 1:
                if result[0]["failed_attempts"] == None: failCount = 1
                else: failCount = result[0]["failed_attempts"] + 1
                stmt2 = table.update().where(and_(table.columns.Char_Name == chartuple[1], table.columns.Server_Name == chartuple[0])).values(failed_attempts = failCount)
                connection.execute(stmt2)
                return True
            if len(result) > 1:                     #FOR DEBUGGING!
                print("Duplicate values found while updating failed Attempts")
                return False
            if len(result) == 0:                    #FOR DEBUGGING!
                print(f"Error while updating failed Attepmpts: Entry for {chartuple[0]}, {chartuple[1]} not found.")
                return False
        except:
            return False
    return False


def getConnectedRealms(id): #returns a set of all realms beloging to the same server_id
    table = Table("realmlist", metadata, autoload=True, autoload_with=engine)
    stmt = select([table.columns.Realm_Name]).where(table.columns.Server_ID == id)

    with engine.connect() as connection:
        try:
            result = connection.execute(stmt).fetchall()
        except Exception as exc:
            print(exc)
            raise
        else:
            if len(result) < 1: return {}
            if len(result) > 0: return {row["Realm_Name"].lower() for row in result}



def getCharsfromGeneral(id, lvl=120, chunksize=100, days=7, fail_thresh=3): #returns chars that from a specific server id from the main table
    if chunksize > 10000 or chunksize < 1:
        print("Error, select chunksize between 1 and 1000.")
        return []
    lookuptime = datetime.datetime.now() - datetime.timedelta(days=days)
    #lookuptime = datetime.datetime.now() - datetime.timedelta(hours=2)
    table = Table("player_general", metadata, autoload=True, autoload_with=engine)
    stmt = table.select()
    stmt = stmt.where(and_(table.columns.Server_ID == id, table.columns.lvl == lvl, 
                           or_(table.columns.scanned_on <= lookuptime, table.columns.scanned_on.is_(None)),
                           or_(table.columns.failed_attempts < fail_thresh, table.columns.failed_attempts.is_(None))))
    stmt = stmt.limit(chunksize)
    with engine.connect() as connection:
        result = connection.execute(stmt).fetchall()
    return [{item[0]:item[1] for item in row.items()} for row in result]


def bulkUpdateCharsGeneral(charlist): #updates a chunk of entries for player_general
    table = Table("player_general", metadata, autoload=True, autoload_with=engine)
    errorlist = 0

    
    with engine.connect() as connection:

        for index, entry in enumerate(charlist): #iterating over the list after the connection was established
            print("Updating entry {}/{}".format(index+1, len(charlist)), end="")
            print("\r", end="")
            stmt = table.select().where(table.columns.Player_ID == entry["Player_ID"])
            stmt2 = table.update().where(table.columns.Player_ID == entry["Player_ID"])
            stmt2 = stmt2.values(Class=entry["Class"], lvl=entry["lvl"], Faction=entry["Faction"], Race=entry["Race"], scanned_on=now(), failed_attempts=entry["failed_attempts"])
            try:
                result = connection.execute(stmt).fetchall() #FOR DEBUGGING! Checks if the transferred char is found in the database.
                if len(result) == 1: connection.execute(stmt2) #marks the char as transferred if it was found in DB
                if len(result) > 1:                             #FOR DEBUGGING!
                    print("Duplicate values found while updating")  
                    errorlist += 1
                if len(result) == 0:                            #FOR DEBUGGING!
                    print(f"Error while updating: Entry for {chartuple[0]}, {chartuple[1]} not found. Removing Entry from player_general.")
                    errorlist += 1
                    print("Removed: {}".format(removeCharfromGeneral(chartuple[1], chartuple[0])))
            except Exception as exc:
                print(f"unhandled exception! \n {exc}")
                raise
    return errorlist


def bulkWriteBG(bgdictlist):    #writes a list of chars into the main table
    print(f"Writing {len(bgdictlist)} entries (single chunk) to Database Table \"player_bg\"")
    table = Table("player_bg", metadata, autoload=True, autoload_with=engine)


    stmt = insert(table)

    with engine.connect() as connection:
        with connection.execution_options(autocommit=False).begin() as trans:
            try: 
                connection.execute(stmt, bgdictlist)
                trans.commit()
                print("Success.")
                return True
            except:                     #Rollback on exception so no "untracked" entries will be writting into DB
                print("Exception occurred")
                trans.rollback()
                return False

    return False

def getCountfromGeneral(id, lvl, days, fail_thresh):
    table = Table("player_general", metadata, autoload=True, autoload_with=engine)
    lookuptime = datetime.datetime.now() - datetime.timedelta(days=days)

    stmt = select([count()]).select_from(table)
    stmt = stmt.where(and_(table.columns.Server_ID == id, table.columns.lvl == lvl, 
                           or_(table.columns.scanned_on <= lookuptime, table.columns.scanned_on.is_(None)),
                           or_(table.columns.failed_attempts < fail_thresh, table.columns.failed_attempts.is_(None))))

    with engine.connect() as connection:
        result = connection.execute(stmt).fetchall()
    return result[0]["count_1"]



if __name__ == "__main__":

    #testresult = getCharNames("Server_Blackmoore", 0)

    #table = Table("Server_Blackmoore", metadata, autoload=True, autoload_with=engine)   

    #print("Writing to LAN DB")
    #print(writeCharNamesAtOnce("Server_Blackmoore", testresult, verbosity=True, chunks=100))

    #testset = []
    #for i in range(20000):
    #    testset.append(("Char_" + str(i), "Server1"))
        
    #writeCharNames("Server_Malfurion_copy", [("Char_300", "Server1"), ("Char_600", "Server1"), ("Char_900", "Server1")])
    #print(writeCharNames("Server_Malfurion_copy", testset))
    #stmt = delete(table)
    #connection.execute(stmt)

    #getCountsServerTable("Server_Antonidas")

    rslt = getCharsfromGeneral(3)


    print("done.")