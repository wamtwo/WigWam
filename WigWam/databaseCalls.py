import os
import time
import numpy as np
from sqlalchemy import create_engine, Table, select, MetaData, insert, delete, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.functions import now
from sqlalchemy.sql import and_


username = os.environ.get("WIGWAMUSER")
password = os.environ.get("WIGWAMPW")
#engine = create_engine("mssql+pyodbc://" + username + ":" + password + "@wigwam.database.windows.net/BzApiDB?driver=SQL+Server+Native+Client+11.0")
engine = create_engine("mssql+pyodbc://" + username + ":" + password + "@192.168.134.122/BzApiDB?driver=SQL+Server+Native+Client+11.0")

metadata = MetaData()

def writeCharNames(target_table, namelist): #writing every single entry with commit into DB. Low performance, high error tolerance.
    if "'" in target_table: target_table = target_table.replace("'", "-")
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    countrejected = 0
    starttime = time.clock()
    print("Writing to Database Table \"" + target_table + "\"")


    with engine.begin() as connection:
        for index, tuple in enumerate(namelist):
            stmt = insert(table).values(Char_Name=tuple[1], Server_Name=tuple[0])
            try:
                #engine.execute(stmt)
                connection.execution_options(autocommit=False).execute(stmt)
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
  
    with engine.execution_options(autocommit=False).begin() as connection:

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
                            connection.execution_options(autocommit=False).execute(stmt, entries)
                        except IntegrityError as ier:
                            print("Integrity Error - attempting to write chunk entry for entry at the end of the process")
                            #trans.rollback()
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
                try:
                    connection.execution_options(autocommit=False).execute(stmt, entries)
                except IntegrityError as ier:
                    print("Integrity Error")
                    ierrlist = namelist_cleaned
                    #raise
                print("Completed!")

    elapsed = (time.clock() - starttime)
    #print("Done. \t --- \t %.2f seconds." %elapsed, end="\n\n")

    if len(ierrlist) > 0:
        print("{} integrity Error candidates. Writing entries for entry.".format(len(ierrlist)))
        print(writeCharNames(target_table, ierrlist))

    return "Done. {} Entries given. Rejections: {} \t --- \t {:.2f} seconds".format(len(namelist), countrejected, elapsed)




def getCharNames(target_table, amount=0):
    if "'" in target_table: target_table = target_table.replace("'", "-")
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    stmt = select([table.columns.Server_Name, table.columns.Char_Name, table.columns.transferred_on])
    if amount > 0: stmt = stmt.limit(amount)
    with engine.begin() as connection:
        result = connection.execute(stmt).fetchall()
    if len(result) == 0: return[("Empty", "Empty")]
    resultlist = []
    for row in result:
        resultlist.append((row[0], row[1], row[2]))
    
    return resultlist


def getServerbyID(id):
    table = Table("serverlist", metadata, autoload=True, autoload_with=engine)
    stmt = select([table.columns.Servername, table.columns.scanned_on, table.columns.Skip]).where(table.columns.Server_ID == id)
    with engine.begin() as connection:
        result = connection.execute(stmt).fetchall()
    if len(result) == 0: return ("Error", "No Server for id {}".format(id))
    else: return (result[0]["Servername"], result[0]["scanned_on"], result[0]["Skip"])


def updateServerScanbyID(id):
    table = Table("serverlist", metadata, autoload=True, autoload_with=engine)
    stmt = update(table).values(scanned_on = now())
    stmt = stmt.where(table.columns.Server_ID == id)
    with engine.begin() as connection:
        result = connection.execute(stmt)
    return "Updated Scan-Date for Server_ID {}".format(id)

def getNumberofServers():
    table = Table("serverlist", metadata, autoload=True, autoload_with=engine)
    stmt = select([table.columns.Server_ID]).order_by(table.columns.Server_ID.asc())
    with engine.begin() as connection:
        result = connection.execute(stmt).fetchall()
    return [row["Server_ID"] for row in result]

def getUntransferredChars(target_table, chunksize=100):
    if "'" in target_table: target_table = target_table.replace("'", "-")
    if chunksize > 1000 or chunksize < 1:
        print("Error, select chunksize between 1 and 1000.")
        return []
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    stmt = select([table.columns.Server_Name, table.columns.Char_Name, table.columns.transferred_on])
    stmt = stmt.where(table.columns.transferred_on.is_(None))
    stmt = stmt.limit(chunksize)
    with engine.begin() as connection:
        result = connection.execute(stmt).fetchall()
    return [(row["Server_Name"], row["Char_Name"], row["transferred_on"]) for row in result]

def transferChartogeneral(name, realm, server_id, pclass, lvl, faction, race):
    table = Table("player_general", metadata, autoload=True, autoload_with=engine)
    values = {"Name": name, "Server_Name":realm, "Server_ID":server_id, "Class": pclass, "lvl":lvl, "Faction":faction, "Race":race,}
    stmt = insert(table)

    with engine.begin() as connection:
        try: 
            connection.execute(stmt, values)
            return True
        except: return False

    return "done."

def setCharasTransferred(target_table, chartuple):
    if "'" in target_table: target_table = target_table.replace("'", "-")
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    stmt = update(table).values(transferred_on = now())
    stmt = stmt.where(and_(table.columns.Char_Name == chartuple[1], table.columns.Server_Name == chartuple[0]))

    with engine.begin() as connection:
        try:
            connection.execute(stmt)
            return True
        except:
            return False

    return False

def removeCharfromGeneral(charname, server_name):
    table = Table("player_general", metadata, autoload=True, autoload_with=engine)
    stmt = delete(table)
    stmt = stmt.where(and_(table.columns.Name == charname, table.colums.Server_Name == server_name))

    with engine.begin() as connection:
        try:
            connection.execute(stmt)
            return True
        except:
            return False

    return False


def removeCharfromTable(target_table, chartuple):
    if "'" in target_table: target_table = target_table.replace("'", "-")
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    stmt = delete(table)
    stmt = stmt.where(and_(table.columns.Char_Name == chartuple[1], table.columns.Server_Name == chartuple[0]))

    with engine.begin() as connection:
        try:
            connection.execute(stmt)
            return True
        except:
            return False

    return False

def bulktransferChartoGeneral(infodictlist, id):
    print("Writing to Database Table \"player_general\"")
    table = Table("player_general", metadata, autoload=True, autoload_with=engine)
    values = []

    for infodict in infodictlist:
        values.append({"Name": infodict["name"].lower(), "Server_Name":infodict["realm"].lower(), "Server_ID":id, "Class": infodict["class"],
                      "lvl":infodict["level"], "Faction":infodict["faction"], "Race":infodict["race"]})

    stmt = insert(table)

    with engine.execution_options(autocommit=False).begin() as connection:
        try: 
            connection.execute(stmt, values)
            return True
        except:
            print("Exception occurred")
            return False

    return False



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


    print("done.")