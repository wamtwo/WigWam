import os
import time
import numpy as np
from config import WWSettings
from sqlalchemy import create_engine, Table, select, MetaData, insert, delete, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.functions import now, count
from sqlalchemy.sql import and_, or_
import datetime
import infoRefinement as ir

#engine = create_engine("mssql+pyodbc://" + username + ":" + password + "@wigwam.database.windows.net/BzApiDB?driver=SQL+Server+Native+Client+11.0")
engine = create_engine(WWSettings.engineString, pool_size=30)

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

def getNumberofServers(language):   #return all Server_IDs from serverlist
    table = Table("serverlist", metadata, autoload=True, autoload_with=engine)
    stmt = select([table.columns.Server_ID]).order_by(table.columns.Server_ID.asc()).where(table.columns.Language == language)
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



def getCharsfromGeneral(id, lvl=120, chunksize=100, days=7, fail_thresh=3, only_scanned=False): #returns chars that from a specific server id from the main table
    if chunksize > 10000:
        print("Error, select chunksize between 1 and 10000.")
        return []
    lookuptime = datetime.datetime.now() - datetime.timedelta(days=days)
    #lookuptime = datetime.datetime.now() - datetime.timedelta(hours=2)
    table = Table("player_general", metadata, autoload=True, autoload_with=engine)
    stmt = table.select()
    if only_scanned == False: stmt = stmt.where(and_(table.columns.Server_ID == id, table.columns.lvl >= lvl, 
                                                     or_(table.columns.scanned_on <= lookuptime, table.columns.scanned_on.is_(None)),
                                                     or_(table.columns.failed_attempts < fail_thresh, table.columns.failed_attempts.is_(None))))
    if only_scanned == True: stmt = stmt.where(and_(table.columns.Server_ID == id, table.columns.lvl >= lvl, table.columns.scanned_on <= lookuptime,
                                                     or_(table.columns.failed_attempts < fail_thresh, table.columns.failed_attempts.is_(None))))
    if chunksize > 0: stmt = stmt.limit(chunksize)
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
    print(f"Writing {len(bgdictlist)} entries to Database Table \"player_bg\"")
    table = Table("player_bg", metadata, autoload=True, autoload_with=engine)    
    stmt_insert = insert(table)

    with engine.connect() as connection:
        with connection.execution_options(autocommit=False).begin() as trans:
            try:

                for index, bgdict in enumerate(bgdictlist):
                    print(f"Writing entry {index+1}/{len(bgdictlist)}", end="")
                    print("\r", end="")

                    stmt_select = table.select().where(table.columns.Player_ID == bgdict["Player_ID"])
                    result = connection.execution_options(autocommit=False).execute(stmt_select).fetchall()
                    if len(result) == 0: connection.execution_options(autocommit=False).execute(stmt_insert, bgdict)  #char not in DB -> will be entered
                    if len(result) == 1:
                        bgChangeTuple = ir.calcBgChange(bgdict, result) #checks for and calculates BG stat changes

                        if bgChangeTuple[0] == False:  # -> if char was inactive, update scan_date, don't update BG Info, set inactive flag
                            stmt_update = table.update().where(table.columns.Player_ID == bgdict.pop("Player_ID"))
                            bgdict["scanned_on"] = datetime.datetime.now()
                            bgdict["active"] = False
                            connection.execution_options(autocommit=False).execute(stmt_update, bgdict)

                        if bgChangeTuple[0] == True: # -> if char has changes in BG stats, update char info and bg info
                            table_change = Table("player_bg_change", metadata, autoload=True, autoload_with=engine)
                            stmt_update_bg_all = table.update().where(table.columns.Player_ID == bgdict.pop("Player_ID"))
                            stmt_insert_bg_change = insert(table_change)
                            bgChangeDict = bgChangeTuple[1]
                            bgChangeDict["scanned_on"] = datetime.datetime.now()
                            bgdict["active"] = True
                            connection.execution_options(autocommit=False).execute(stmt_update_bg_all, bgdict)
                            connection.execution_options(autocommit=False).execute(stmt_insert_bg_change, bgChangeDict)

                    if len(result) > 1:
                        print("\nConsitency Error!")
                        trans.rollback()
                        return False
                print("\nSuccess!")

            except Exception as exc:                     #Rollback on exception so no "untracked" entries will be written into DB
                print("\nException occurred")
                trans.rollback()
                return False

            else:
                trans.commit()
                return True

    return False




def getCountfromGeneral(id, lvl, days, fail_thresh):
    table = Table("player_general", metadata, autoload=True, autoload_with=engine)
    lookuptime = datetime.datetime.now() - datetime.timedelta(days=days)

    stmt = select([count()]).select_from(table)
    stmt = stmt.where(and_(table.columns.Server_ID == id, table.columns.lvl >= lvl, 
                           or_(table.columns.scanned_on <= lookuptime, table.columns.scanned_on.is_(None)),
                           or_(table.columns.failed_attempts < fail_thresh, table.columns.failed_attempts.is_(None))))

    with engine.connect() as connection:
        result = connection.execute(stmt).fetchall()
    return result[0]["count_1"]


def getBGData(idlist, change=False):
    if change == True: table = Table("player_bg_change", metadata, autoload=True, autoload_with=engine)
    if change == False: table = Table("player_bg", metadata, autoload=True, autoload_with=engine)
    faillist = []
    resultlist = []

    idlist_chunked = [[idlist[x] for x in range(y*1000, (y+1)*1000)] if (y+1)*1000 <= len(idlist) else [idlist[x] for x in range(y*1000, len(idlist))] for y in range(len(idlist)//1000+1)]

    print(f"\tWriting {len(idlist)} in {len(idlist_chunked)} Chunks.")
    starttime = time.clock()

    with engine.connect() as connection:

        for index, chunk in enumerate(idlist_chunked):
            print(f"\t\tFetching chunk {index+1}/{len(idlist_chunked)}", end="")
            print("\r", end="")

            stmt = table.select().where(table.columns.Player_ID.in_(chunk))

            result = connection.execute(stmt).fetchall()
            for row in result:
                resultlist.append({item[0]:item[1] for item in row.items()})

            if len(chunk) > len(result): 
                for x in range(len(chunk)-len(result)):
                    faillist.append("TBA")

        elapsed = (time.clock() - starttime)
        print(f"\n\tWritten in {elapsed:.2f} seconds")
    return (resultlist, faillist)

def writetoBGServer(DB_bg_dict_list, change=False):
    if change == True: table = Table("BG_server_change", metadata, autoload=True, autoload_with=engine)
    if change == False: table = Table("BG_server", metadata, autoload=True, autoload_with=engine)
    stmt = insert(table)

    with engine.connect() as connection:

        for DB_bg_dict in DB_bg_dict_list:
            print(f"Writing Data of type {DB_bg_dict['Type']} into DB_Server")
            connection.execute(stmt, DB_bg_dict)

    return True    


def getBGServer(id, newest_only=False, change=False):
    if change == True: table = Table("BG_Server_change", metadata, autoload=True, autoload_with=engine)
    if change == False: table = Table("BG_Server", metadata, autoload=True, autoload_with=engine)
    stmt = table.select().where(table.columns.Server_ID == id)
    if newest_only == True:
        with engine.connect() as connection:
            stmt = select([table.columns.Type.distinct()]).where(table.columns.Server_ID == id)
            typerslt = [row["Type"] for row in connection.execute(stmt).fetchall()]
            resultlist = []
            
            for stattype in typerslt:
                stmt = table.select().where(and_(table.columns.Server_ID == id, table.columns.Type == stattype))
                stmt = stmt.order_by(table.columns.scanned_on.desc()).limit(1)
                resultlist.append(connection.execute(stmt).fetchall())

            result_dict = [{item[0]:item[1] for item in row[0].items()} for row in resultlist]

    else:
        with engine.connect() as connection:

            result = connection.execute(stmt).fetchall()

        result_dict = [{item[0]:item[1] for item in row.items()} for row in result]
    return result_dict

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

    #bgdictlist = []
    #bgdictlist.append({"Player_ID": 1, "BG_played": 6, "BG_won": 6, "played_most_n": 6, "played_most_c":6, "won_most_n":6, "won_most_c":6, "BG_played_c":6, "BG_won_c":6,
    #              "AV_played":6, "AV_won":6, "AV_tower_def":6, "AV_tower_cap":6, "AB_played":6, "AB_won":6, "Gil_played":6, "Gil_won":6, "EotS_played":6, "EotS_won":6,
    #              "EotS_flags":6, "EotS_played_est":6, "EotS_won_est":6, "EotS_played_def":6, "EotS_won_def":6, "SS_played":6, "SS_won":6, "SotA_played":6, "SotA_won":6, 
    #              "TP_played":6, "TP_won":6, "TP_flags_cap":6, "TP_flags_ret":6, "WS_played":6, "WS_won":6, "WS_flags_cap":6, "WS_flags_ret":6, "SM_played":6,  "SM_won":6,
    #              "TK_played":6, "TK_won":6, "IoC_played":6, "IoC_won":6, "DG_played":6, "DG_won":6})
    #bgdictlist.append({"Player_ID": 2, "BG_played": 2, "BG_won": 2, "played_most_n": 2, "played_most_c":2, "won_most_n":2, "won_most_c":2, "BG_played_c":2, "BG_won_c":2,
    #              "AV_played":2, "AV_won":2, "AV_tower_def":2, "AV_tower_cap":2, "AB_played":2, "AB_won":2, "Gil_played":2, "Gil_won":2, "EotS_played":2, "EotS_won":2,
    #              "EotS_flags":2, "EotS_played_est":2, "EotS_won_est":2, "EotS_played_def":2, "EotS_won_def":2, "SS_played":2, "SS_won":2, "SotA_played":2, "SotA_won":2, 
    #              "TP_played":2, "TP_won":2, "TP_flags_cap":2, "TP_flags_ret":2, "WS_played":2, "WS_won":2, "WS_flags_cap":2, "WS_flags_ret":2, "SM_played":2,  "SM_won":2,
    #              "TK_played":2, "TK_won":2, "IoC_played":2, "IoC_won":2, "DG_played":2, "DG_won":2})
    #bgdictlist.append({"Player_ID": 3, "BG_played": 9, "BG_won": 9, "played_most_n": 9, "played_most_c":9, "won_most_n":9, "won_most_c":9, "BG_played_c":9, "BG_won_c":9,
    #              "AV_played":9, "AV_won":9, "AV_tower_def":9, "AV_tower_cap":9, "AB_played":9, "AB_won":9, "Gil_played":9, "Gil_won":9, "EotS_played":9, "EotS_won":9,
    #              "EotS_flags":9, "EotS_played_est":9, "EotS_won_est":9, "EotS_played_def":9, "EotS_won_def":9, "SS_played":9, "SS_won":9, "SotA_played":9, "SotA_won":9, 
    #              "TP_played":9, "TP_won":9, "TP_flags_cap":9, "TP_flags_ret":9, "WS_played":9, "WS_won":9, "WS_flags_cap":9, "WS_flags_ret":9, "SM_played":9,  "SM_won":9,
    #              "TK_played":9, "TK_won":9, "IoC_played":9, "IoC_won":9, "DG_played":9, "DG_won":9})
    #bgdictlist.append({"Player_ID": 4, "BG_played": 4, "BG_won": 4, "played_most_n": 4, "played_most_c":4, "won_most_n":4, "won_most_c":4, "BG_played_c":4, "BG_won_c":4,
    #              "AV_played":4, "AV_won":4, "AV_tower_def":4, "AV_tower_cap":4, "AB_played":4, "AB_won":4, "Gil_played":4, "Gil_won":4, "EotS_played":4, "EotS_won":4,
    #              "EotS_flags":4, "EotS_played_est":4, "EotS_won_est":4, "EotS_played_def":4, "EotS_won_def":4, "SS_played":4, "SS_won":4, "SotA_played":4, "SotA_won":4, 
    #              "TP_played":4, "TP_won":4, "TP_flags_cap":4, "TP_flags_ret":4, "WS_played":4, "WS_won":4, "WS_flags_cap":4, "WS_flags_ret":4, "SM_played":4,  "SM_won":4,
    #              "TK_played":4, "TK_won":4, "IoC_played":4, "IoC_won":4, "DG_played":4, "DG_won":4})
    #bgdictlist.append({"Player_ID": 5, "BG_played": 8, "BG_won": 8, "played_most_n": 8, "played_most_c":8, "won_most_n":8, "won_most_c":8, "BG_played_c":8, "BG_won_c":8,
    #              "AV_played":8, "AV_won":8, "AV_tower_def":8, "AV_tower_cap":8, "AB_played":8, "AB_won":8, "Gil_played":8, "Gil_won":8, "EotS_played":8, "EotS_won":8,
    #              "EotS_flags":8, "EotS_played_est":8, "EotS_won_est":8, "EotS_played_def":8, "EotS_won_def":8, "SS_played":8, "SS_won":8, "SotA_played":8, "SotA_won":8, 
    #              "TP_played":8, "TP_won":8, "TP_flags_cap":8, "TP_flags_ret":8, "WS_played":8, "WS_won":8, "WS_flags_cap":8, "WS_flags_ret":8, "SM_played":8,  "SM_won":8,
    #              "TK_played":8, "TK_won":8, "IoC_played":8, "IoC_won":8, "DG_played":8, "DG_won":8})
    #bgdictlist.append({"Player_ID": 6, "BG_played": 9, "BG_won": 9, "played_most_n": 9, "played_most_c":9, "won_most_n":9, "won_most_c":9, "BG_played_c":9, "BG_won_c":9,
    #              "AV_played":9, "AV_won":9, "AV_tower_def":9, "AV_tower_cap":9, "AB_played":9, "AB_won":9, "Gil_played":9, "Gil_won":9, "EotS_played":9, "EotS_won":9,
    #              "EotS_flags":9, "EotS_played_est":9, "EotS_won_est":9, "EotS_played_def":9, "EotS_won_def":9, "SS_played":9, "SS_won":9, "SotA_played":9, "SotA_won":9, 
    #              "TP_played":9, "TP_won":9, "TP_flags_cap":9, "TP_flags_ret":9, "WS_played":9, "WS_won":9, "WS_flags_cap":9, "WS_flags_ret":9, "SM_played":9,  "SM_won":9,
    #              "TK_played":9, "TK_won":9, "IoC_played":9, "IoC_won":9, "DG_played":9, "DG_won":9})           
    #bgdictlist.append({"Player_ID": 7, "BG_played": 10, "BG_won": 10, "played_most_n": 10, "played_most_c":10, "won_most_n":10, "won_most_c":10, "BG_played_c":10, "BG_won_c":10,
    #              "AV_played":10, "AV_won":10, "AV_tower_def":10, "AV_tower_cap":10, "AB_played":10, "AB_won":10, "Gil_played":10, "Gil_won":10, "EotS_played":10, "EotS_won":10,
    #              "EotS_flags":10, "EotS_played_est":10, "EotS_won_est":10, "EotS_played_def":10, "EotS_won_def":10, "SS_played":10, "SS_won":10, "SotA_played":10, "SotA_won":10, 
    #              "TP_played":10, "TP_won":10, "TP_flags_cap":10, "TP_flags_ret":10, "WS_played":10, "WS_won":10, "WS_flags_cap":10, "WS_flags_ret":10, "SM_played":10,  "SM_won":10,
    #              "TK_played":10, "TK_won":10, "IoC_played":10, "IoC_won":10, "DG_played":10, "DG_won":10})       
    #bgdictlist.append({"Player_ID": 8, "BG_played": 11, "BG_won": 11, "played_most_n": 11, "played_most_c":11, "won_most_n":11, "won_most_c":11, "BG_played_c":11, "BG_won_c":11,
    #              "AV_played":11, "AV_won":11, "AV_tower_def":11, "AV_tower_cap":11, "AB_played":11, "AB_won":11, "Gil_played":11, "Gil_won":11, "EotS_played":11, "EotS_won":11,
    #              "EotS_flags":11, "EotS_played_est":11, "EotS_won_est":11, "EotS_played_def":11, "EotS_won_def":11, "SS_played":11, "SS_won":11, "SotA_played":11, "SotA_won":11, 
    #              "TP_played":11, "TP_won":11, "TP_flags_cap":11, "TP_flags_ret":11, "WS_played":11, "WS_won":11, "WS_flags_cap":11, "WS_flags_ret":11, "SM_played":11,  "SM_won":11,
    #              "TK_played":11, "TK_won":11, "IoC_played":11, "IoC_won":11, "DG_played":11, "DG_won":11})          
    #bulkWriteBG(bgdictlist)

    getBGServer(3, newest_only=True)

    print("done.")