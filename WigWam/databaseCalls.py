import os
import time
import numpy as np
from sqlalchemy import create_engine, Table, select, MetaData, insert, delete
from sqlalchemy.exc import IntegrityError


username = os.environ.get("WIGWAMUSER")
password = os.environ.get("WIGWAMPW")
#engine = create_engine("mssql+pyodbc://" + username + ":" + password + "@wigwam.database.windows.net/BzApiDB?driver=SQL+Server+Native+Client+11.0")
engine = create_engine("mssql+pyodbc://" + username + ":" + password + "@192.168.134.122/BzApiDB?driver=SQL+Server+Native+Client+11.0")

metadata = MetaData()
connection = engine.connect()

def writeCharNames(target_table, namelist): #writing every single entry with commit into DB. Low performance, high error tolerance.
    table = Table(target_table, metadata, autoload=True, autoload_with=engine2)
    countrejected = 0
    starttime = time.clock()
    print("Writing to Database Table \"" + target_table + "\"")

    with connection.begin() as trans:
        for index, tuple in enumerate(namelist):
            stmt = insert(table).values(Char_Name=tuple[1], Server_Name=tuple[0])
            try:
                #engine.execute(stmt)
                connection.execution_options(autocommit=False).execute(stmt)
            except:
                #trans.rollback()
                countrejected += 1
                #raise
            print(str(index) + "/" + str(len(namelist)), end="")
            print("\r", end="")
    
    elapsed = (time.clock() - starttime)
    print("Done. \t --- \t %.2f seconds." %elapsed, end="\n\n")
    return "{} entries given. {} entries rejected.".format(len(namelist), countrejected)


def writeCharNamesAtOnce(target_table, namelist, verbosity=False, chunks=10): # writing all entries at once. Higher performance, low/no error tolerance
    if chunks > 1000 or chunks < 1: return "Chunksize error: Chunksize can only be between 1 and 1000"
    print("Writing to Database Table \"" + target_table + "\"")

    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    entries = []
    countrejected = 0
    starttime = time.clock()

    comparedata = getCharNames(target_table)
    compareset = set()

    for row in comparedata:
        compareset.add((row[0], row[1]))
    
    namelist_cleaned = []
    for tuple in namelist:
        if tuple not in compareset: namelist_cleaned.append(tuple)
        else: countrejected += 1
  
    if verbosity == True and len(namelist_cleaned) >= chunks:
        splitlist = np.array_split(namelist_cleaned,chunks)

        i = 0
        for list in splitlist:
            for tuple in list:
                entries.append({"Char_Name":tuple[1], "Server_Name":tuple[0]})
            stmt = insert(table)

            if len(entries) > 0:
                try:
                    engine.execute(stmt, entries)
                except:
                    print("Exception occurred")
                    raise
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
                engine.execute(stmt, entries)
            except:
                print("Exception occurred")
                raise
            print("Completed!")

    elapsed = (time.clock() - starttime)
    #print("Done. \t --- \t %.2f seconds." %elapsed, end="\n\n")
    connection.close()
    return "Done. {} Entries given. Rejections: {} \t --- \t {:.2f} seconds".format(len(namelist), countrejected, elapsed)




def getCharNames(target_table, amount=0):
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    stmt = select([table.columns.Server_Name, table.columns.Char_Name])
    if amount > 0: stmt = stmt.limit(amount)
    result = connection.execute(stmt).fetchall()
    resultlist = []
    for row in result:
        resultlist.append((row[0], row[1]))

    return resultlist


if __name__ == "__main__":

    testresult = getCharNames("Server_Blackmoore", 0)

    table = Table("Server_Blackmoore", metadata, autoload=True, autoload_with=engine)   

    print("Writing to LAN DB")
    print(writeCharNamesAtOnce("Server_Blackmoore", testresult, verbosity=True, chunks=100))

    #testset = []
    #for i in range(20000):
    #    testset.append(("Char_" + str(i), "Server1"))
        
    #writeCharNames("Server_Malfurion_copy", [("Char_300", "Server1"), ("Char_600", "Server1"), ("Char_900", "Server1")])
    #print(writeCharNames("Server_Malfurion_copy", testset))
    #stmt = delete(table)
    #connection.execute(stmt)


    print("done.")