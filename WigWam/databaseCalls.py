import os
import time
from sqlalchemy import create_engine, Table, select, MetaData, insert, delete
from sqlalchemy.exc import IntegrityError


username = os.environ.get("WIGWAMUSER")
password = os.environ.get("WIGWAMPW")
engine = create_engine("mssql+pyodbc://" + username + ":" + password + "@wigwam.database.windows.net/BzApiDB?driver=SQL+Server+Native+Client+11.0")

metadata = MetaData()
connection = engine.connect()

def writeCharNames(target_table, namelist):
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    countrejected = 0
    starttime = time.clock()
    print("Writing to Database Table \"" + target_table + "\"")

    with connection.begin() as trans:
        for index, tuple in enumerate(namelist):
            stmt = insert(table).values(Char_Name=tuple[0], Server_Name=tuple[1])
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


def writeCharNamesAtOnce(target_table, namelist):

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

    for tuple in namelist_cleaned:
        entries.append({"Char_Name":tuple[0], "Server_Name":tuple[1]})
    stmt = insert(table)


    if len(entries) > 0:
        try:
            engine.execute(stmt, entries)
        except:
            print("Exception occurred")

    elapsed = (time.clock() - starttime)
    #print("Done. \t --- \t %.2f seconds." %elapsed, end="\n\n")
    return "Done. {} Entries given. Rejections: {} \t --- \t {:.2f} seconds".format(len(namelist), countrejected, elapsed)

def getCharNames(target_table, amount=0):
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    stmt = select([table.columns.Char_Name, table.columns.Server_Name])
    if amount > 0: stmt = stmt.limit(amount)
    result = connection.execute(stmt).fetchall()

    return result


if __name__ == "__main__":
    #chrlst = [("Morty", "Server1"), ("Rick", "Server1"), ("Summer", "Server1"), ("Jerry", "Server1"), ("Beth", "Server1")]
    #chrlst = [("Winter", "Server1"), ("Spring", "Server1"), ("Summer", "Server2"), ("Autumn", "Server1"), ("Fall", "Server1"), ("Morty", "Server1"), ("Wubba", "Server1")]
    #chrlst = [("Winter", "Server1"), ("Spring", "Server2"), ("Summer", "Server3"), ("Autumn", "Server2"), ("Fall", "Server1"), ("Morty", "Server3"), ("Wubba", "Server1")]
    #print(writeCharNames("Server_Example", chrlst))

    #testresult = getCharNames("Server_Malfurion", 1000)
    #testresult_fixed = []
    #for tuple in testresult:
    #    tuple_new = (tuple[1], tuple[0])
    #    testresult_fixed.append(tuple_new)

    testresult_fixed = []
    for i in range(10000):
        testresult_fixed.append(("Char_" + str(i), "Server1"))


    table = Table("Server_Malfurion_copy", metadata, autoload=True, autoload_with=engine)   
    stmt = delete(table)
    connection.execute(stmt)
    print(writeCharNamesAtOnce("Server_Malfurion_copy", testresult_fixed))
    
    testset = []
    for i in range(20000):
        testset.append(("Char_" + str(i), "Server1"))
        
    #writeCharNames("Server_Malfurion_copy", [("Char_300", "Server1"), ("Char_600", "Server1"), ("Char_900", "Server1")])
    #print(writeCharNames("Server_Malfurion_copy", testset))
    #stmt = delete(table)
    #connection.execute(stmt)
    print(writeCharNamesAtOnce("Server_Malfurion_copy", testresult_fixed))
    print(writeCharNamesAtOnce("Server_Malfurion_copy", testset))

    print("done.")