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
                engine.execute(stmt)
            except:
                trans.rollback()
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
    starttime = time.clock()
    for tuple in namelist:
        entries.append({"Char_Name":tuple[0], "Server_Name":tuple[1]})
    stmt = insert(table)
    try:
        engine.execute(stmt, entries)
    except:
        connection.commit()
    elapsed = (time.clock() - starttime)
    print("Done. \t --- \t %.2f seconds." %elapsed, end="\n\n")
    return("Done.")

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

    testresult = getCharNames("Server_Malfurion", 1000)
    testresult_fixed = []
    for tuple in testresult:
        tuple_new = (tuple[1], tuple[0])
        testresult_fixed.append(tuple_new)

    table = Table("Server_Malfurion_copy", metadata, autoload=True, autoload_with=engine)   
    #stmt = delete(table)
    #print(writeCharNames("Server_Malfurion_copy", testresult_fixed))
    stmt = delete(table)
    connection.execute(stmt)
    writeCharNames("Server_Malfurion_copy", [("Alfar", "Malfurion")])
    writeCharNamesAtOnce("Server_Malfurion_copy", testresult_fixed)

    print("done.")