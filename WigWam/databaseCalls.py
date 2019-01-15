import os
from sqlalchemy import create_engine, Table, select, MetaData, insert
from sqlalchemy.exc import IntegrityError


username = os.environ.get("WIGWAMUSER")
password = os.environ.get("WIGWAMPW")
engine = create_engine("mssql+pyodbc://" + username + ":" + password + "@wigwam.database.windows.net/BzApiDB?driver=SQL+Server+Native+Client+11.0")

metadata = MetaData()
connection = engine.connect()

def writeCharNames(target_table, namelist):
    table = Table(target_table, metadata, autoload=True, autoload_with=engine)
    countrejected = 0

    with connection.begin() as trans:
        for tuple in namelist:
            stmt = insert(table).values(Char_Name=tuple[0], Server_Name=tuple[1])
            try:
                engine.execute(stmt)
            except:
                trans.rollback()
                countrejected += 1
                #raise
        
    return "{} entries given. {} entries rejected.".format(len(namelist), countrejected)



if __name__ == "__main__":
    #chrlst = [("Morty", "Server1"), ("Rick", "Server1"), ("Summer", "Server1"), ("Jerry", "Server1"), ("Beth", "Server1")]
    #chrlst = [("Winter", "Server1"), ("Spring", "Server1"), ("Summer", "Server2"), ("Autumn", "Server1"), ("Fall", "Server1"), ("Morty", "Server1"), ("Wubba", "Server1")]
    chrlst = [("Winter", "Server1"), ("Spring", "Server2"), ("Summer", "Server3"), ("Autumn", "Server2"), ("Fall", "Server1"), ("Morty", "Server3"), ("Wubba", "Server1")]
    print(writeCharNames("Server_Example", chrlst))