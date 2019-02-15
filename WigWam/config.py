import os

class Config(object):
    SECRET_KEY = os.environ.get("SECRET_KEY") or "123_Wam!"

class WWSettings():
    engineString = "mssql+pyodbc://" + os.environ.get("WIGWAMUSER") + ":" + os.environ.get("WIGWAMPW") + "@192.168.134.122/BzApiDB?driver=SQL+Server+Native+Client+11.0" 
    workerz = 6