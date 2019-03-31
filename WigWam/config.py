import os

class Config(object):
    SECRET_KEY = os.environ.get("SECRET_KEY") or "123_Wam!"

class WWSettings():
    engineString = "mssql+pyodbc://" + os.environ.get("WIGWAMUSER") + ":" + os.environ.get("WIGWAMPW") + "@127.0.0.1/BzApiDB?driver=SQL+Server+Native+Client+11.0" 
    workerz = 50