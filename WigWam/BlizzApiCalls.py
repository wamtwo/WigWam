import os
import time
from warnings import simplefilter
simplefilter("ignore")
import glob
import codecs
import pandas as pd
import numpy as np
import pprint
import json
import requests
from sklearn.externals import joblib
from config import WWSettings
import infoRefinement as ir
import urllib, json 
from urllib.parse import quote, urlencode
import requests
import asyncio
import concurrent.futures
from timeit import default_timer
import databaseCalls as dbcalls



def getApiToken():
    data = {"grant_type":"client_credentials"}
    print(data)
    apiclient = os.environ.get("WOWAPICLIENT")
    apisecret = os.environ.get("WOWAPISECRET")
    #body = str.encode(json.dumps(data))

    BlizzardTokenUrl = "https://eu.battle.net/oauth/token"
    resp = requests.post(BlizzardTokenUrl, data=data, auth=(apiclient, apisecret))

    rdata = resp.json()
    print(resp.text)

    if "error" in rdata.keys(): return str(rdata["error"])
    else:
        tokDict = {"Token":rdata["access_token"],"Type":rdata["token_type"], "Expires":rdata["expires_in"]}

    joblib.dump(tokDict, "token.pkl")

    return "token requested"

def testpickle():
    tokDict = {"Token": "EUKNVle4Mc8LqjFEvRAqn186fZIdp2S094", "Type": "bla", "Expires": "bla"}
    joblib.dump(tokDict, "token.pkl")

def getStatistic(char,realm,region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "statistics"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getStatistic(char, realm, region)
    
    else:    
        return resp.json()

def getRatedPvP(char,realm,region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "pvp"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getRatedPvP(char, realm, region)
    
    else:    
        return resp.json()

def getAudit(char,realm,region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "audit"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getAudit(char, realm, region)
    
    else:    
        return resp.json()

def getQuests(char,realm,region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "quests"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getQuests(char, realm, region)
    
    else:    
        return resp.json()

def getMounts(char,realm,region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "mounts"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getMounts(char, realm, region)
    
    else:    
        return resp.json()

def getHonorableKills(char, realm, region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "statistics"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getHonorableKills(char, realm, region)
    
    else:
        rdict = resp.json()
        return rdict["statistics"]["subCategories"][2]["subCategories"][1]

def getKillingBlows(char, realm, region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "statistics"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getKillingBlows(char, realm, region)
    
    else:
        rdict = resp.json()
        return rdict["statistics"]["subCategories"][2]["subCategories"][2]


def getDeaths(char, realm, region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "statistics"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getDeaths(char, realm, region)
    
    else:
        rdict = resp.json()
        return rdict["statistics"]["subCategories"][3]


def getPvP(char, realm, region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "statistics"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getPvP(char, realm, region)
    
    else:
        rdict = resp.json()
        return rdict["statistics"]["subCategories"][9]

def getBGs(char, realm, region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + quote(char)
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "statistics"}
    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, params=data, headers=headers, timeout=10)
    

    if resp.status_code == 401:
        print(getApiToken())
        return getBGs(char, realm, region)

    elif resp.status_code == 500:
        print(f"Api response for {realm}, {char}: Internal Server Error.")
        return (0, "error", char, realm)
    
    else:
        try:
            rdict = resp.json()
        except Exception as exc:
            print(f"Error while decoding json. Exception: {exc}")
            return (0, "error", char, realm)
        else:
            if "status" in rdict: return (rdict, "error", char, realm)
            elif len(rdict) == 0: return (rdict, "error", char, realm)
            elif rdict["name"].lower() != char.lower(): 
                print("Charname mismatch! {} <-> {}".format(rdict["name"], char))
                return (rdict, "error", char, realm)
            elif rdict["realm"].lower() != realm.lower():
                print("Realm mismatch! {} <-> {}".format(rdict["realm"], realm))
                return (rdict, "error", char, realm)            
            rdict2 = rdict["statistics"]["subCategories"][9]["subCategories"][1]
            del rdict["statistics"]

            rdict["name"], rdict["realm"] = ir.refineCharandRealm(rdict["name"], rdict["realm"])

            return (rdict, rdict2)

def checkBGOrder(dict):
    checkList = []
    baseList = joblib.load("bgOrder.pkl")
    for d in dict["statistics"]:
        checkList.append(d["name"])
    match = True

    for index, entry in enumerate(checkList):
        if entry == baseList[index]:
            match = True
        else:
            match = False
            break

    if match == True: return True
    else: return False



def getRaces():
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/data/character/races"
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getRaces()
    
    else:
        rdict = resp.json()
        return rdict


def getClasses():
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/data/character/classes"
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getClasses()
    
    else:
        rdict = resp.json()
        return rdict


def getAuctions(realm, region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/auction/data/" + realm
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    headers = {"Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, headers=headers)

    if resp.status_code == 401:
        print(getApiToken())
        return getAuctions(realm, region)

    if resp.status_code == 500:
        print("Internal Server Error")
        return {}
    
    else:
        rdict = resp.json()
        return (rdict)


def getGuild(char,realm,region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/character/" + realm + "/" + char
    #BlizzApiUrl = "https://37.244.28.18/wow/character/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "guild"}
    headers = {"Host":"eu.api.blizzard.com", "Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, data, headers=headers, timeout=30)

    if resp.status_code == 401:
        print(getApiToken())
        return getGuild(char, realm, region)

    if resp.status_code == 500:
        print(f"Internal Server Error for {char} {realm}")
        return ("0","0")
    try:
        resp=resp.json()
    except Exception as exc:
        print(f"Error while decoding! \tException: {exc}")
        return ("0","0")

    if "guild" in resp:
        return(resp["guild"]["name"],resp["guild"]["realm"])
        
    else:
        return ("0","0")




def getMembers(char,realm,region):
    BlizzApiUrl = "https://eu.api.blizzard.com/wow/guild/" + realm + "/" + char
    #BlizzApiUrl = "https://37.244.28.18/wow/guild/" + realm + "/" + char
    tokDict = joblib.load("token.pkl")
    apiKey = tokDict["Token"]

    data = {"fields": "members"}
    headers = {"Host":"eu.api.blizzard.com", "Content-Type":"application/json", "Authorization": "Bearer "+ apiKey}
    resp = requests.get(BlizzApiUrl, data, headers=headers, timeout=30)

    if resp.status_code == 401:
        print(getApiToken())
        return getMembers(char, realm, region)

    if resp.status_code == 500:
        print(f"Internal Server error for {char}, {realm}")
        return None
    else:    
        rdict = resp.json()
        if "members" in rdict:
            rdict2=(rdict["members"])       
            return (rdict2)
        else:
            print(rdict)


def getAllChars(realm,region):
    start = time.time()
    print("Fetching data on ",realm,"-",region)
    testp = getAuctions(realm,region)
    if len(testp) == 0: return []
    ttt=(testp["files"][0]["url"])
    response = urllib.request.urlopen(ttt)
    data = json.loads(response.read())
    
    realmlist=[]
    charlist=[]
    komlist=[]
    guildlist=[]
   
    for item in data["auctions"]:
        realmlist.append(str(item["ownerRealm"]).lower())
        charlist.append(item["owner"])
  
    komlist=(list(zip(realmlist,charlist)))
    komlist2= list(set(map(tuple, komlist)))
    komlist2tmp=len(komlist2)
    
    print("\tFetching all chars in auctions")
    cou=1
    cou2=len(komlist2)
    exceptioncount = 0
    starttime = time.clock()
    with concurrent.futures.ThreadPoolExecutor (max_workers=WWSettings.workerz) as executor:

        tasks = (executor.submit(getGuild,komlist2[i][1],komlist2[i][0],region) for i in range (len(komlist2)))

        for f in concurrent.futures.as_completed(tasks, timeout=1200):
            print("\t" + str(cou) + "/" + str(cou2), end="")
            print("\r", end="")

            cou=cou+1
            try:
                t=f.result()
            except Exception as exc:
                print("Exception {}".format(exc))
                exceptioncount += 1
            else:
                if t[0]!="0":
                    guildlist.append(f.result())
    
    elapsed = (time.clock() - starttime)
    print("Exceptions: {}\t Elapsed: {:.2f} ".format(exceptioncount, elapsed))
 
    guildlist3=list(set(map(tuple,guildlist)))
    print("\t" + str(cou) + "/" + str(cou2), end="")
    print("  Done.")

    print("\tFetching all guild members")
    cou=1
    cou2=len(guildlist3)
    cou120=0
    with concurrent.futures.ThreadPoolExecutor(max_workers=WWSettings.workerz) as executor:

        tasks2 = (executor.submit(getMembers,guildlist3[i][0],guildlist3[i][1],region) for i in range(len(guildlist3)))

        for f2 in concurrent.futures.as_completed(tasks2):
            print("\t" + str(cou) + "/" + str(cou2), end="")
            print("\r", end="")
            cou=cou+1
            try:
                if f2.result() is None:
                    print("Guild not found")
                else:
                    t2=f2.result()
                    for j in range(len(t2)):
                        #print(t2[j]["character"]["name"])
                        if "realm" in t2[j]["character"]:
                            komlist2.append((str(t2[j]["character"]["realm"]).lower(),t2[j]["character"]["name"]))
                            if t2[j]["character"]["level"]==120:
                                cou120=cou120+1
            except Exception as exc:
                print("Exception {}".format(exc))
                exceptioncount += 1


    print("\t" + str(cou) + "/" + str(cou2), end="")
    print("  Done.")
    komlist3= list(set(map(tuple, komlist2)))

    print("\t\tAuctions: ",len(komlist))
    print("\t\tUnique Chars in Auctions: ",komlist2tmp)
    print("\t\tGuilds: ",len(guildlist))
    print("\t\tChars: ",len(komlist3))
    print("\t\tChars lvl 120 ",cou120)
    print("\telapsed time (seconds) ",time.time() - start)
    return komlist3


def getBulkBgs(charlist):
    resultlist = []

    starttime = time.clock()
    count = 1
    exceptioncount = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=WWSettings.workerz) as executor:
        
        task = (executor.submit(getBGs, charlist[i][1], charlist[i][0], "eu") for i in range(len(charlist)))

        for future in concurrent.futures.as_completed(task):
            print("\t" + str(count) + "/" + str(len(charlist)), end="")
            print("\r", end="")
            count += 1
            try:
                result = future.result()
            except Exception as exc:
                print("Exception {}".format(exc))
                exceptioncount += 1
            else:
                resultlist.append(result)
    
    elapsed = (time.clock() - starttime)
    print("Exceptions: {}\t Elapsed: {:.2f} ".format(exceptioncount, elapsed))

    return resultlist

def playerIDgetBGs(charinfo):
    result = getBGs(charinfo["Name"], charinfo["Server_Name"], "eu")
    if result[1] == "error":
        if charinfo["failed_attempts"] == None: charinfo["failed_attempts"] = 1
        else: charinfo["failed_attempts"] += 1
        return charinfo        #ADD LOGIC for failed BG calls from player_general here.
    else: 
        rdict1, rdict2 = result

        updated_info = ir.refineCharInfo(rdict1)
        charinfo["Class"] = updated_info["class"]
        charinfo["lvl"] = updated_info["level"]
        charinfo["Faction"] = updated_info["faction"]
        charinfo["Race"] = updated_info["race"]
        charinfo["failed_attempts"] = 0
        return charinfo


    return True


def getBulkBgsforGeneral(charlist):
    resultlist = []
    workerz = 6

    starttime = time.clock()
    count = 1
    exceptioncount = 0

#    result = playerIDgetBGs(charlist[0])
    with concurrent.futures.ThreadPoolExecutor(max_workers=workerz) as executor:
        
        task = (executor.submit(playerIDgetBGs, charinfo) for charinfo in charlist)

        for future in concurrent.futures.as_completed(task):
            print("\t" + str(count) + "/" + str(len(charlist)), end="")
            print("\r", end="")
            count += 1
            try:
                result = future.result()
            except Exception as exc:
                print("Exception {}".format(exc))
                exceptioncount += 1
            else:
                resultlist.append(result)
    
    elapsed = (time.clock() - starttime)
    print("Exceptions: {}\t Elapsed: {:.2f} ".format(exceptioncount, elapsed))

    return resultlist


if __name__ == "__main__":
        
   # serverlist=("Azshara","Antonidas","Blackmoore","Blackhand","Aegwynn","Thrall","Eredar","Dalvengyr","Frostmourne","Nazjatar","Zuluhed","Frostwolf","Alleria","Malfurion","Malygos","Arthas")


    server="Blackmoore"
    out=getAllChars(server,"eu")
    #print(dbcalls.writeCharNamesAtOnce("Server_" + server, out, verbosity=True, chunks=100))

    #print(getBGs("hyperzx", "aegwynn", "eu"))


    #print(dbcalls.getCharNames("Server_Malfurion"))
    #print(dbcalls.writeCharNames("Server_Malfurion", out))
    

    #while True:
    #    min=25
    #    out=getAllChars("Malfurion","eu")
    #    print(dbcalls.writeCharNamesAtOnce("Server_Malfurion", out))
    #    for i in range (0,min*60-1):
    #        print("Resuming in ",min*60-i," seconds with the next Server", end="")
    #        print("\r", end="")
    #        #print("Resuming in ",min*60-i," seconds with the next Server")
    #        time.sleep(1)
    #    print(" ")
    
    #min=5
    #for item in serverlist:
    #    out=getAllChars(item,"eu")
    #    for i in range (0,min-1):
    #        print("Resuming in ",min-i," Minutes with the next Server")
    #        time.sleep(60)


    #print(out)
    #print(len(out))
        
    #print(getApiToken())
    #testpickle()
    #print(json.dumps(getBGs("christhina","azshara","eu"),sort_keys=True, indent=4))
    #ir.createMoreBGCharts("christhina", "azshara", cardsdict)
    #check = checkBGOrder(testp)
    #print(check)
    #testdict = ir.refineBGInfo(testp)
    #print(testdict)
    #print(json.dumps(getClasses(), sort_keys=True, indent=4))


