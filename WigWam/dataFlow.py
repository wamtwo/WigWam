import databaseCalls as dbc
import BlizzApiCalls as bac
import infoRefinement as ir
import sys
import time


def scanServer(id):  #fetches charname and realm for the selected server via the auction api (getAllChars) and writes them in to the corresponding staging tabe
    print("Fetching Data for Server ID {}".format(id))
    serverinfo = dbc.getServerbyID(id)
    if serverinfo[0] == "error": return serverinfo[1]
    if serverinfo[2] == True: return f"Server {serverinfo[0]} is set to \"Skip\"."
    server = serverinfo[0]
    server_realms = dbc.getConnectedRealms(id)
    print("Server {}, last scanned on: {}".format(serverinfo[0], serverinfo[1]))

    charlist_raw = bac.getAllChars(server,"eu")
    charlist = [(row[0], row[1]) for row in charlist_raw if row[0].lower() in server_realms]

    #for verbosity:
    print(f"{len(charlist_raw) - len(charlist)} entries don't match selected Server" )
    if len(charlist_raw) - len(charlist) > 0:
        print([chartuple for chartuple in charlist_raw if chartuple[0].lower() not in server_realms])

    print(dbc.writeCharNamesAtOnce("Server_" + server, charlist, verbosity=True, chunks=100))
    print(dbc.updateServerScanbyID(id))

    return "Server {} scanned successfully".format(server)

def scanAllServers(exceptions={}):  #fetches all server_ids and loops over all of them with scanServer(id)
    serverIDs = dbc.getNumberofServers()
    servercount = 0
    for id in serverIDs:
        if id in exceptions: print(f"Skipping Server ID {id}")
        else:
            print(scanServer(id))
            print("\n\n ############## \n\n")
            servercount += 1
    return "Done. Scanned {} Servers.".format(servercount)

def transferCharbyServerID(id, chunksize=100): #transfers a chunk of entries from a staging table to the main db. -> use bulkTransferCharbyServerId
    serverinfo = dbc.getServerbyID(id)
    if serverinfo[0] == "error": return serverinfo[1]
    if serverinfo[2] == True: return f"Server {serverinfo[0]} is set to \"Skip\"."
    print(f"Searching for untransferred Chars. Chunksize set to {chunksize}")
    server = serverinfo[0]

    charlist = dbc.getUntransferredChars("Server_"+server, chunksize=chunksize)
    if len(charlist) == 0: return "No untransferred Chars found"
    print(f"Found {len(charlist)} untransferred Chars")

    index = 1

    for char in charlist:
        print(f"fetching Char Info {index}/{len(charlist)} -> {char[1]:12}", end="\r")
        index += 1
        infodict = bac.getBGs(char[1], char[0], "eu")[0]

        if "status" in infodict:
           print("{}: {}".format(infodict["reason"], char[1]), end="\n")
           print("Removed Char from Table {}: {}".format("Server_"+server, dbc.removeCharfromTable("Server_"+server, char)))
        else:
            infodict = ir.refineCharInfo(infodict)
            if dbc.transferChartogeneral(infodict["name"], infodict["realm"], id, infodict["class"], infodict["level"], infodict["faction"], infodict["race"]) == True:
                if dbc.setCharasTransferred("Server_"+server, char) == True:
                    continue
                else: print("Error while setting Char to transferred. Char removed from player_general: {}".format(dbc.removeCharfromGeneral(char[1], char[0])))
    print("")
    return "Done."


def bulktransferCharbyServerID(id, chunksize=100, fail_thresh=1): # transfers a chunk of entries from a staging table into the main table.
    serverinfo = dbc.getServerbyID(id)                          #returns the name of the server from the serverlist
    if serverinfo[0] == "error": return (serverinfo[1], 0)
    if serverinfo[2] == True: return (f"Server {serverinfo[0]} is set to \"Skip\".", 0)
    print(f"Searching for untransferred Chars. Chunksize set to {chunksize}, Failed Attempts Threshold is set to {fail_thresh}")
    server = serverinfo[0]

    charlist = dbc.getUntransferredChars("Server_"+server, chunksize=chunksize, fail_thresh=fail_thresh)  #fetches all untransferred chars from the staging table
    if len(charlist) == 0: return ("No untransferred Chars found", 0)
    print(f"Found {len(charlist)} untransferred Chars.. fetching Char Information:")

    resultlist = bac.getBulkBgs(charlist)                       #fetches Char Information from the blizzard api for all chars fetched from the staging table

    goodlist, badlist = splitResultlist(resultlist)             #splits api responses into "good" and "bad" (charname mismatch, realm mismatch, server error, etc...)
    infodictlist = []
    
    for infolist in goodlist:
        infodictlist.append(ir.refineCharInfo(infolist))        #refines the char information for entries in the goodlist

    if len(infodictlist) > 0:                                   #writes all chars from the goodlist with their api results into the main table
        print("Transferring Char Information to DB...")
        if dbc.bulktransferChartoGeneral(infodictlist, id) == True:
            print("Chunk written to player_general successfully. Written {} entries.".format(len(infodictlist)), end="\n\n")
        else:
            print("Error while writing to player_general.")
            return ("Error!",0)

    if len(badlist) > 0:                                        #increses failed_attempts for the entries from the badlist in the staging table
        failcount = 0
        print("Updating failed Attempts for bad API responses")
        for index, entry in enumerate(badlist):
            print("Updating entry {}/{}".format(index+1, len(badlist)), end="")
            print("\r", end="")
            if dbc.increaseFailCount("Server_"+server, (entry[3], entry[2])) == True: failcount += 1
            else:
                print("Error while incresing failed attempts {} from Server_{}".format(entry[2],server))
        print("\nUpdated failed Attempts for {} entries".format(failcount), end="\n\n")
#    if len(badlist) > 0:
#        delcount = 0
#        print("Removing bad entries from Server_{}".format(server))
#        for index, entry in enumerate(badlist):
#            print("Updating entry {}/{}".format(index+1, len(badlist)), end="")
#            print("\r", end="")
#            if dbc.removeCharfromTable("Server_"+server, (entry[3], entry[2])) == True: delcount += 1
#            else:
#                print("Error while deleting {} from Server_{}".format(entry[2],server))
#        print("\nDeleted {} entries".format(delcount), end="\n\n")

    print("\nMarking Chars as transferred in Server_{}".format(server))
    transresult = bulkMarkAsTransferred(infodictlist, server)   #marks the successfully transferred chars (goodlist) as transferred in the staging table
    print(transresult[0])



    return ("Done. Written {} entries into player_general, updated {} and increased Failed Attempts on {} entries from Server_{}".format(len(goodlist), transresult[1], len(badlist), server), len(charlist))

def bulkMarkAsTransferred(infodictlist, server): #marks a list of chars as transferred in a staging table
    if len(infodictlist) < 1: return ("List is empty", 0)
    tuplelist = [(entry["realm"], entry["name"]) for entry in infodictlist]


    errorcount = dbc.bulkSetCharasTransferred("Server_"+server, tuplelist)
#    for index, entry in enumerate(infodictlist):
#        print("Updating entry {}/{}".format(index+1, len(infodictlist)), end="")
#        print("\r", end="")
#        if dbc.setCharasTransferred("Server_"+server, (entry["realm"], entry["name"])) == True: continue
#        else:
#            print("Error marking {}, {} as transferred. Removing from player_general".format(entry["realm"], entry["name"]))
#            print("Removed: {}".format(dbc.removeCharfromGeneral(entry["name"], entry["realm"])))
#            errorcount += 1

    return ("{} entries given. {} errors while updating.".format(len(tuplelist), errorcount), len(tuplelist)-errorcount)


def transferAllfromServer(id, chunksize=500, fail_thresh=1): #transfers all chars (chunk by chunk) from a staging table to the main table by looping over bulkTransferCharbyServerID()
    print(f"Attempting to transfer all Chars from Server ID {id}. Chunksize set to {chunksize}")
    serverinfo = dbc.getServerbyID(id)
    if serverinfo[0] == "error": return f"{serverinfo[1]}"
    if serverinfo[2] == True: return f"Server {serverinfo[0]} is set to \"Skip\"."
    all, untrans = dbc.getCountsServerTable("Server_"+serverinfo[0]) #fetches all chars + untransferred chars from a staging table
    print(f"{serverinfo[0]} - Total {all:7d} \t Untransferred {untrans:7d}")
    iterationestimate = untrans // chunksize +1                     #estimates iterations
    print(f"Estimated Iterrations: {iterationestimate}")
    starttime = time.clock()
    iterations = 1
    while True:                                     #loops over bulktransferCharbyServerID the number of untransferred chars is smaller than the specified chunk size
        print("\n\n ################################")
        print(f"\nStarting Iteration {iterations} of {iterationestimate} estimated iterrations.")
        result = bulktransferCharbyServerID(id, chunksize, fail_thresh=fail_thresh)
        print(result[0])
        if result[1] < chunksize: break
        iterations += 1
    elapsed = (time.clock() - starttime)
    return "Done in {} iterations. Elapsed: {:.2f} seconds".format(iterations, elapsed)



def splitResultlist(resultlist):    #splits into goodlist and badlist for bulktransferCharbyServerID()
    if len(resultlist) < 1: return ("error", "error")

    goodlist = []
    badlist = []

    for result in resultlist:
        if result[1] == "error":
            badlist.append(result)
        else:
            goodlist.append(result[0])

    return (goodlist, badlist)


def transferAllfromAllServers(exceptions={}):   #calls transferAllfromServer() for every server in serverlist db
    serverIDs = dbc.getNumberofServers()
    servercount = 0

    for id in serverIDs:
        if id in exceptions: print(f"Skipping Server ID {id}")
        else:
            print(f"ServerID {id} von {len(serverIDs)}")
            print(transferAllfromServer(id,1000))
            print("\n\n ############## \n\n")
            servercount += 1
    return "Done. Scanned {} Servers.".format(servercount)

def printAllServerCounts():                     #prints total entries + untransferred entries for all servers in serverlist db.
    serverIDs = dbc.getNumberofServers()
    print("{:32} Total \t Untransferred".format("Server"))
    for id in serverIDs:
        serverinfo = dbc.getServerbyID(id)
        if serverinfo[0] == "error": print(serverinfo[1])
        if serverinfo[2] == True: print(f"Server {serverinfo[0]} is set to \"Skip\".", 0)
        else:
            all, untrans = dbc.getCountsServerTable("Server_"+serverinfo[0])
            print(f"{serverinfo[0]:30} {all:7d} \t {untrans:7d}")


def scanfromGeneralbyServerID(id, lvl=120, chunksize=1000):
    serverinfo = dbc.getServerbyID(id)                          #returns the name of the server from the serverlist
    if serverinfo[0] == "error": return (serverinfo[1], 0)
    print(f"Scanning Chars from player_general. Chunksize set to {chunksize}")
    server = serverinfo[0]

    charlist = dbc.getCharsfromGeneral(id, lvl, 1000, days=1)

    charlist_updated = bac.getBulkBgsforGeneral(charlist)

    print("Updating entries")
    errorcount = dbc.bulkUpdateCharsGeneral(charlist_updated)
    print(f"Updating complete with {errorcount} errors.")



    return True


if __name__ == "__main__":
    #print(scanServer(16))
    #print(scanServer(39))
    #print(scanAllServers())

    #print("getting chars")
    #transferCharbyServerID(14, chunksize=100)

    #print(dbc.removeCharfromGeneral("Abarta", 14))

    #testblabla = bac.getBGs("Abbam", "Malfurion", "eu")

    #print(bulktransferCharbyServerID(2, chunksize=1000, fail_thresh=1))



    #bac.getAllChars("Antonidas", "eu")

    #print(transferAllfromServer(3, 1000, fail_thresh=1))

    #printAllServerCounts()

    scanfromGeneralbyServerID(3)

    print("done.")