import databaseCalls as dbc
import BlizzApiCalls as bac
import infoRefinement as ir
import sys


def scanServer(id):
    print("Fetching Data for Server ID {}".format(id))
    serverinfo = dbc.getServerbyID(id)
    if serverinfo[0] == "error": return serverinfo[1]
    if serverinfo[2] == True: return f"Server {serverinfo[0]} is set to \"Skip\"."
    server = serverinfo[0]
    print("Server {}, last scanned on: {}".format(serverinfo[0], serverinfo[1]))

    charlist = bac.getAllChars(server,"eu")
    print(dbc.writeCharNamesAtOnce("Server_" + server, charlist, verbosity=True, chunks=100))
    print(dbc.updateServerScanbyID(id))

    return "Server {} scanned successfully".format(server)

def scanAllServers(exceptions={}):
    serverIDs = dbc.getNumberofServers()
    servercount = 0
    for id in serverIDs:
        if id in exceptions: print(f"Skipping Server ID {id}")
        else:
            print(scanServer(id))
            print("\n\n ############## \n\n")
            servercount += 1
    return "Done. Scanned {} Servers.".format(servercount)

def transferCharbyServerID(id, chunksize=100):
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
            if dbc.transferChartogeneral(infodict["name"], id, infodict["class"], infodict["level"], infodict["faction"], infodict["race"]) == True:
                if dbc.setCharasTransferred("Server_"+server, char) == True:
                    continue
                else: print("Error while setting Char to transferred. Char removed from player_general: {}".format(dbc.removeCharfromGeneral(char[1], id)))
    print("")
    return "Done."



if __name__ == "__main__":
    #print(scanServer(18))
    #print(scanAllServers())

    #print("getting chars")
    transferCharbyServerID(14, chunksize=100)

    #print(dbc.removeCharfromGeneral("Abarta", 14))

    #testblabla = bac.getBGs("Abbam", "Malfurion", "eu")

    print("done.")