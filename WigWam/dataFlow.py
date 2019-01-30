import databaseCalls as dbc
import BlizzApiCalls as bac
import infoRefinement as ir


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




if __name__ == "__main__":
    #print(scanServer(18))
    print(scanAllServers(exceptions={1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,19,20,21,22,23,24,25,26,28,29,30,31,32,33,34,35,36,38,39,41,42,43,44}))
    print("done.")