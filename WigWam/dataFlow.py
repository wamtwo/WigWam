import databaseCalls as dbc
import BlizzApiCalls as bac
import infoRefinement as ir


def scanServer(id):
    print("Fetching Data for Server ID {}".format(id))
    serverinfo = dbc.getServerbyID(id)
    if serverinfo[0] == "error": return serverinfo[1]
    server = serverinfo[0]
    print("Server {}, last scanned on: {}".format(serverinfo[0], serverinfo[1]))

    charlist = bac.getAllChars(server,"eu")
    print(dbc.writeCharNamesAtOnce("Server_" + server, charlist, verbosity=True, chunks=100))
    print(dbc.updateServerScanbyID(id))

    return "Server {} scanned successfully".format(server)

def scanAllServers():
    serverIDs = dbc.getNumberofServers()
    servercount = 0
    for id in serverIDs:
        print(scanServer(id))
        print("\n\n ############## \n\n")
        servercount += 1
    return "Done. Scanned {} Servers.".format(servercount)




if __name__ == "__main__":
    print(scanAllServers())
    print("done.")