import os
import time
from warnings import simplefilter
simplefilter("ignore")
import glob
import codecs
import pandas as pd
import numpy as np
import requests
import matplotlib.pyplot as plt
import json
import requests
from sklearn.externals import joblib
from matplotlib.gridspec import GridSpec



def refineBGInfo(bgdict): 
    #reorganizes api bg data
    if bgdict["name"] != "Battlegrounds": return "Error, incorrect Data or Data Format received (refineBGInfo)"
    else:
        simpledict = {}   
        for item in bgdict["statistics"]: #putting relevant information into simple 1 dimensional dict
            if "highest" in item.keys(): simpledict[item["name"]] = (item["highest"], str(item["quantity"]))
            else: simpledict[item["name"]] = item["quantity"]
        
        calctuple = calcStats(simpledict) # calculates battles/victories without EotS and estimates EotS stats
        simpledict["Battlegrounds played (calculated)"], simpledict["Battlegrounds won (calculated)"] = calctuple[0]
        simpledict["Eye of the Storm battles (estimated)"], simpledict["Eye of the Storm victories (estimated)"] = calctuple[1]
        simpledict["Eye of the Storm battles deficit"], simpledict["Eye of the Storm victories deficit"] = calctuple[2]

        if simpledict["Battleground played the most"] == 0: simpledict["Battleground played the most"] = ("None", "0")
        if simpledict["Battleground won the most"] == 0: simpledict["Battleground won the most"] = ("None", "0")

        cardsdict = {}
        #reorganizing simpledict into dict with blocks of associated data ("cards")
        cardsdict["General"] = {"Battlegrounds played": simpledict["Battlegrounds played"],
                                "Battlegrounds won": simpledict["Battlegrounds won"],
                                "Battleground played the most": simpledict["Battleground played the most"][0] + " (" + simpledict["Battleground played the most"][1] + ")",
                                "Battleground won the most": simpledict["Battleground won the most"][0] + " (" + simpledict ["Battleground won the most"][1] + ")",
                                "Battlegrounds played (calculated)": simpledict["Battlegrounds played (calculated)"],
                                "Battlegrounds won (calculated)": simpledict["Battlegrounds won (calculated)"]}


        cardsdict["Alterac Valley"] = {"Alterac Valley battles": simpledict["Alterac Valley battles"], 
                                       "Alterac Valley victories": simpledict["Alterac Valley victories"],
                                       "Alterac Valley towers defended": simpledict["Alterac Valley towers defended"],
                                       "Alterac Valley towers captured": simpledict["Alterac Valley towers captured"]}


        cardsdict["Arathi Basin"] = {"Arathi Basin battles": simpledict["Arathi Basin battles"],
                                     "Arathi Basin victories": simpledict["Arathi Basin victories"]}


        cardsdict["Battle for Gilneas"] = {"Battle for Gilneas battles": simpledict["Battle for Gilneas battles"],
                                           "Battle for Gilneas victories": simpledict["Battle for Gilneas victories"]}

        cardsdict["Eye of the Storm"] = {"Eye of the Storm battles": simpledict["Eye of the Storm battles"],
                                         "Eye of the Storm victories": simpledict["Eye of the Storm victories"],
                                         "Eye of the Storm flags captured": simpledict["Eye of the Storm flags captured"],
                                         "Eye of the Storm battles (estimated)": simpledict["Eye of the Storm battles (estimated)"],
                                         "Eye of the Storm victories (estimated)": simpledict["Eye of the Storm victories (estimated)"],
                                         "Eye of the Storm battles deficit": simpledict["Eye of the Storm battles deficit"],
                                         "Eye of the Storm victories deficit": simpledict["Eye of the Storm victories deficit"]}

        cardsdict["Seething Shore"] = {"Seething Shore battles": simpledict["Seething Shore battles"],
                                       "Seething Shore victories": simpledict["Seething Shore victories"]}

        cardsdict["Strand of the Ancients"] = {"Strand of the Ancients battles": simpledict["Strand of the Ancients battles"],
                                               "Strand of the Ancients victories": simpledict["Strand of the Ancients victories"]}

        cardsdict["Twin Peaks"] = {"Twin Peaks battles": simpledict["Twin Peaks battles"],
                                   "Twin Peaks victories": simpledict["Twin Peaks victories"],
                                   "Twin Peaks flags captured": simpledict["Twin Peaks flags captured"],
                                   "Twin Peaks flags returned": simpledict["Twin Peaks flags returned"]}

        cardsdict["Warsong Gulch"] = {"Warsong Gulch battles": simpledict["Warsong Gulch battles"],
                                      "Warsong Gulch victories": simpledict["Warsong Gulch victories"],
                                      "Warsong Gulch flags captured": simpledict["Warsong Gulch flags captured"],
                                      "Warsong Gulch flags returned": simpledict["Warsong Gulch flags returned"]}

        cardsdict["Silvershard Mines"] = {"Silvershard Mines battles": simpledict["Silvershard Mines battles"],
                                          "Silvershard Mines victories": simpledict["Silvershard Mines victories"]}

        cardsdict["Temple of Kotmogu"] = {"Temple of Kotmogu battles": simpledict["Temple of Kotmogu battles"],
                                          "Temple of Kotmogu victories": simpledict["Temple of Kotmogu victories"]}

        cardsdict["Isle of Conquest"] = {"Isle of Conquest battles": simpledict["Isle of Conquest battles"],
                                         "Isle of Conquest victories": simpledict["Isle of Conquest victories"]}

        cardsdict["Deepwind Gorge"] = {"Deepwind Gorge battles": simpledict["Deepwind Gorge battles"],
                                       "Deepwind Gorge victories": simpledict["Deepwind Gorge victories"]}


        return cardsdict


def refineBGInfoforDB(bgdict): 
    #reorganizes api bg data
    if bgdict["name"] != "Battlegrounds": return "Error, incorrect Data or Data Format received (refineBGInfo)"
    else:
        simpledict = {}   
        for item in bgdict["statistics"]: #putting relevant information into simple 1 dimensional dict
            if "highest" in item.keys(): simpledict[item["name"]] = (item["highest"], str(item["quantity"]))
            else: simpledict[item["name"]] = item["quantity"]
        
        calctuple = calcStats(simpledict) # calculates battles/victories without EotS and estimates EotS stats
        simpledict["Battlegrounds played (calculated)"], simpledict["Battlegrounds won (calculated)"] = calctuple[0]
        simpledict["Eye of the Storm battles (estimated)"], simpledict["Eye of the Storm victories (estimated)"] = calctuple[1]
        simpledict["Eye of the Storm battles deficit"], simpledict["Eye of the Storm victories deficit"] = calctuple[2]

        if simpledict["Battleground played the most"] == 0: simpledict["Battleground played the most"] = ("None", 0)
        if simpledict["Battleground won the most"] == 0: simpledict["Battleground won the most"] = ("None", 0)

        dbdict = {"BG_played": simpledict["Battlegrounds played"], "BG_won": simpledict["Battlegrounds won"],
                  "played_most_n": simpledict["Battleground played the most"][0], "played_most_c":simpledict["Battleground played the most"][1],
                  "won_most_n":simpledict["Battleground won the most"][0], "won_most_c":simpledict["Battleground won the most"][1],
                  "BG_played_c": simpledict["Battlegrounds played (calculated)"], "BG_won_c": simpledict["Battlegrounds won (calculated)"],
                  "AV_played": simpledict["Alterac Valley battles"], "AV_won": simpledict["Alterac Valley victories"],
                  "AV_tower_def": simpledict["Alterac Valley towers defended"], "AV_tower_cap": simpledict["Alterac Valley towers captured"],
                  "AB_played": simpledict["Arathi Basin battles"], "AB_won": simpledict["Arathi Basin victories"],
                  "Gil_played": simpledict["Battle for Gilneas battles"], "Gil_won": simpledict["Battle for Gilneas victories"],
                  "EotS_played": simpledict["Eye of the Storm battles"], "EotS_won": simpledict["Eye of the Storm victories"],
                  "EotS_flags": simpledict["Eye of the Storm flags captured"], "EotS_played_est": simpledict["Eye of the Storm battles (estimated)"],
                  "EotS_won_est": simpledict["Eye of the Storm victories (estimated)"], "EotS_played_def": simpledict["Eye of the Storm battles deficit"], 
                  "EotS_won_def": simpledict["Eye of the Storm victories deficit"], 
                  "SS_played": simpledict["Seething Shore battles"], "SS_won": simpledict["Seething Shore victories"], 
                  "SotA_played": simpledict["Strand of the Ancients battles"], "SotA_won": simpledict["Strand of the Ancients victories"], 
                  "TP_played": simpledict["Twin Peaks battles"], "TP_won": simpledict["Twin Peaks victories"], 
                  "TP_flags_cap": simpledict["Twin Peaks flags captured"], "TP_flags_ret": simpledict["Twin Peaks flags returned"], 
                  "WS_played": simpledict["Warsong Gulch battles"], "WS_won": simpledict["Warsong Gulch victories"],
                  "WS_flags_cap": simpledict["Warsong Gulch flags captured"], "WS_flags_ret": simpledict["Warsong Gulch flags returned"], 
                  "SM_played": simpledict["Silvershard Mines battles"], "SM_won": simpledict["Silvershard Mines victories"],
                  "TK_played": simpledict["Temple of Kotmogu battles"], "TK_won": simpledict["Temple of Kotmogu victories"], 
                  "IoC_played": simpledict["Isle of Conquest battles"], "IoC_won": simpledict["Isle of Conquest victories"], 
                  "DG_played": simpledict["Deepwind Gorge battles"], "DG_won": simpledict["Deepwind Gorge victories"]}

        return dbdict





def createBGCharts(char, realm, cardsdict): #deprecated test function, use createMoreBGCharts(char, realm, cardsdict) instead
    filename = "figures/" + char + "_" + realm + ".jpg"
    totalGames = cardsdict["General"]["Battlegrounds played"]
    totalWins = cardsdict["General"]["Battlegrounds won"]
    colors = ["lightgreen", "lightcoral"]
    sizes = [totalWins, totalGames - totalWins]
    explode = (0.0,0)

    fig = plt.figure()
    plt.pie(sizes, shadow=False, autopct=lambda p: "{:.2f}% ({:,.0f})".format(p,p * sum(sizes)/100), startangle=45, colors=colors, counterclock=False)
    plt.axis("equal")
    plt.title("Total Battlegrounds")
    plt.legend(["Win","Loss"])
    fig.savefig("static/" + filename)
    plt.close(fig)

    #filepath = os.path.abspath(filename)

    return filename



def createMoreBGCharts(char, realm, cardsdict):
    #creates pie chars from bg data (except eye of the storm -> faulty data from api)
    filename = "figures/" + char + "_" + realm + "_.svg"
    totalGames = cardsdict["General"]["Battlegrounds played"]
    totalWins = cardsdict["General"]["Battlegrounds won"]
    colors = ["lightgreen", "lightcoral"]
    sizes = [totalWins, totalGames - totalWins]
    explode = (0.0,0)

    fig = plt.figure(figsize=(7,15), frameon=False)
    gs1 = GridSpec(6,3)

    plt.subplot(gs1[0:2, 0:3])
    if sizes[1] + sizes[0] == 0: plt.pie([1], shadow=False, startangle=45, colors=["grey"], counterclock=False)
    else: plt.pie(sizes, shadow=False, autopct=lambda p: "{:.2f}% ({:,.0f})".format(p,p * sum(sizes)/100), startangle=45, colors=colors, counterclock=False )
    plt.axis("equal")
    plt.title("Total Battlegrounds", y=1, fontsize=20)
    plt.legend(["Win","Loss"])

    plt.subplot(gs1[2, 0])
    sizes = [cardsdict["Alterac Valley"]["Alterac Valley victories"],cardsdict["Alterac Valley"]["Alterac Valley battles"] - cardsdict["Alterac Valley"]["Alterac Valley victories"]]
    if sizes[1] + sizes[0] == 0: plt.pie([1], shadow=False, startangle=45, colors=["grey"], counterclock=False )
    plt.pie(sizes, shadow=False, autopct=lambda p: "{:.2f}% ({:,.0f})".format(p,p * sum(sizes)/100), startangle=45, colors=colors, counterclock=False )
    plt.axis("equal")
    plt.title("Alterac Valley", y=0.95)

    plt.subplot(gs1[2, 1])
    sizes = [cardsdict["Arathi Basin"]["Arathi Basin victories"],cardsdict["Arathi Basin"]["Arathi Basin battles"] - cardsdict["Arathi Basin"]["Arathi Basin victories"]]
    if sizes[1] + sizes[0] == 0: plt.pie([1], shadow=False, startangle=45, colors=["grey"], counterclock=False)
    else: plt.pie(sizes, shadow=False, autopct=lambda p: "{:.2f}% ({:,.0f})".format(p,p * sum(sizes)/100), startangle=45, colors=colors, counterclock=False )
    plt.axis("equal")
    plt.title("Arathi Basin", y=0.95)

    plt.subplot(gs1[2,2])
    sizes = [cardsdict["Battle for Gilneas"]["Battle for Gilneas victories"],cardsdict["Battle for Gilneas"]["Battle for Gilneas battles"] - cardsdict["Battle for Gilneas"]["Battle for Gilneas victories"]]
    if sizes[1] + sizes[0] == 0: plt.pie([1], shadow=False, startangle=45, colors=["grey"], counterclock=False)
    else: plt.pie(sizes, shadow=False, autopct=lambda p: "{:.2f}% ({:,.0f})".format(p,p * sum(sizes)/100), startangle=45, colors=colors, counterclock=False )
    plt.axis("equal")
    plt.title("Battle for Gilneas", y=0.95)

    plt.subplot(gs1[3,0])
    sizes = [cardsdict["Seething Shore"]["Seething Shore victories"],cardsdict["Seething Shore"]["Seething Shore battles"] - cardsdict["Seething Shore"]["Seething Shore victories"]]
    if sizes[1] + sizes[0] == 0: plt.pie([1], shadow=False, startangle=45, colors=["grey"], counterclock=False)
    else: plt.pie(sizes, shadow=False, autopct=lambda p: "{:.2f}% ({:,.0f})".format(p,p * sum(sizes)/100), startangle=45, colors=colors, counterclock=False )
    plt.axis("equal")
    plt.title("Seething Shore", y=0.95)

    plt.subplot(gs1[3,1])
    sizes = [cardsdict["Twin Peaks"]["Twin Peaks victories"],cardsdict["Twin Peaks"]["Twin Peaks battles"] - cardsdict["Twin Peaks"]["Twin Peaks victories"]]
    if sizes[1] + sizes[0] == 0: plt.pie([1], shadow=False, startangle=45, colors=["grey"], counterclock=False)
    else: plt.pie(sizes, shadow=False, autopct=lambda p: "{:.2f}% ({:,.0f})".format(p,p * sum(sizes)/100), startangle=45, colors=colors, counterclock=False )
    plt.axis("equal")
    plt.title("Twin Peaks", y=0.95)

    plt.subplot(gs1[3,2])
    sizes = [cardsdict["Warsong Gulch"]["Warsong Gulch victories"],cardsdict["Warsong Gulch"]["Warsong Gulch battles"] - cardsdict["Warsong Gulch"]["Warsong Gulch victories"]]
    if sizes[1] + sizes[0] == 0: plt.pie([1], shadow=False, startangle=45, colors=["grey"], counterclock=False)
    else: plt.pie(sizes, shadow=False, autopct=lambda p: "{:.2f}% ({:,.0f})".format(p,p * sum(sizes)/100), startangle=45, colors=colors, counterclock=False )
    plt.axis("equal")
    plt.title("Warsong Gulch", y=0.95)

    plt.subplot(gs1[4,0])
    sizes = [cardsdict["Silvershard Mines"]["Silvershard Mines victories"],cardsdict["Silvershard Mines"]["Silvershard Mines battles"] - cardsdict["Silvershard Mines"]["Silvershard Mines victories"]]
    if sizes[1] + sizes[0] == 0: plt.pie([1], shadow=False, startangle=45, colors=["grey"], counterclock=False)
    else: plt.pie(sizes, shadow=False, autopct=lambda p: "{:.2f}% ({:,.0f})".format(p,p * sum(sizes)/100), startangle=45, colors=colors, counterclock=False )
    plt.axis("equal")
    plt.title("Silvershard Mines", y=0.95)

    plt.subplot(gs1[4,1])
    sizes = [cardsdict["Temple of Kotmogu"]["Temple of Kotmogu victories"],cardsdict["Temple of Kotmogu"]["Temple of Kotmogu battles"] - cardsdict["Temple of Kotmogu"]["Temple of Kotmogu victories"]]
    if sizes[1] + sizes[0] == 0: plt.pie([1], shadow=False, startangle=45, colors=["grey"], counterclock=False)
    else: plt.pie(sizes, shadow=False, autopct=lambda p: "{:.2f}% ({:,.0f})".format(p,p * sum(sizes)/100), startangle=45, colors=colors, counterclock=False )
    plt.axis("equal")
    plt.title("Temple of Kotmogu", y=0.95)

    plt.subplot(gs1[4,2])
    sizes = [cardsdict["Isle of Conquest"]["Isle of Conquest victories"],cardsdict["Isle of Conquest"]["Isle of Conquest battles"] - cardsdict["Isle of Conquest"]["Isle of Conquest victories"]]
    if sizes[1] + sizes[0] == 0: plt.pie([1], shadow=False, startangle=45, colors=["grey"], counterclock=False)
    else: plt.pie(sizes, shadow=False, autopct=lambda p: "{:.2f}% ({:,.0f})".format(p,p * sum(sizes)/100), startangle=45, colors=colors, counterclock=False )
    plt.axis("equal")
    plt.title("Isle of Conquest", y=0.95)

    plt.subplot(gs1[5,0])
    sizes = [cardsdict["Deepwind Gorge"]["Deepwind Gorge victories"],cardsdict["Deepwind Gorge"]["Deepwind Gorge battles"] - cardsdict["Deepwind Gorge"]["Deepwind Gorge victories"]]
    if sizes[1] + sizes[0] == 0: plt.pie([1], shadow=False, startangle=45, colors=["grey"], counterclock=False)
    else: plt.pie(sizes, shadow=False, autopct=lambda p: "{:.2f}% ({:,.0f})".format(p,p * sum(sizes)/100), startangle=45, colors=colors, counterclock=False )
    plt.axis("equal")
    plt.title("Deepwind Gorge", y=0.95)

#    plt.show()
    fig.savefig("static/" + filename, bbox_inches='tight', pad_inches=0)

    #filepath = os.path.abspath(filename)

    return filename


def calcStats(simpledict): # calculates stats by aggregating stats for each bg without EotS
    matchlist = []
    victorieslist = []

    for key in simpledict.keys():
        if "battles" in key and "Rated" not in key: matchlist.append(simpledict[key])
        if "victories" in key and "Rated" not in key: victorieslist.append(simpledict[key])

    matches = sum(matchlist) - simpledict["Eye of the Storm battles"]
    victories = sum(victorieslist) - simpledict["Eye of the Storm victories"]

    ceotsbattles = simpledict["Battlegrounds played"] - matches
    ceotsvictories = simpledict["Battlegrounds won"] - victories

    matchdeficit = ceotsbattles - simpledict["Eye of the Storm battles"]
    victoriesdeficit = ceotsvictories - simpledict["Eye of the Storm victories"]

    return ((matches, victories), (ceotsbattles, ceotsvictories), (matchdeficit, victoriesdeficit))


def refineCharInfo(infodict):
    raceList = {1:"Human", 2:"Orc", 3:"Dwarf", 4:"Night Elf", 5:"Undead", 6:"Tauren", 7:"Gnome", 8:"Troll", 9:"Goblin", 10:"Blood Elf", 11:"Draenei",
                22:"Worgen", 24:"Pandaren", 25:"Pandaren", 26:"Pandaren", 27:"Nightborne", 28:"Highmountain Tauren", 29:"Void Elf", 30:"Lightforged Draeenei",
                34:"Dark Iron Dwarf", 36:"Mag'har Orc"}
    factionList = ["Alliance", "Horde", "Neutral"]
    classList = ["Warrior", "Paladin", "Hunter", "Rogue", "Priest", "Death Knight", "Shaman", "Mage", "Warlock", "Monk", "Druid", "Demon Hunter"]
    genderList = ["Male", "Female"]
    infodict["avatar"] = "https://render-eu.worldofwarcraft.com/character/{}".format(infodict["thumbnail"])
    infodict["class"] = classList[infodict["class"] - 1]
    infodict["gender"] = genderList[infodict["gender"]]
    infodict["faction"] = factionList[infodict["faction"]]
    infodict["race"] = raceList.get(infodict["race"], "Unknown")

    return infodict

def refineCharandRealm(char, realm):
    char = char.lower()
    realm = realm.lower()
    return (char, realm)

def createBGDF(charlist, bglist):
    df_char = pd.DataFrame(charlist)
    df_bg = pd.DataFrame(bglist)

    df_main = pd.merge(df_char, df_bg, on=["Player_ID"])
    
    df_horde = df_main[df_main.Faction == "Horde"]
    df_alliance = df_main[df_main.Faction == "Alliance"]

    general_dict = createServerBGEntry("General", df_main)
    horde_dict = createServerBGEntry("Horde", df_horde)
    alliance_dict = createServerBGEntry("Alliance", df_alliance)

    return (general_dict, horde_dict, alliance_dict)



def createServerBGEntry(cat, df):
    if cat not in {"General", "Horde", "Alliance"}:
        print("Category Error!")
        return {}
    else:
        DB_bg_dict = {}
        diff_set = {"Unnamed: 0", "Class", "Faction", "Name", "Player_ID", "Race", "Server_ID", "Server_Name", "failed_attempts", "lvl", "scanned_on_x", "scanned_on_y", "played_most_n", "won_most_n"}

        for item in df:
            if item not in diff_set:
                DB_bg_dict[f"{item}"] = int(df[item].sum())
                DB_bg_dict[f"{item}_mean"] = round(df[item].mean(),2)
                DB_bg_dict[f"{item}_max"] = int(df[item].max())
                DB_bg_dict[f"{item}_75"] = round(df[item].quantile(0.75),2)

        DB_bg_dict["played_most_n"] = df["played_most_n"].replace("None", np.nan).dropna().value_counts()[[0]].index.item()
        DB_bg_dict["won_most_n"] = df["won_most_n"].replace("None", np.nan).dropna().value_counts()[[0]].index.item()
        DB_bg_dict["Type"] = cat
        DB_bg_dict["Server_ID"] = df["Server_ID"].head(1).item()
        DB_bg_dict["entries"] = int(df["Name"].count())


        return DB_bg_dict

def createServerCharts(bg_dict):
    df = pd.DataFrame(bg_dict)
    #print(df.head())

    df = df.set_index(["Type", df["scanned_on"]]).sort_index()
    #print(df)

    df_general = df.loc[("General", df.index.levels[1].max())]
    df_horde = df.loc[("Horde", df.index.levels[1].max())]
    df_alliance = df.loc[("Alliance", df.index.levels[1].max())]

    print(f"\tCreating BG Chart for Server ID {df_general['Server_ID']}...")
    plotServerBGs(df_general, df_horde, df_alliance)
    print("\tDone.")

    #df_played = df[["BG_played", "BG_played_c"]].unstack(level="Type")
    #print(df_played)

def createDiffCharts(bg_dict):
    df = pd.DataFrame(bg_dict)
    df = df.set_index(["Type", df["scanned_on"]]).sort_index()

    col_list = ["BG_played", "BG_won", "BG_played_c", "BG_won_c", "AV_played", "AV_won","AB_played", "AB_won", "Gil_played", "Gil_won", "EotS_played", "EotS_won",
                "SS_played", "SS_won", "TP_played", "TP_won", "WS_played", "WS_won", "SM_played", "SM_won", "TK_played", "TK_won", "IoC_played", "IoC_won", "DG_played", "DG_won"]

    #df_general = df[col_list].loc[("General")].select_dtypes(include=["int64", "float64"]).diff()
    #df_general_diff = df[col_list].loc[("General")].diff().loc[df.index.levels[1].max()]
    #df_general_diff["Type", "entries"] = "General", 
    #df_horde_diff = df[col_list].loc[("Horde")].diff().loc[df.index.levels[1].max()]
    #df_horde_diff["Type"] = "Horde"
    #df_alliance_diff = df[col_list].loc[("Alliance")].diff().loc[df.index.levels[1].max()]
    #df_alliance_diff["Type"] = "Alliance"

    df_general_diff = df[col_list +["entries"]].loc[("General")]
    df_general_diff[col_list] = df_general_diff[col_list].diff()
    df_general_diff["Type"] = "General"
    df_general_diff = df_general_diff.loc[df_general_diff.index.max()]

    df_horde_diff = df[col_list +["entries"]].loc[("Horde")]
    df_horde_diff[col_list] = df_horde_diff[col_list].diff()
    df_horde_diff["Type"] = "Horde"
    df_horde_diff = df_horde_diff.loc[df_horde_diff.index.max()]

    df_alliance_diff = df[col_list +["entries"]].loc[("Alliance")]
    df_alliance_diff[col_list] = df_alliance_diff[col_list].diff()
    df_alliance_diff["Type"] = "Alliance"
    df_alliance_diff = df_alliance_diff.loc[df_alliance_diff.index.max()]

    df_general_diff = refineDiffDf(df_general_diff)
    df_horde_diff = refineDiffDf(df_horde_diff)
    df_alliance_diff = refineDiffDf(df_alliance_diff)

    #print(df_general_diff)

    plotServerBGs(df_general_diff, df_horde_diff, df_alliance_diff)

    print("done.")

def plotServerBGs(dfg, dfh, dfa):
    filename = "figures/" + str(dfg["Server_ID"]) + "_BG" + "_.svg"

    fig = plt.figure(figsize=(10,16), frameon=False)
    gs1 = GridSpec(9,7)
    #total BGs General
    sizes = (dfg["BG_won_mean"], dfg["BG_played_mean"] - dfg["BG_won_mean"])
    plotBGPie(gs1[0:2, 0:2], sizes, f"Total BGs \n({dfg['BG_won']} to {dfg['BG_played'] - dfg['BG_won']})", True)

    #total BGs Horde
    sizes = (dfh["BG_won_mean"], dfh["BG_played_mean"] - dfh["BG_won_mean"])
    plotBGPie(gs1[0:2, 2:4], sizes, f"Total BGs Horde \n({dfh['BG_won']} to {dfh['BG_played'] - dfh['BG_won']})", True)

    #total BGs Alliance
    sizes = (dfa["BG_won_mean"], dfa["BG_played_mean"] - dfa["BG_won_mean"])
    plotBGPie(gs1[0:2, 4:7], sizes, f"Total BGs Alliance \n({dfa['BG_won']} to {dfa['BG_played'] - dfa['BG_won']})", True, True)

    #calculated general
    sizes = (dfg["BG_won_c_mean"], dfg["BG_played_c_mean"] - dfg["BG_won_c_mean"])
    plotBGPie(gs1[2, 0], sizes, f" BGs (calculated)\nTotal")

    #calculated Horde
    sizes = (dfh["BG_won_c_mean"], dfh["BG_played_c_mean"] - dfh["BG_won_c_mean"])
    plotBGPie(gs1[2, 1], sizes, f"Horde")

    #calculated Alliance
    sizes = (dfa["BG_won_c_mean"], dfa["BG_played_c_mean"] - dfa["BG_won_c_mean"])
    plotBGPie(gs1[2, 2], sizes, f"Alliance")

    #AV general
    sizes = (dfg["AV_won_mean"], dfg["AV_played_mean"] - dfg["AV_won_mean"])
    plotBGPie(gs1[2, 4], sizes, f"Alterac Valley\nTotal")

    #AV Horde
    sizes = (dfh["AV_won_mean"], dfh["AV_played_mean"] - dfh["AV_won_mean"])
    plotBGPie(gs1[2, 5], sizes, f"Horde")

    #AV Alliance
    sizes = (dfa["AV_won_mean"], dfa["AV_played_mean"] - dfa["AV_won_mean"])
    plotBGPie(gs1[2, 6], sizes, f"Alliance")

    #AB gernal
    sizes = (dfg["AB_won_mean"], dfg["AB_played_mean"] - dfg["AB_won_mean"])
    plotBGPie(gs1[3, 0], sizes, f" Arathi Basin\nTotal")

    #AB Horde
    sizes = (dfh["AB_won_mean"], dfh["AB_played_mean"] - dfh["AB_won_mean"])
    plotBGPie(gs1[3, 1], sizes, f"Horde")

    #AB Alliance
    sizes = (dfa["AB_won_mean"], dfa["AB_played_mean"] - dfa["AB_won_mean"])
    plotBGPie(gs1[3, 2], sizes, f"Alliance")

    #Gil general
    sizes = (dfg["Gil_won_mean"], dfg["Gil_played_mean"] - dfg["Gil_won_mean"])
    plotBGPie(gs1[3, 4], sizes, f"Battle for Gilneas\nTotal")

    #Gil Horde
    sizes = (dfh["Gil_won_mean"], dfh["Gil_played_mean"] - dfh["Gil_won_mean"])
    plotBGPie(gs1[3, 5], sizes, f"Horde")

    #Gil Alliance
    sizes = (dfa["Gil_won_mean"], dfa["Gil_played_mean"] - dfa["Gil_won_mean"])
    plotBGPie(gs1[3, 6], sizes, f"Alliance")

    #SS gernal
    sizes = (dfg["SS_won_mean"], dfg["SS_played_mean"] - dfg["SS_won_mean"])
    plotBGPie(gs1[4, 0], sizes, f" Seething Shore\nTotal")

    #SS Horde
    sizes = (dfh["SS_won_mean"], dfh["SS_played_mean"] - dfh["SS_won_mean"])
    plotBGPie(gs1[4, 1], sizes, f"Horde")

    #SS Alliance
    sizes = (dfa["SS_won_mean"], dfa["SS_played_mean"] - dfa["SS_won_mean"])
    plotBGPie(gs1[4, 2], sizes, f"Alliance")

    #SotA general
    sizes = (dfg["SotA_won_mean"], dfg["SotA_played_mean"] - dfg["SotA_won_mean"])
    plotBGPie(gs1[4, 4], sizes, f"Strand of the Ancients\nTotal")

    #SotA Horde
    sizes = (dfh["SotA_won_mean"], dfh["SotA_played_mean"] - dfh["SotA_won_mean"])
    plotBGPie(gs1[4, 5], sizes, f"Horde")

    #SotA Alliance
    sizes = (dfa["SotA_won_mean"], dfa["SotA_played_mean"] - dfa["SotA_won_mean"])
    plotBGPie(gs1[4, 6], sizes, f"Alliance")

    #TP gernal
    sizes = (dfg["TP_won_mean"], dfg["TP_played_mean"] - dfg["TP_won_mean"])
    plotBGPie(gs1[5, 0], sizes, f" Twin Peaks\nTotal")

    #TP Horde
    sizes = (dfh["TP_won_mean"], dfh["TP_played_mean"] - dfh["TP_won_mean"])
    plotBGPie(gs1[5, 1], sizes, f"Horde")

    #TP Alliance
    sizes = (dfa["TP_won_mean"], dfa["TP_played_mean"] - dfa["TP_won_mean"])
    plotBGPie(gs1[5, 2], sizes, f"Alliance")

    #WS general
    sizes = (dfg["WS_won_mean"], dfg["WS_played_mean"] - dfg["WS_won_mean"])
    plotBGPie(gs1[5, 4], sizes, f"Warsong Gulch\nTotal")

    #WS Horde
    sizes = (dfh["WS_won_mean"], dfh["WS_played_mean"] - dfh["WS_won_mean"])
    plotBGPie(gs1[5, 5], sizes, f"Horde")

    #WS Alliance
    sizes = (dfa["WS_won_mean"], dfa["WS_played_mean"] - dfa["WS_won_mean"])
    plotBGPie(gs1[5, 6], sizes, f"Alliance")

    #SM gernal
    sizes = (dfg["SM_won_mean"], dfg["SM_played_mean"] - dfg["SM_won_mean"])
    plotBGPie(gs1[6, 0], sizes, f" Silvershard Mine\nTotal")

    #SM Horde
    sizes = (dfh["SM_won_mean"], dfh["SM_played_mean"] - dfh["SM_won_mean"])
    plotBGPie(gs1[6, 1], sizes, f"Horde")

    #SM Alliance
    sizes = (dfa["SM_won_mean"], dfa["SM_played_mean"] - dfa["SM_won_mean"])
    plotBGPie(gs1[6, 2], sizes, f"Alliance")

    #TK general
    sizes = (dfg["TK_won_mean"], dfg["TK_played_mean"] - dfg["TK_won_mean"])
    plotBGPie(gs1[6, 4], sizes, f"Temple of Kotmogu\nTotal")

    #TK Horde
    sizes = (dfh["TK_won_mean"], dfh["TK_played_mean"] - dfh["TK_won_mean"])
    plotBGPie(gs1[6, 5], sizes, f"Horde")

    #TK Alliance
    sizes = (dfa["TK_won_mean"], dfa["TK_played_mean"] - dfa["TK_won_mean"])
    plotBGPie(gs1[6, 6], sizes, f"Alliance")

    #IoC gernal
    sizes = (dfg["IoC_won_mean"], dfg["IoC_played_mean"] - dfg["IoC_won_mean"])
    plotBGPie(gs1[7, 0], sizes, f" Isle of Conquest\nTotal")

    #IoC Horde
    sizes = (dfh["IoC_won_mean"], dfh["IoC_played_mean"] - dfh["IoC_won_mean"])
    plotBGPie(gs1[7, 1], sizes, f"Horde")

    #IoC Alliance
    sizes = (dfa["IoC_won_mean"], dfa["IoC_played_mean"] - dfa["IoC_won_mean"])
    plotBGPie(gs1[7, 2], sizes, f"Alliance")

    #DG general
    sizes = (dfg["DG_won_mean"], dfg["DG_played_mean"] - dfg["DG_won_mean"])
    plotBGPie(gs1[7, 4], sizes, f"Deepwind Gorge\nTotal")

    #DG Horde
    sizes = (dfh["DG_won_mean"], dfh["DG_played_mean"] - dfh["DG_won_mean"])
    plotBGPie(gs1[7, 5], sizes, f"Horde")

    #DG Alliance
    sizes = (dfa["DG_won_mean"], dfa["DG_played_mean"] - dfa["DG_won_mean"])
    plotBGPie(gs1[7, 6], sizes, f"Alliance")

    #EotS gernal
    sizes = (dfg["EotS_won_mean"], dfg["EotS_played_mean"] - dfg["EotS_won_mean"])
    plotBGPie(gs1[8, 0], sizes, f"Eye of the Storm\nTotal")

    #EotS Horde
    sizes = (dfh["EotS_won_mean"], dfh["EotS_played_mean"] - dfh["EotS_won_mean"])
    plotBGPie(gs1[8, 1], sizes, f"Horde")

    #EotS Alliance
    sizes = (dfa["EotS_won_mean"], dfa["EotS_played_mean"] - dfa["EotS_won_mean"])
    plotBGPie(gs1[8, 2], sizes, f"Alliance")

    #EotS Est general
    sizes = (dfg["EotS_won_est_mean"], dfg["EotS_played_est_mean"] - dfg["EotS_won_est_mean"])
    plotBGPie(gs1[8, 4], sizes, f"Eye of the Storm Estimated\nTotal")

    #EotS Est Horde
    sizes = (dfh["EotS_won_est_mean"], dfh["EotS_played_est_mean"] - dfh["EotS_won_est_mean"])
    plotBGPie(gs1[8, 5], sizes, f"Horde")

    #EotS Est Alliance
    sizes = (dfa["EotS_won_est_mean"], dfa["EotS_played_est_mean"] - dfa["EotS_won_est_mean"])
    plotBGPie(gs1[8, 6], sizes, f"Alliance")

    fig.savefig("static/" + filename, bbox_inches='tight', pad_inches=0)

    return True

def plotBGPie(gs, sizes, title, isBig=False, isLegend=False):
    colors = ["limegreen", "lightgrey"]
    ax = plt.subplot(gs)
    if sizes[1] + sizes[0] == 0: texts, autotext = ax.pie([1], shadow=False, startangle=135, colors=["grey"], counterclock=False)
    elif sizes[0] < 1 or sizes[1] < 1:
        sizes = [sizes[0]*10, sizes[1]*10]
        wedges, texts, autotext = ax.pie(sizes, shadow=False, autopct=lambda p: "{:.2f}%\n(Ø {:,.2f})".format(p,p * sum(sizes)/1000), startangle=135, colors=colors, counterclock=False )
    else: wedges, texts, autotext = ax.pie(sizes, shadow=False, autopct=lambda p: "{:.2f}%\n(Ø {:,.2f})".format(p,p * sum(sizes)/100), startangle=135, colors=colors, counterclock=False )
    plt.axis("equal")
    if isLegend == True: plt.legend(["Win","Loss"])
    if isBig == True: plt.title(title, y=0.95, fontsize=10)
    else: 
        plt.title(title, y=0.85, fontsize=8)
        plt.setp(autotext, size=8)


def aggregateServerBGs(bg_dict_list, language):
    df = pd.DataFrame(bg_dict_list)
    #print(df.head())
    resultlist = []

    if language == "all": s_id = int(0)
    elif language == "german": s_id = int(1001)
    elif language == "english": s_id = int(1002)

    df = df.set_index(["Type", "Server_ID"], drop=False).sort_index()
    #print(df.head())

    df_general = df.loc[("General")]
    df_horde = df.loc[("Horde")]
    df_alliance = df.loc[("Alliance")]

    dfset = [df_general, df_horde, df_alliance]

    for dfx in dfset:
        dbdict = {}
        for entry in dfx:            
            if "mean" in entry: dbdict[entry] = dfx[entry[:-5]].sum() / dfx["entries"].sum()
            elif "max" in entry: dbdict[entry] = int(dfx[entry].max())
            elif "75" in entry: dbdict[entry] = dfx[entry].mean() #inaccurate...
            elif "Server_ID" in entry: dbdict[entry] = s_id
            elif "Type" in entry: dbdict[entry] = dfx[entry].iloc[0]
            elif "most_n" in entry: dbdict[entry] = dfx[[entry, entry[:-2]+"_c"]].groupby(entry).sum().sort_values(by=entry[:-2]+"_c", ascending=False).index[0]
            elif "most_c_max" in entry: dbdict[entry] = int(dfx[entry].max())
            elif "most_c" in entry: dbdict[entry] = int(dfx[[entry[:-2]+"_n", entry]].groupby(entry[:-2]+"_n").sum().sort_values(by=entry, ascending=False)[entry].iloc[0])
            elif "scanned" in entry: pass          
            else: dbdict[entry] = int(dfx[entry].sum())

        resultlist.append(dbdict)



#        resultlist.append({"Server_ID": 0,
#                           "Type": dfx["Type"].iloc[0],
#                           "BG_played": dfx["BG_played"].sum(),
#                           "BG_played_mean":int(dfx["BG_played"].mean()),
#                           "BG_played_max": dfx["BG_played_max"].max(),
#                           "BG_played_75": int(dfx["BG_played_75"].mean()),
#                           "BG_won": dfx["BG_won"].sum(),
#                           "BG_won_mean":int(dfx["BG_won"].mean()),
#                           "BG_won_max": dfx["BG_won_max"].max(),
#                           "BG_won_75": int(dfx["BG_won_75"].mean()),
#                           "BG_played_c": dfx["BG_played_c"].sum(),
#                           "BG_played_c_mean":int(dfx["BG_played_c"].mean()),
#                           "BG_played_c_max": dfx["BG_played_c_max"].max(),
#                           "BG_played_c_75": int(dfx["BG_played_c_75"].mean()),
#                           "AV_played": dfx["AV_played"].sum(),
#                           "AV_played_mean":int(dfx["AV_played"].mean()),
#                           "AV_played_max": dfx["AV_played_max"].max(),
#                           "AV_played_75": int(dfx["AV_played_75"].mean()),
#                           "AV_won": dfx["AV_won"].sum(),
#                           "AV_won_mean":int(dfx["AV_won"].mean()),
#                           "AV_won_max": dfx["AV_won_max"].max(),
#                           "AV_won_75": int(dfx["AV_won_75"].mean()),
#                           "AV_tower_def": dfx["AV_tower_def"].sum(),
#                           "AV_tower_def_mean":int(dfx["AV_tower_def"].mean()),
#                           "AV_tower_def_max": dfx["AV_tower_def_max"].max(),
#                           "AV_tower_def_75": int(dfx["AV_tower_def_75"].mean()),
#                           "AV_tower_cap": dfx["AV_tower_cap"].sum(),
#                           "AV_tower_cap_mean":int(dfx["AV_tower_cap"].mean()),
#                           "AV_tower_cap_max": dfx["AV_tower_cap_max"].max(),
#                           "AV_tower_cap_75": int(dfx["AV_tower_cap_75"].mean()),
#                           "AB_played": dfx["AB_played"].sum(),
#                           "AB_played_mean":int(dfx["AB_played"].mean()),
#                           "AB_played_max": dfx["AB_played_max"].max(),
#                           "AB_played_75": int(dfx["AB_played_75"].mean()),
#                           "AB_won": dfx["AB_won"].sum(),
#                           "AB_won_mean":int(dfx["AB_won"].mean()),
#                           "AB_won_max": dfx["AB_won_max"].max(),
#                           "AB_won_75": int(dfx["AB_won_75"].mean()),
#                           "Gil_played": dfx["Gil_played"].sum(),
#                           "Gil_played_mean":int(dfx["Gil_played"].mean()),
#                           "Gil_played_max": dfx["Gil_played_max"].max(),
#                           "Gil_played_75": int(dfx["Gil_played_75"].mean()),
#                           "Gil_won": dfx["Gil_won"].sum(),
#                           "Gil_won_mean":int(dfx["Gil_won"].mean()),
#                           "Gil_won_max": dfx["Gil_won_max"].max(),
#                           "Gil_won_75": int(dfx["Gil_won_75"].mean()),
#                           "EotS_played": dfx["EotS_played"].sum(),
#                           "EotS_played_mean":int(dfx["EotS_played"].mean()),
#                           "EotS_played_max": dfx["EotS_played_max"].max(),
#                           "EotS_played_75": int(dfx["EotS_played_75"].mean()),
#                           "EotS_won": dfx["EotS_won"].sum(),
#                           "EotS_won_mean":int(dfx["EotS_won"].mean()),
#                           "EotS_won_max": dfx["EotS_won_max"].max(),
#                           "EotS_won_75": int(dfx["EotS_won_75"].mean()),
#                           "EotS_flags": dfx["EotS_flags"].sum(),
#                           "EotS_flags_mean":int(dfx["EotS_flags"].mean()),
#                           "EotS_flags_max": dfx["EotS_flags_max"].max(),
#                           "EotS_flags_75": int(dfx["EotS_flags_75"].mean()),
#                           "EotS_played_est": dfx["EotS_played_est"].sum(),
#                           "EotS_played_est_mean":int(dfx["EotS_played_est"].mean()),
#                           "EotS_played_est_max": dfx["EotS_played_est_max"].max(),
#                           "EotS_played_est_75": int(dfx["EotS_played_est_75"].mean()),
#                           "EotS_won_est": dfx["EotS_won_est"].sum(),
#                           "EotS_won_est_mean":int(dfx["EotS_won_est"].mean()),
#                           "EotS_won_est_max": dfx["EotS_won_est_max"].max(),
#                           "EotS_won_est_75": int(dfx["EotS_won_est_75"].mean()),
#                           "EotS_played_def": dfx["EotS_played_def"].sum(),
#                           "EotS_played_def_mean":int(dfx["EotS_played_def"].mean()),
#                           "EotS_played_def_max": dfx["EotS_played_def_max"].max(),
#                           "EotS_played_def_75": int(dfx["EotS_played_def_75"].mean()),
#                           "EotS_won_def": dfx["EotS_won_def"].sum(),
#                           "EotS_won_def_mean":int(dfx["EotS_won_def"].mean()),
#                           "EotS_won_def_max": dfx["EotS_won_def_max"].max(),
#                           "EotS_won_def_75": int(dfx["EotS_won_def_75"].mean()),
#                           "SS_played": dfx["SS_played"].sum(),
#                           "SS_played_mean":int(dfx["SS_played"].mean()),
#                           "SS_played_max": dfx["SS_played_max"].max(),
#                           "SS_played_75": int(dfx["SS_played_75"].mean()),
#                           "SS_won": dfx["SS_won"].sum(),
#                           "SS_won_mean":int(dfx["SS_won"].mean()),
#                           "SS_won_max": dfx["SS_won_max"].max(),
#                           "SS_won_75": int(dfx["SS_won_75"].mean()),
#                           "SotA_played": dfx["SotA_played"].sum(),
#                           "SotA_played_mean":int(dfx["SotA_played"].mean()),
#                           "SotA_played_max": dfx["SotA_played_max"].max(),
#                           "SotA_played_75": int(dfx["SotA_played_75"].mean()),
#                           "SotA_won": dfx["SotA_won"].sum(),
#                           "SotA_won_mean":int(dfx["SotA_won"].mean()),
#                           "SotA_won_max": dfx["SotA_won_max"].max(),
#                           "SotA_won_75": int(dfx["SotA_won_75"].mean()),
#                           "TP_played": dfx["TP_played"].sum(),
#                           "TP_played_mean":int(dfx["TP_played"].mean()),
#                           "TP_played_max": dfx["TP_played_max"].max(),
#                           "TP_played_75": int(dfx["TP_played_75"].mean()),
#                           "TP_won": dfx["TP_won"].sum(),
#                           "TP_won_mean":int(dfx["TP_won"].mean()),
#                           "TP_won_max": dfx["TP_won_max"].max(),
#                           "TP_won_75": int(dfx["TP_won_75"].mean()),
#                           "TP_flags_cap": dfx["TP_flags_cap"].sum(),
#                           "TP_flags_cap_mean":int(dfx["TP_flags_cap"].mean()),
#                           "TP_flags_cap_max": dfx["TP_flags_cap_max"].max(),
#                           "TP_flags_cap_75": int(dfx["TP_flags_cap_75"].mean()),
#                           "TP_flags_ret": dfx["TP_flags_ret"].sum(),
#                           "TP_flags_ret_mean":int(dfx["TP_flags_ret"].mean()),
#                           "TP_flags_ret_max": dfx["TP_flags_ret_max"].max(),
#                           "TP_flags_ret_75": int(dfx["TP_flags_ret_75"].mean()),
#                           "WS_played": dfx["WS_played"].sum(),
#                           "WS_played_mean":int(dfx["WS_played"].mean()),
#                           "WS_played_max": dfx["WS_played_max"].max(),
#                           "WS_played_75": int(dfx["WS_played_75"].mean()),
#                           "WS_won": dfx["WS_won"].sum(),
#                           "WS_won_mean":int(dfx["WS_won"].mean()),
#                           "WS_won_max": dfx["WS_won_max"].max(),
#                           "WS_won_75": int(dfx["WS_won_75"].mean()),
#                           "WS_flags_cap": dfx["WS_flags_cap"].sum(),
#                           "WS_flags_cap_mean":int(dfx["WS_flags_cap"].mean()),
#                           "WS_flags_cap_max": dfx["WS_flags_cap_max"].max(),
#                           "WS_flags_cap_75": int(dfx["WS_flags_cap_75"].mean()),
#                           "WS_flags_ret": dfx["WS_flags_ret"].sum(),
#                           "WS_flags_ret_mean":int(dfx["WS_flags_ret"].mean()),
#                           "WS_flags_ret_max": dfx["WS_flags_ret_max"].max(),
#                           "WS_flags_ret_75": int(dfx["WS_flags_ret_75"].mean()),
#                           "TK_played": dfx["TK_played"].sum(),
#                           "TK_played_mean":int(dfx["TK_played"].mean()),
#                           "TK_played_max": dfx["TK_played_max"].max(),
#                           "TK_played_75": int(dfx["TK_played_75"].mean()),
#                           "TK_won": dfx["TK_won"].sum(),
#                           "TK_won_mean":int(dfx["TK_won"].mean()),
#                           "TK_won_max": dfx["TK_won_max"].max(),
#                           "TK_won_75": int(dfx["TK_won_75"].mean()),
#                           "IoC_played": dfx["IoC_played"].sum(),
#                           "IoC_played_mean":int(dfx["IoC_played"].mean()),
#                           "IoC_played_max": dfx["IoC_played_max"].max(),
#                           "IoC_played_75": int(dfx["IoC_played_75"].mean()),
#                           "IoC_won": dfx["IoC_won"].sum(),
#                           "IoC_won_mean":int(dfx["IoC_won"].mean()),
#                           "IoC_won_max": dfx["IoC_won_max"].max(),
#                           "IoC_won_75": int(dfx["IoC_won_75"].mean()),
#                           "DG_played": dfx["DG_played"].sum(),
#                           "DG_played_mean":int(dfx["DG_played"].mean()),
#                           "DG_played_max": dfx["DG_played_max"].max(),
#                           "DG_played_75": int(dfx["DG_played_75"].mean()),
#                           "DG_won": dfx["DG_won"].sum(),
#                           "DG_won_mean":int(dfx["DG_won"].mean()),
#                           "DG_won_max": dfx["DG_won_max"].max(),
#                           "DG_won_75": int(dfx["DG_won_75"].mean()),
#                           "played_most_n": dfx[["played_most_n","played_most_c"]].groupby("played_most_n").sum().sort_values(by="played_most_c", ascending=False).index[0],
#                           "played_most_c": int(dfx[["played_most_n","played_most_c"]].groupby("played_most_n").sum().sort_values(by="played_most_c", ascending=False)["played_most_c"].iloc[0]),
#                           "played_most_c_mean": int(dfx["played_most_c"].mean()),
#                           "played_most_c_max": dfx["played_most_c_max"].max(),
#                           "played_most_c_75": int(dfx["played_most_c_75"].mean()),
#                           "won_most_n": dfx[["won_most_n","won_most_c"]].groupby("won_most_n").sum().sort_values(by="won_most_c", ascending=False).index[0],
#                           "won_most_c": int(dfx[["won_most_n","won_most_c"]].groupby("won_most_n").sum().sort_values(by="won_most_c", ascending=False)["won_most_c"].iloc[0]),
#                           "won_most_c_mean": int(dfx["won_most_c"].mean()),
#                           "won_most_c_max": dfx["won_most_c_max"].max(),
#                           "won_most_c_75": int(dfx["won_most_c_75"].mean()),
#                           "entries":dfx["entries"].sum()
#                           })



    #print(df_general.head())
    #print(df_general.describe())

    return resultlist

def refineDiffDf(df):
    for entry in df.index:
        if entry not in ("Type", "entries"):
            df[entry +"_mean"] = (df[entry] / df["entries"]) *100
    df["EotS_played_est_mean"] = 0
    df["EotS_won_est_mean"] = 0
    df["SotA_played_mean"] = 0
    df["SotA_won_mean"] = 0
    df["Server_ID"] = "0_diff"
    return df