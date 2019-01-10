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
    if bgdict["name"] != "Battlegrounds": return "Error, incorrect Data or Data Format received (refineBGInfo)"
    else:
        simpledict = {}   
        for item in bgdict["statistics"]:
            if "highest" in item.keys(): simpledict[item["name"]] = (item["highest"], str(item["quantity"]))
            else: simpledict[item["name"]] = item["quantity"]
        cardsdict = {}

        cardsdict["General"] = {"Battlegrounds played": simpledict["Battlegrounds played"],
                                "Battlegrounds won": simpledict["Battlegrounds won"],
                                "Battleground played the most": simpledict["Battleground played the most"][0] + " (" + simpledict["Battleground played the most"][1] + ")",
                                "Battleground won the most": simpledict["Battleground won the most"][0] + " (" + simpledict ["Battleground won the most"][1] + ")"}


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
                                         "Eye of the Storm flags captured": simpledict["Eye of the Storm flags captured"]}

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


def createBGCharts(char, realm, cardsdict):
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
    filename = "figures/" + char + "_" + realm + "_.jpg"
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