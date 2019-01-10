import os
import time
from warnings import simplefilter
simplefilter("ignore")
import glob
import codecs
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import json
import requests
from flask import Flask, jsonify, abort, make_response, request, url_for, render_template, redirect, flash
import BlizzApiCalls as bac
from config import Config
from apiforms import CharSelect
import infoRefinement as ir


app = Flask(__name__)
app.config.from_object(Config)

@app.route('/')
def index():
    return render_template("index.html", title="Home")

@app.route('/start', methods=["GET", "POST"])
def SelectChar():
    form = CharSelect()
    if form.validate_on_submit():
        flash("Fetching Data...")
        return redirect("/" + form.info.data + "/" + form.realm.data + "/" + form.char.data)
    return render_template("charselect.html", title="Character Selection", form=form)


@app.route("/statistics/<realm>/<char>", methods=["GET"])
def GetStatistics(realm, char):

    return jsonify(bac.getStatistic(char, realm, "eu"))

@app.route("/ratedpvp/<realm>/<char>", methods=["GET"])
def GetRatedPvP(realm, char):

    return jsonify(bac.getRatedPvP(char, realm, "eu"))

@app.route("/audit/<realm>/<char>", methods=["GET"])
def GetAudit(realm, char):

    return jsonify(bac.getAudit(char, realm, "eu"))

@app.route("/quests/<realm>/<char>", methods=["GET"])
def GetQuests(realm, char):

    return jsonify(bac.getQuests(char, realm, "eu"))


@app.route("/mounts/<realm>/<char>", methods=["GET"])
def GetMounts(realm, char):

    return jsonify(bac.getMounts(char, realm, "eu"))

@app.route("/honorableKills/<realm>/<char>", methods=["GET"])
def GetHonorableKills(realm, char):

    return jsonify(bac.getHonorableKills(char, realm, "eu"))

@app.route("/killingBlows/<realm>/<char>", methods=["GET"])
def GetKillingBlows(realm, char):

    return jsonify(bac.getKillingBlows(char, realm, "eu"))

@app.route("/deaths/<realm>/<char>", methods=["GET"])
def GetDeaths(realm, char):

    return jsonify(bac.getDeaths(char, realm, "eu"))

@app.route("/pvp/<realm>/<char>", methods=["GET"])
def GetPvP(realm, char):

    return jsonify(bac.getPvP(char, realm, "eu"))

@app.route("/bg/<realm>/<char>", methods=["GET"])
def GetBGs(realm, char):

    return jsonify(bac.getBGs(char, realm, "eu")[1])

@app.route("/bg2/<realm>/<char>", methods=["GET"])
def GetBG2s(realm, char):

    dictTuple = bac.getBGs(char, realm, "eu")
    cardsdict = ir.refineBGInfo(dictTuple[1])
    infodict = dictTuple[0]
    infodict = ir.refineCharInfo(infodict)
    chartfile = ir.createMoreBGCharts(infodict["name"], infodict["realm"], cardsdict)
    return render_template("bg.html", title="Character Selection", cardsdict=cardsdict, infodict=infodict, chartfile=chartfile)



if __name__ == "__main__":
    app.run(host="0.0.0.0")