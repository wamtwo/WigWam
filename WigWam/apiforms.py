from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, SelectField
from wtforms.validators import DataRequired

class CharSelect(FlaskForm):
    choiceList = [("statistics", "Full Statistics"), ("ratedPvP", "Rated PvP"), ("audit", "Audit Data"), ("quests", "Quests"), ("mounts", "Mounts"),
                 ("honorableKills", "Honorable Kills"), ("killingBlows", "Killing Blows"), ("deaths", "Deaths"), ("pvp", "PvP"), ("bg", "Battlegrounds"), ("bg2", "Battlegrounds(2)")]

    realm = StringField("Realm", validators=[DataRequired()])
    char = StringField("Charname", validators=[DataRequired()])
    info = SelectField("Info", validators=[DataRequired()], choices=choiceList)
    submit = SubmitField("Go!")

