from flask import json, request
from flask_classy import FlaskView, route
from git import Repo, Git
import sqlite3
from modules.app_config import cbpi
from werkzeug.utils import secure_filename
import pprint
import time
import os
from modules.steps import Step, StepView


class KBH(FlaskView):


    @route('/', methods=['GET'])
    def get(self):
        conn = None
        global KBH_VERSION
        try:
            if not os.path.exists(self.api.app.config['UPLOAD_FOLDER'] + '/kbh.db'):
                self.api.notify(headline="File Not Found", message="Please upload a Kleiner Brauhelfer Database", type="danger")
                return ('', 404)
            # test kbh1 databse format
            conn = sqlite3.connect(self.api.app.config['UPLOAD_FOLDER'] + '/kbh.db')
            c = conn.cursor()
            c.execute('SELECT ID, Sudname, BierWurdeGebraut FROM Sud')
            data = c.fetchall()
            result = []
            for row in data:
                result.append({"id": row[0], "name": row[1], "brewed": row[2]})
            KBH_VERSION = 1
            return json.dumps(result)
        except:
            # if kbh1 format does not work, try kbh2 database format
            try:
                conn = sqlite3.connect(self.api.app.config['UPLOAD_FOLDER'] + '/kbh.db')
                c = conn.cursor()
                c.execute('SELECT ID, Sudname, Status FROM Sud')
                data = c.fetchall()
                result = []
                for row in data:
                    result.append({"id": row[0], "name": row[1], "brewed": row[2]})
                KBH_VERSION = 2
                return json.dumps(result)
            except Exception as e:
                print (e)
                self.api.notify(headline="Failed to load KHB database", message="ERROR", type="danger")
                return ('', 500)

        finally:
            if conn:
                conn.close()

    def allowed_file(self, filename):
        return '.' in filename and filename.rsplit('.', 1)[1] in set(['sqlite'])

    @route('/upload', methods=['POST'])
    def upload_file(self):
        try:
            if request.method == 'POST':
                file = request.files['file']
                if file and self.allowed_file(file.filename):
                    filename = secure_filename(file.filename)
                    file.save(os.path.join(self.api.app.config['UPLOAD_FOLDER'], "kbh.db"))
                    self.api.notify(headline="Upload Successful", message="The Kleiner Brauhelfer Database was uploaded succesfully")
                    return ('', 204)
                return ('', 404)
        except Exception as e:
            self.api.notify(headline="Upload Failed", message="Failed to upload Kleiner Brauhelfer", type="danger")

            return ('', 500)

    @route('/<int:id>', methods=['POST'])
    def load(self, id):
        bm_recipe_creation = cbpi.get_config_parameter("bm_recipe_creation", None)
        self.api.notify(headline="Braumeister Recipe Upload", message="Activated: %s" % bm_recipe_creation)

        if bm_recipe_creation == "YES" and KBH_VERSION == 2:
            mashstep_type = "BM_MashStep"
            mashinstep_type = "BM_MashInStep"
            mashoutstep_type = "BM_ManualStep"
            boilstep_type = "BM_BoilStep"
            firstwortstep_type = "BM_ManualStep"
            boil_temp = 99 if cbpi.get_config_parameter("unit", "C") == "C" else 210

        else:
            mashstep_type = cbpi.get_config_parameter("step_mash", "MashStep")
            mashinstep_type = cbpi.get_config_parameter("step_mashin", "MashInStep")
            boilstep_type = cbpi.get_config_parameter("step_boil", "BoilStep")
            boil_temp = 100 if cbpi.get_config_parameter("unit", "C") == "C" else 212

        chilstep_type = cbpi.get_config_parameter("step_chil", "ChilStep")
        mash_kettle = cbpi.get_config_parameter("step_mash_kettle", None)
        boil_kettle = cbpi.get_config_parameter("step_boil_kettle", None)

        # READ KBH DATABASE
        Step.delete_all()
        StepView().reset()
        conn = None
        try:
            conn = sqlite3.connect(self.api.app.config['UPLOAD_FOLDER'] + '/kbh.db')
            c = conn.cursor()
            if KBH_VERSION == 1: # kbh database v1
                c.execute('SELECT EinmaischenTemp, Sudname FROM Sud WHERE ID = ?', (id,))
                row = c.fetchone()
                name = row[1]
                self.api.set_config_parameter("brew_name", name)
                Step.insert(**{"name": "MashIn", "type": mashinstep_type, "config": {"kettle": mash_kettle, "temp": row[0]}})

                for row in c.execute('SELECT * FROM Rasten WHERE SudID = ?', (id,)):
                    Step.insert(**{"name": row[5], "type": mashstep_type, "config": {"kettle": mash_kettle, "temp": row[3], "timer": row[4]}})
                Step.insert(**{"name": "Chil", "type": chilstep_type, "config": {"timer": 15}})
                ## Add cooking step
                c.execute('SELECT max(Zeit) FROM Hopfengaben WHERE SudID = ?', (id,))
                row = c.fetchone()
                Step.insert(**{"name": "Boil", "type": boilstep_type, "config": {"kettle": boil_kettle, "temp": boil_temp, "timer": row[0]}})
            else: # KBH Version 2 databse
                c.execute('SELECT Sudname FROM Sud WHERE ID = ?', (id,))
                row = c.fetchone()
                name = row[0]
                self.api.set_config_parameter("brew_name", name)
                c.execute('SELECT Temp FROM Rasten WHERE Typ = 0 AND SudID = ?', (id,))
                row = c.fetchone()
                Step.insert(**{"name": "MashIn", "type": mashinstep_type, "config": {"kettle": mash_kettle, "temp": row[0]}})
                for row in c.execute('SELECT Name, Temp, Dauer FROM Rasten WHERE Typ <> 0 AND SudID = ?', (id,)):
                    Step.insert(**{"name": row[0], "type": mashstep_type, "config": {"kettle": mash_kettle, "temp": row[1], "timer": row[2]}})
                ## Add Step to remove malt pipe and eventually first wort hop step if BM recipe usage is defined
                if bm_recipe_creation == "YES" :
                ## Add Step to remove malt pipe
                    Step.insert(**{
                        "name": "Remove Malt Pipe", 
                        "type": mashoutstep_type, 
                        "config": {
                            "heading": "MashOut Step Completed!",
                            "message": "Please remove Malt Pipe and Sparge. Press Next to continue",
                            "notifyType": "info",
                            "proceed": "Pause",
                            "kettle": mash_kettle
                        }
                    })
                ## Check if first wort step needs to be added
                first_wort_alert = self.getFirstWortAlert(id)
                ## Add boil step
                boil_time_alerts = self.getBoilAlerts(id)
                c.execute('SELECT Kochdauer FROM Sud WHERE ID = ?', (id,))
                row = c.fetchone()
                ## Add boiling step
                Step.insert(**{
                    "name": "Boil",
                    "type": boilstep_type,
                    "config": {
                        "kettle": boil_kettle,
                        "temp": boil_temp,
                        "timer": row[0],
                        ## Beer XML defines additions as the total time spent in boiling,
                        ## CBP defines it as time-until-alert
     
                        ## Also, The model supports five boil-time additions.
                        ## Set the rest to None to signal them being absent
                        "first_wort_hop": 'Yes'  if first_wort_alert == True else 'No',
                        "hop_1": boil_time_alerts[0] if len(boil_time_alerts) >= 1 else None,
                        "hop_2": boil_time_alerts[1] if len(boil_time_alerts) >= 2 else None,
                        "hop_3": boil_time_alerts[2] if len(boil_time_alerts) >= 3 else None,
                        "hop_4": boil_time_alerts[3] if len(boil_time_alerts) >= 4 else None,
                        "hop_5": boil_time_alerts[4] if len(boil_time_alerts) >= 5 else None
                    }
                })
            ## Add Whirlpool step
            Step.insert(**{"name": "Whirlpool", "type": chilstep_type, "config": {"timer": 15}})

            #setBrewName(name)
            self.api.emit("UPDATE_ALL_STEPS", Step.get_all())
            self.api.notify(headline="Recipe %s loaded successfully" % name, message="")
        except Exception as e:
            self.api.notify(headline="Failed to load Recipe", message=e.message, type="danger")
            return ('', 500)
        finally:
            if conn:
                conn.close()
        return ('', 204)

    def getBoilAlerts(self, id):
        alerts = []
        try:
            conn = sqlite3.connect(self.api.app.config['UPLOAD_FOLDER'] + '/kbh.db')
            c = conn.cursor()
            # get the hop addition times
            c.execute('SELECT Zeit FROM Hopfengaben WHERE Vorderwuerze = 0 AND SudID = ?', (id,))
            rows = c.fetchall()

            for row in rows:
                alerts.append(float(row[0]))

            # get any misc additions if available
            c.execute('SELECT Zugabedauer FROM WeitereZutatenGaben WHERE Zeitpunkt = 1 AND SudID = ?', (id,))
            rows = c.fetchall()

            for row in rows:
                alerts.append(float(row[0]))


            ## Dedupe and order the additions by their time, to prevent multiple alerts at the same time
            alerts = sorted(list(set(alerts)))

            ## CBP should have these additions in reverse
            alerts.reverse()
        except Exception as e:
            self.api.notify(headline="Failed to load Recipe", message=e.message, type="danger")
            return ('', 500)
        finally:
            if conn:
                conn.close()

        return alerts


    def getFirstWortAlert(self, id):
        alert = False
        try:
            conn = sqlite3.connect(self.api.app.config['UPLOAD_FOLDER'] + '/kbh.db')
            c = conn.cursor()
            c.execute('SELECT Zeit FROM Hopfengaben WHERE Vorderwuerze = 1 AND SudID = ?', (id,))
            row = c.fetchall()
            if len(row) != 0:
                alert = True
        except Exception as e:
            self.api.notify(headline="Failed to load Recipe", message=e.message, type="danger")
            return ('', 500)
        finally:
            if conn:
                conn.close()
        return alert

@cbpi.initalizer()
def init(cbpi):

    KBH.api = cbpi
    KBH.register(cbpi.app, route_base='/api/kbh')
