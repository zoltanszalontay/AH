#!/usr/bin/python
# v1.0
import datetime as dt
import logging
import websocket
import json
import pyodbc as odbc
from pushover import Pushover

sql = {}    # SQL configuration (server, database, table)
euis = {}   # Sensor ID (EUI) - Sensor type pairs

class sensor_data:
    def __init__(self, time, eui, value, unit, sensor_type_id, sensor_model):
        self.time = time
        self.eui = eui
        self.value = float(value)
        self.unit = unit
        self.sensor_type_id = sensor_type_id
        self.sensor_model = int(sensor_model)

sdlist = []
sd_temp = sensor_data 
sd_power = sensor_data

def on_message(ws, message):
    #log.info("on_message()")

    time = gettime()
    
    jsondata = json.loads(message)
    decode(time, jsondata["EUI"], jsondata["data"])
    
    # Insert sensor data to SQL table
    for i in range(len(sdlist)):
        log.info("|| %-30s | %-20s || %s, %s, %8.2f, %-4s, %2d ||" % (sdlist[i].sensor_model, jsondata["data"], sdlist[i].time, sdlist[i].eui, float(sdlist[i].value), sdlist[i].unit, sdlist[i].sensor_type_id))
        write_sql(sdlist[i].time, sdlist[i].eui, round(float(sdlist[i].value), 2), sdlist[i].unit, sdlist[i].sensor_type_id)

def decode(time, eui, message):
    #log.info("decode()")
    
    # Reinitialize sensor list
    del sdlist[:]

    sd = sensor_data("", "", 0.0, "", "", "")
    sd.time = time
    sd.eui = eui
    
    # Decode supported sensors only (Rising HF, Digimondo and LS-11x)
    if (euis[eui] == "Rising HF"):
        # log.info("Rising HF - Temperature | %s | %s ", eui, message)
        # Decoding temperature
        temp = int(message[4:6] + message[2:4], 16) * 175.72 / 65536 - 46.85
        sdlist.append(sensor_data(sd.time, sd.eui, float(temp), "°C", 1, "Rising HF - Temperature"))

        # Decode humidity
        # log.info("Rising HF - Humidity | %s | %s ", eui, message)
        hum = int(message[6:8], 16)*125/2**8 - 6
        sdlist.append(sensor_data(sd.time, sd.eui, float(hum), "%RH", 2, "Rising HF - Humidity"))

        # Decode battery percentage
        # log.info("Rising HF - Battery | %s | %s ", eui, message)
        battery = (int(message[16:], 16) + 150) * 0.01 / 3.64
        sdlist.append(sensor_data(sd.time, sd.eui, float(battery), "%", 3, "Rising HF - Battery"))

    elif (euis[eui] == "Digimondo"):
        # log.info("Digimondo - Power consumption | %s | %s ", eui, message)
        # Decode power consumption
        consumption = int(message[2:8], 16)
        sdlist.append(sensor_data(sd.time, sd.eui, float(consumption), "kWh", 4, "Digimondo - Power consumption"))

    elif (euis[eui] == "LS-11x"):
        # log.info("LS-11x - Temperature | %s | %s ", eui, message)
        # Decoding temperature
        temp = int(message[2:6], 16) / 100
        sdlist.append(sensor_data(sd.time, sd.eui, float(temp), "°C", 1, "LS-11x - Temperature"))

        # log.info("LS-11x - Humidity | %s | %s ", eui, message)
        # Decoding temperature
        hum = int(message[6:10], 16) / 100
        sdlist.append(sensor_data(sd.time, sd.eui, float(hum), "%RH", 2, "LS-11x - Humidity"))

        # log.info("LS-11x - Gas density | %s | %s ", eui, message)
        # Decoding Gas density
        dev_type = ("CO2", "CO", "PM2.5")
        unit = dev_type[int(message[0:2], 16) - 1]
        co2 = int(message[10:], 16)
        sdlist.append(sensor_data(sd.time, sd.eui, float(co2), unit, 5, "LS-11x - Gas density"))

def write_sql(time, eui, value, unit, sensor_type_id):
    # log.info("write_sql()")
    sql_cmd = 'INSERT INTO ' + '\"' + sql['table'] + '\" (Time, \" EUI\",  \" Value\", \" Unit\", \"Sensor_type_ID\") VALUES (?, ?, ?, ?, ?)'
    # log.info("SQL command: %s %s, %s, %s, %s, %s", sql_cmd, time, eui, value, unit, sensor_type_id)
    cursor.execute(sql_cmd, time, eui, value, unit, sensor_type_id)
    sql_conn.commit()

def gettime():
    #log.info("gettime()")
    now = dt.datetime.now()
    time = str(now.day).rjust(2, '0') + "/" + str(now.month).rjust(2, '0') + "/" + str(now.year).rjust(2, '0') + " " + str(now.hour).rjust(2, '0') + ":" + str(now.minute).rjust(2, '0')
    return time

def on_open(ws):
    log.info("Web Socket open")
    push_notification("Web socket open")

def on_close(ws):
    log.info("Web Socket closed")
    push_notification("Web socket closed")
    exit(1)

def on_error(ws, error):
    log.info("Web Socket error")
    push_notification("Web socket error")
    print(error)

def write_sql_info():
    with open('sqlinfo.txt', 'w') as f:
        f.write(json.dumps(sql))
    f.close()

def write_sensor_list():
    with open('sensors.txt', 'w') as f:
        f.write(json.dumps(euis))
    f.close()

def push_notification(message):
    msg = po.msg(message)
    msg.set("AH IoT", "Data streamer")
    po.send(msg)

if __name__ == "__main__":

    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)
    logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %H:%M')

    # Start push notification
    po = Pushover("aj2rn8589hxxcjj8aoqayoxp8cffg6") # application key
    po.user("uenw7rc79czvecd3fsjrms9bm32ucm")       # user key

    # Read SQL connection data
    with open('sqlinfo.txt', 'r') as f:
        sql = json.loads(f.read())
    f.close()

    # Read EUI list
    with open('sensors.txt', 'r') as f:
        euis = json.loads(f.read())
    f.close()

    sql_conn = odbc.connect(        \
        'DRIVER={SQL Server};'
        + 'SERVER=' + sql["server"] + ';'
        + 'DATABASE=' + sql["database"] + ';'
        + 'Trusted_Connection=yes;'
    )
    cursor = sql_conn.cursor()

    # Read URL containing web socket app hash
    f = open('app_url.txt', 'r')
    app_url = f.readline()
    f.close()

    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(app_url,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
