import threading

import datetime
import requests
import json
import re
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

import yfinance as yf

import os
import firebase_admin
from firebase_admin import credentials, db

from io import StringIO
import numpy as np

import time
import datetime, pytz

global_validate = "NO"

def connectFirebase():

    pre = os.path.dirname(os.path.realpath(__file__))
    fname = 'credencial.json'
    path = os.path.join(pre, fname)

    cred = credentials.Certificate(path)

    if not firebase_admin._apps:
        cred = credentials.Certificate(path)
        firebase_admin.initialize_app(cred, {'databaseURL': 'https://smartinvesment-d35f0-default-rtdb.firebaseio.com/'})

def getFinviz(sheet_name, order):

    l_index = sheet_name
    
    filtro = l_index

    """
    columnas = "0,1,2,3,4,5,6,7,8,9,\
                10,11,12,13,14,15,16,17,18,19,\
                20,21,22,23,24,25,26,27,28,29,\
                30,31,32,33,34,35,36,37,38,39,\
                40,41,42,43,44,45,46,47,48,49,\
                50,51,52,53,54,55,56,57,58,59,\
                60,61,62,63,64,65,66,67,68,69,\
                70,71,72,73,74,75,76,77,78,79,\
                80,81,82,83,84,85,86,87,88,89,\
                90,91,92,93,94,95,96,97,98,99,\
                100,101,102,103,104,105,106,107,108,109,\
                110,111,112,113,114,115,116,117,118,119,\
                120,121,122,123,124,125,126,127,128"
    """    
    columnas = "1,2,3,4,5,6,68,63,64,67,65,66"
    
    if filtro != "":

        filtro = "&f=" + filtro

    if columnas != "":

        columnas = "&c=" + columnas
    
    if order != "":

        order = "&o=" + order

    atributes = "v=151"+columnas+filtro+order

    token = "8fc1c72b-34c1-41ca-aefd-0725936eb9ef"

    url = "https://elite.finviz.com/export.ashx?"+atributes+"&auth="+token

    response = requests.get(url)

    # Creamos un objeto StringIO para poder leer el contenido del archivo csv como un string
    data = response.content.decode('utf-8')

    data = Convert_JSON(data)

    return data

def Convert_JSON(fileText):
    
    fileText = re.sub(r'"[^"]+"', lambda v: v.group().replace(",", ""), fileText)

    allLines = fileText.split("\r\n")

    header = allLines[0]
    dataLines = allLines[1:]

    fieldNames = header.split(',')

    objList = []

    #count = 0

    #if len(dataLines) < 20:
    count = len(dataLines)
    #else:
    #    count = 20   

    for i in range(count):
        if dataLines[i] == "":
            continue
        obj = {}
        data = dataLines[i].split(',')
        for j in range(len(fieldNames)):
            fieldName = fieldNames[j].lower()
            fieldName = fieldName.replace(" ", "_")
            fieldName = fieldName.replace("\"", "")
            fieldName = fieldName.replace(".", "")
            fieldName = fieldName.replace("/", "_")
            fieldName = fieldName.replace("-", "_")
            fieldName = fieldName.replace('(', "")
            fieldName = fieldName.replace(')', "")
            fieldName = fieldName.replace('%', "PERCENT")
            fieldName = fieldName.upper()
            data_j = data[j].replace("\"", "")
            data_j = data_j.replace("%", "")
            obj[fieldName] = data_j
        objList.append(obj)

    jsonText = json.dumps(objList)

    return jsonText

def setData(name_table):

    # Mayor Performance 

    #sheet_name = "cap_mega,exch_nasd,idx_dji,sh_avgvol_o2000"
    
    order = "-earningsdate"

    sheet_name = "cap_1to,earningsdate_nextweek|prevweek|thisweek,sh_avgvol_o1000"

    l_Finviz = getFinviz(sheet_name,order)

    df_finviz = pd.read_json(StringIO(l_Finviz))

    setFirebase(df_finviz,name_table,"All")

    sheet_name = "cap_1to,earningsdate_today,sh_avgvol_o1000"

    l_Finviz = getFinviz(sheet_name,order)

    df_finviz = pd.read_json(StringIO(l_Finviz))

    setFirebase(df_finviz,name_table,"Today")



def setFirebase(df,l_date,l_tipo):

    sheet_name = "Data_Earning"

    ref = db.reference()

    ruta_destino = 'tb' + sheet_name + "/" + l_date + "/" + l_tipo  # Cambia a la colección en tu base de datos

    users_ref = ref.child(ruta_destino)

    for i in range(len(df)):

        l_TICKER = str(df['TICKER'].iloc[i])

        l_TICKER = l_TICKER.replace(".", "_")
        l_TICKER = l_TICKER.replace("/", "_")

        l_COMPANY = str(df['COMPANY'].iloc[i])
        l_SECTOR = str(df['SECTOR'].iloc[i])
        l_INDUSTRY = str(df['INDUSTRY'].iloc[i])
        l_COUNTRY = str(df['COUNTRY'].iloc[i])
        l_MARKET_CAP = str(df['MARKET_CAP'].iloc[i])
        l_EARNINGS_DATE = str(df['EARNINGS_DATE'].iloc[i])
        l_AVERAGE_VOLUME = str(df['AVERAGE_VOLUME'].iloc[i])
        l_RELATIVE_VOLUME = str(df['RELATIVE_VOLUME'].iloc[i])
        l_VOLUME = str(df['VOLUME'].iloc[i])
        l_PRICE = str(df['PRICE'].iloc[i])
        l_CHANGE = str(df['CHANGE'].iloc[i])
            
        users_ref.child(l_TICKER).set({
            'COMPANY' : str(l_COMPANY),
            'SECTOR' : str(l_SECTOR),
            'INDUSTRY' : str(l_INDUSTRY),
            'COUNTRY' : str(l_COUNTRY),
            'MARKET_CAP' : str(l_MARKET_CAP),
            'EARNINGS_DATE' : str(l_EARNINGS_DATE),
            'AVERAGE_VOLUME' : str(l_AVERAGE_VOLUME),
            'RELATIVE_VOLUME' : str(l_RELATIVE_VOLUME),
            'VOLUME' : str(l_VOLUME),
            'PRICE' : str(l_PRICE),
            'CHANGE' : str(l_CHANGE),
        })
        

    print("Se guardo en el firebase.")

def es_multiplo(numero, multiplo):
    # Si el residuo es 0, es múltiplo
    if numero % multiplo == 0:
        return True
    else:
        return False

def executed():

    global global_validate

    utc=pytz.UTC

    current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))

    year = current_time.year
    
    month = current_time.month

    day = current_time.day

    hour = current_time.hour

    minute = current_time.minute

    second = current_time.second

    l_month = ""
    
    if(month <= 9):
        l_month = "0"+str(month)
    else:
        l_month = str(month)
    
    l_day = ""
    
    if(day <= 9):
        l_day = "0"+str(day)
    else:
        l_day = str(day)

    name_table = str(year) + "_" + str(l_month) + "_" + str(l_day)
    
    connectFirebase()
    setData(name_table)

    global_validate = "SI"

threading_start = threading.Thread(target=executed, args=())

# Lo lanzo

threading_start.start()

# Resto de mi código que se ejecutará de forma paralela
while True:
    if(global_validate == "SI"):
        break
    time.sleep(1)