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

def setData(name_table,l_data):

    # Mayor Performance 

    order = "-marketcap"

    sheet_name = "cap_1to,sh_avgvol_o1000"

    #sheet_name = "cap_mega,exch_nasd,idx_dji,sh_avgvol_o2000"

    l_Finviz = getFinviz(sheet_name,order)

    df_finviz = pd.read_json(StringIO(l_Finviz))

    setFirebase(df_finviz,name_table,l_data)

def setFirebase(df,l_date,l_data):

    sheet_name = "Data_Oportunity_Minutes"

    ref = db.reference()

    l_TICKER = ""

    for i in range(len(df)):
        if l_TICKER == "":
            l_TICKER = str(df['TICKER'].iloc[i]) 
        else:
            l_TICKER = l_TICKER + " " + str(df['TICKER'].iloc[i])
    
    data_5min = yf.download(l_TICKER, start=l_data, end=None, interval="15m", group_by='tickers')

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

        try:

            data_ticker = data_5min[l_TICKER]

            data = data_ticker

            df_2 = pd.DataFrame(data).transpose()

            df_2 = pd.DataFrame(df_2).transpose().reset_index()

            df_2['SMA7'] = df_2['Close'].rolling(window=7).mean()

            df_2['SMA7_Crossed_Above'] = np.where((df_2['Close'] > df_2['SMA7']) & (df_2['Close'].shift(1) < df_2['SMA7'].shift(1)), 'X', '')

            df_2['SMA7_Crossed_Below'] = np.where((df_2['Close'] < df_2['SMA7']) & (df_2['Close'].shift(1) > df_2['SMA7'].shift(1)), 'X', '')

            df_2['SMA7_Crossed_Complete'] = np.where((df_2['SMA7_Crossed_Above'] == 'X') & (df_2['SMA7_Crossed_Below'].shift(1) == 'X'), 'X', '')

            df_2['SMA50'] = df_2['Close'].rolling(window=50).mean()

            df_2['SMA50_UP'] = df_2.apply(lambda row: 'X' if row['Close'] > row['SMA50'] else '', axis=1)

            df_2['SMA50_Crossed_Above'] = np.where((df_2['Close'] > df_2['SMA50']) & (df_2['Close'].shift(1) < df_2['SMA50'].shift(1)), 'X', '')

            df_2['SMA50_Crossed_Below'] = np.where((df_2['Close'] < df_2['SMA50']) & (df_2['Close'].shift(1) > df_2['SMA50'].shift(1)), 'X', '')

            df_2['SMA50_Crossed_Complete'] = np.where((df_2['SMA50_Crossed_Above'] == 'X') & (df_2['SMA50_Crossed_Below'].shift(1) == 'X'), 'X', '')

            df_2['SMA200'] = df_2['Close'].rolling(window=200).mean()

            df_2['SMA200_UP'] = df_2.apply(lambda row: 'X' if row['Close'] > row['SMA200'] else '', axis=1)

            df_2['SMA200_Crossed_Above'] = np.where((df_2['Close'] > df_2['SMA200']) & (df_2['Close'].shift(1) < df_2['SMA200'].shift(1)), 'X', '')

            df_2['SMA200_Crossed_Below'] = np.where((df_2['Close'] < df_2['SMA200']) & (df_2['Close'].shift(1) > df_2['SMA200'].shift(1)), 'X', '')

            df_2['SMA200_Crossed_Complete'] = np.where((df_2['SMA200_Crossed_Above'] == 'X') & (df_2['SMA200_Crossed_Below'].shift(1) == 'X'), 'X', '')

            df_2 = df_2[-1:]

            #p_date = df_2['Datetime'].iloc[0]
            p_open = df_2['Open'].iloc[0]
            p_high = df_2['High'].iloc[0]
            p_low = df_2['Low'].iloc[0]
            p_close = df_2['Close'].iloc[0]
            p_adj_close = df_2['Adj Close'].iloc[0]
            p_volume = df_2['Volume'].iloc[0]

            p_sm7 = df_2['SMA7'].iloc[0]
            p_sm7_c_a = df_2['SMA7_Crossed_Above'].iloc[0]
            p_sm7_c_b = df_2['SMA7_Crossed_Below'].iloc[0]
            p_sm7_c_c = df_2['SMA7_Crossed_Complete'].iloc[0]

            p_sm50 = df_2['SMA50'].iloc[0]
            p_sm50_up = df_2['SMA50_UP'].iloc[0]
            p_sm50_c_a = df_2['SMA50_Crossed_Above'].iloc[0]
            p_sm50_c_b = df_2['SMA50_Crossed_Below'].iloc[0]
            p_sm50_c_c = df_2['SMA50_Crossed_Complete'].iloc[0]

            p_sm200 = df_2['SMA200'].iloc[0]
            p_sm200_up = df_2['SMA200_UP'].iloc[0]
            p_sm200_c_a = df_2['SMA200_Crossed_Above'].iloc[0]
            p_sm200_c_b = df_2['SMA200_Crossed_Below'].iloc[0]
            p_sm200_c_c = df_2['SMA200_Crossed_Complete'].iloc[0]

            ruta_destino = 'tb' + sheet_name + "/" + l_date + "/" + "SMA7"  # Cambia a la colección en tu base de datos

            users_ref = ref.child(ruta_destino)

            if(p_sm7_c_a == "X"):

                ruta_destino = 'tb' + sheet_name + "/" + l_date + "/" + "SMA7" + "/" + "Above"  # Cambia a la colección en tu base de datos

                users_ref = ref.child(ruta_destino)
                    
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

                    #'Date' : str(p_date),
                    'Open' : str(p_open),
                    'High' : str(p_high),
                    'Low' : str(p_low),
                    'Close' : str(p_close),
                    'Adj_Close' : str(p_adj_close),
                    'Volume' : str(p_volume),

                    'SMA7' : str(p_sm7),
                    'SMA7_Crossed_Above' : str(p_sm7_c_a),
                    'SMA7_Crossed_Below' : str(p_sm7_c_b),
                    'SMA7_Crossed_Complete' : str(p_sm7_c_c),

                    'SMA50' : str(p_sm50),
                    'SMA50_UP' : str(p_sm50_up),
                    'SMA50_Crossed_Above' : str(p_sm50_c_a),
                    'SMA50_Crossed_Below' : str(p_sm50_c_b),
                    'SMA50_Crossed_Complete' : str(p_sm50_c_c),
                    
                    'SMA200' : str(p_sm200),
                    'SMA200_UP' : str(p_sm200_up),
                    'SMA200_Crossed_Above' : str(p_sm200_c_a),
                    'SMA200_Crossed_Below' : str(p_sm200_c_b),
                    'SMA200_Crossed_Complete' : str(p_sm200_c_c)
                })
            
            if(p_sm7_c_b == "X"):

                ruta_destino = 'tb' + sheet_name + "/" + l_date + "/" + "SMA7" + "/" + "Below"  # Cambia a la colección en tu base de datos

                users_ref = ref.child(ruta_destino)
                    
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

                    #'Date' : str(p_date),
                    'Open' : str(p_open),
                    'High' : str(p_high),
                    'Low' : str(p_low),
                    'Close' : str(p_close),
                    'Adj_Close' : str(p_adj_close),
                    'Volume' : str(p_volume),

                    'SMA7' : str(p_sm7),
                    'SMA7_Crossed_Above' : str(p_sm7_c_a),
                    'SMA7_Crossed_Below' : str(p_sm7_c_b),
                    'SMA7_Crossed_Complete' : str(p_sm7_c_c),

                    'SMA50' : str(p_sm50),
                    'SMA50_UP' : str(p_sm50_up),
                    'SMA50_Crossed_Above' : str(p_sm50_c_a),
                    'SMA50_Crossed_Below' : str(p_sm50_c_b),
                    'SMA50_Crossed_Complete' : str(p_sm50_c_c),
                    
                    'SMA200' : str(p_sm200),
                    'SMA200_UP' : str(p_sm200_up),
                    'SMA200_Crossed_Above' : str(p_sm200_c_a),
                    'SMA200_Crossed_Below' : str(p_sm200_c_b),
                    'SMA200_Crossed_Complete' : str(p_sm200_c_c)
                })

            if(p_sm50_c_a == "X"):

                ruta_destino = 'tb' + sheet_name + "/" + l_date + "/" + "SMA50" + "/" + "Above"  # Cambia a la colección en tu base de datos

                users_ref = ref.child(ruta_destino)
                    
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

                    #'Date' : str(p_date),
                    'Open' : str(p_open),
                    'High' : str(p_high),
                    'Low' : str(p_low),
                    'Close' : str(p_close),
                    'Adj_Close' : str(p_adj_close),
                    'Volume' : str(p_volume),

                    'SMA7' : str(p_sm7),
                    'SMA7_Crossed_Above' : str(p_sm7_c_a),
                    'SMA7_Crossed_Below' : str(p_sm7_c_b),
                    'SMA7_Crossed_Complete' : str(p_sm7_c_c),

                    'SMA50' : str(p_sm50),
                    'SMA50_UP' : str(p_sm50_up),
                    'SMA50_Crossed_Above' : str(p_sm50_c_a),
                    'SMA50_Crossed_Below' : str(p_sm50_c_b),
                    'SMA50_Crossed_Complete' : str(p_sm50_c_c),
                    
                    'SMA200' : str(p_sm200),
                    'SMA200_UP' : str(p_sm200_up),
                    'SMA200_Crossed_Above' : str(p_sm200_c_a),
                    'SMA200_Crossed_Below' : str(p_sm200_c_b),
                    'SMA200_Crossed_Complete' : str(p_sm200_c_c)
                })
            
            if(p_sm50_c_b == "X"):

                ruta_destino = 'tb' + sheet_name + "/" + l_date + "/" + "SMA50" + "/" + "Below"  # Cambia a la colección en tu base de datos

                users_ref = ref.child(ruta_destino)
                    
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

                    #'Date' : str(p_date),
                    'Open' : str(p_open),
                    'High' : str(p_high),
                    'Low' : str(p_low),
                    'Close' : str(p_close),
                    'Adj_Close' : str(p_adj_close),
                    'Volume' : str(p_volume),

                    'SMA7' : str(p_sm7),
                    'SMA7_Crossed_Above' : str(p_sm7_c_a),
                    'SMA7_Crossed_Below' : str(p_sm7_c_b),
                    'SMA7_Crossed_Complete' : str(p_sm7_c_c),

                    'SMA50' : str(p_sm50),
                    'SMA50_UP' : str(p_sm50_up),
                    'SMA50_Crossed_Above' : str(p_sm50_c_a),
                    'SMA50_Crossed_Below' : str(p_sm50_c_b),
                    'SMA50_Crossed_Complete' : str(p_sm50_c_c),
                    
                    'SMA200' : str(p_sm200),
                    'SMA200_UP' : str(p_sm200_up),
                    'SMA200_Crossed_Above' : str(p_sm200_c_a),
                    'SMA200_Crossed_Below' : str(p_sm200_c_b),
                    'SMA200_Crossed_Complete' : str(p_sm200_c_c)
                })
            
            if(p_sm200_c_a == "X"):

                ruta_destino = 'tb' + sheet_name + "/" + l_date + "/" + "SMA200" + "/" + "Above"  # Cambia a la colección en tu base de datos

                users_ref = ref.child(ruta_destino)
                    
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
                    
                    #'Date' : str(p_date),
                    'Open' : str(p_open),
                    'High' : str(p_high),
                    'Low' : str(p_low),
                    'Close' : str(p_close),
                    'Adj_Close' : str(p_adj_close),
                    'Volume' : str(p_volume),

                    'SMA7' : str(p_sm7),
                    'SMA7_Crossed_Above' : str(p_sm7_c_a),
                    'SMA7_Crossed_Below' : str(p_sm7_c_b),
                    'SMA7_Crossed_Complete' : str(p_sm7_c_c),

                    'SMA50' : str(p_sm50),
                    'SMA50_UP' : str(p_sm50_up),
                    'SMA50_Crossed_Above' : str(p_sm50_c_a),
                    'SMA50_Crossed_Below' : str(p_sm50_c_b),
                    'SMA50_Crossed_Complete' : str(p_sm50_c_c),
                    
                    'SMA200' : str(p_sm200),
                    'SMA200_UP' : str(p_sm200_up),
                    'SMA200_Crossed_Above' : str(p_sm200_c_a),
                    'SMA200_Crossed_Below' : str(p_sm200_c_b),
                    'SMA200_Crossed_Complete' : str(p_sm200_c_c)
                })
            
            if(p_sm200_c_b == "X"):

                ruta_destino = 'tb' + sheet_name + "/" + l_date + "/" + "SMA200" + "/" + "Below"  # Cambia a la colección en tu base de datos

                users_ref = ref.child(ruta_destino)
                    
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

                    #'Date' : str(p_date),
                    'Open' : str(p_open),
                    'High' : str(p_high),
                    'Low' : str(p_low),
                    'Close' : str(p_close),
                    'Adj_Close' : str(p_adj_close),
                    'Volume' : str(p_volume),

                    'SMA7' : str(p_sm7),
                    'SMA7_Crossed_Above' : str(p_sm7_c_a),
                    'SMA7_Crossed_Below' : str(p_sm7_c_b),
                    'SMA7_Crossed_Complete' : str(p_sm7_c_c),

                    'SMA50' : str(p_sm50),
                    'SMA50_UP' : str(p_sm50_up),
                    'SMA50_Crossed_Above' : str(p_sm50_c_a),
                    'SMA50_Crossed_Below' : str(p_sm50_c_b),
                    'SMA50_Crossed_Complete' : str(p_sm50_c_c),
                    
                    'SMA200' : str(p_sm200),
                    'SMA200_UP' : str(p_sm200_up),
                    'SMA200_Crossed_Above' : str(p_sm200_c_a),
                    'SMA200_Crossed_Below' : str(p_sm200_c_b),
                    'SMA200_Crossed_Complete' : str(p_sm200_c_c)
                })
        except Exception as e:
            print("The error is: ",e)
    print("Se guardo en el firebase.")

def es_multiplo(numero, multiplo):
    # Si el residuo es 0, es múltiplo

    residuo = numero % multiplo

    if residuo == 0:
        return True
    else:
        return False


while True:

    utc=pytz.UTC

    vela_minutos = 15

    current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))

    year = current_time.year
    
    month = current_time.month

    day = current_time.day

    hour = current_time.hour

    minute = current_time.minute

    second = current_time.second

    time_now = datetime.datetime(year,month,day,hour,minute,second,0)

    time_now = utc.localize(time_now)

    # datetime(year, month, day, hour, minute, second, microsecond)
    open_time = datetime.datetime(year, month, day, 9, 30, 00, 0)

    open_time = utc.localize(open_time)

    close_time = datetime.datetime(year, month, day, 16, 00, 00, 0)

    close_time = utc.localize(close_time)

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

    l_hour = ""
    
    if(hour <= 9):
        l_hour = "0"+str(hour)
    else:
        l_hour = str(hour)
    
    l_minute = ""
    
    if(minute <= 9):
        l_minute = "0"+str(minute)
    else:
        l_minute = str(minute)

    a_month = month-1

    l_month_a = ""
    
    if(a_month <= 9):
        l_month_a = "0"+str(a_month)
    else:
        l_month_a = str(a_month)

    name_table = str(year) + "_" + str(l_month) + "_" + str(l_day) + "_" + str(l_hour) + "_" + str(l_minute)

    l_data = str(year) + "-" + str(l_month_a) + "-" + str(l_day)

    #print(open_time, close_time, time_now)

    """name_table = str(year) + "_" + str(l_month) + "_" + str(l_day) + "_" + str("11") + "_" + str("15")
    connectFirebase()
    setData(name_table)
    break"""

    if (time_now >= open_time):
        if(time_now <= close_time):
            if(time_now != open_time):
                minute = int(minute)
                vela_minutos = int(vela_minutos)
                if es_multiplo(minute, vela_minutos):
                    if(second == 0):
                        connectFirebase()
                        setData(name_table,l_data)
        else:
            break
    time.sleep(1)