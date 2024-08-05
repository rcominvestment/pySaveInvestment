import os
import firebase_admin
from firebase_admin import credentials, db

import threading
import json
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

from io import StringIO

import time
import datetime, pytz

import urllib
import urllib.request
from urllib.error import HTTPError

from bs4 import BeautifulSoup
import json

global_validate = "NO"

class Good():
	def __init__(self):
		self.value = "+"
		self.name = "good"

	def __repr__(self):
		return "<Good(value='%s')>" % (self.value)


class Bad():
	def __init__(self):
		self.value = "-"
		self.name = "bad"

	def __repr__(self):
		return "<Bad(value='%s')>" % (self.value)


class Unknow():
	def __init__(self):
		self.value = "?"
		self.name = "unknow"

	def __repr__(self):
		return "<Unknow(value='%s')>" % (self.value)		


class Investing():
	def __init__(self, uri='https://sslecal2.investing.com?columns=exc_flags,exc_currency,exc_importance,exc_actual,exc_forecast,exc_previous&importance=1,2,3&countries=5&calType=week&timeZone=42&lang=4'):
		self.uri = uri
		self.req = urllib.request.Request(uri)
		self.req.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36')
		self.result = []

	def news(self):
		try:
			response = urllib.request.urlopen(self.req)
			
			html = response.read()
			
			soup = BeautifulSoup(html, "html.parser")

			# Find event item fields
			table = soup.find('table', {"id": "ecEventsTable"})
			tbody = table.find('tbody')
			rows = tbody.findAll('tr')

			count = 0
			
			for i in range(len(rows)):

				try:

					row = rows[i] 

					_datetime = row.attrs['event_timestamp']

					date_split = _datetime.split()

					date_split_a = date_split[0].split('-')

					date_split_b = date_split[1].split(':')

					date_split_y = date_split_a[0]

					date_split_m = date_split_a[1]

					date_split_d = date_split_a[2]

					date_split_h = date_split_b[0]

					date_split_min = date_split_b[1]

					dt = datetime.datetime(int(date_split_y), int(date_split_m), int(date_split_d), int(date_split_h), int(date_split_min), 0, 0)

					_datetime = utc_to_local(dt)

					year = _datetime.year
					
					month = _datetime.month
					
					day = _datetime.day
					
					hour = _datetime.hour
					
					minute = _datetime.minute

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

					_datetime = str(year) + "-" + str(l_month) + "-" + str(l_day) + " " + str(l_hour) + ":" + str(l_minute) + ":" + "00"

					tr = rows[i]
					
					cols = tr.find('td', {"class": "flagCur"})
					flag = cols.find('span')

					_pais = flag.get('title')

					_bull = 0

					impact = tr.find('td', {"class": "sentiment"})
					try:
						bull = impact.findAll('i', {"class": "grayFullBullishIcon"})
						_bull = len(bull)
					except:
						bull = 0

					event = tr.find('td', {"class": "event"})

					_name = event.text.strip()

					_actual = ""

					try:
						actual = tr.find('td', {"class": "act"})
						_actual = actual.text.strip()
					except:
						_actual = ""

					_fore = ""

					try:
						fore = tr.find('td', {"class": "fore"})
						_fore = fore.text.strip()
					except:
						_fore = ""

					_prev = ""

					try:
						prev = tr.find('td', {"class": "prev"})
						_prev = prev.text.strip()
					except:
						_prev = ""					

					_news = {"code": "D"+str(count),
					"timestamp": _datetime,
					"pais": _pais,
					"impacto": _bull,
					"descripcion": _name,
					"actual": _actual,
					"prevision": _fore,
					"anterior": _prev}

					count = count+1

					self.result.append(_news)

				except Exception as e:
					str(e)

		
		except HTTPError as error:
			print ("Oops... Get error HTTP {}".format(error.code))

		return self.result
	
def connectFirebase():

	pre = os.path.dirname(os.path.realpath(__file__))
	fname = 'credencial.json'
	path = os.path.join(pre, fname)

	cred = credentials.Certificate(path)

	if not firebase_admin._apps:
		cred = credentials.Certificate(path)
		firebase_admin.initialize_app(cred, {'databaseURL': 'https://smartinvesment-d35f0-default-rtdb.firebaseio.com/'})

def getData():

	#i = Investing('https://sslecal2.investing.com?columns=exc_flags,exc_currency,exc_importance,exc_actual,exc_forecast,exc_previous&importance=2,3&countries=5&calType=week&timeZone=42&lang=4')
	i = Investing('https://sslecal2.investing.com?columns=exc_flags,exc_currency,exc_importance,exc_actual,exc_forecast,exc_previous&importance=1,2,3&countries=5&calType=week&timeZone=8&lang=4')

	json_data_ = i.news()

	json_data = json.dumps(json_data_, indent=4)

	return json_data

def utc_to_local(utc_dt):
	local_tz = pytz.timezone('US/Eastern')
	local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
	return local_tz.normalize(local_dt)

def setFirebase(df,l_date):

	sheet_name = "Data_Calendar"

	ref = db.reference()

	ruta_destino = 'tb' + sheet_name + "/" + l_date  # Cambia a la colección en tu base de datos

	users_ref = ref.child(ruta_destino)

	for i in range(len(df)):

		l_COUNT = str(df['code'].iloc[i])
		l_TIME = str(df['timestamp'].iloc[i])

		l_COUNTRY = str(df['pais'].iloc[i])
		l_IMPACTO = str(df['impacto'].iloc[i])
		l_DESCRIPCION = str(df['descripcion'].iloc[i])
		l_ACT = str(df['actual'].iloc[i])
		l_PREV = str(df['prevision'].iloc[i])
		l_ANT = str(df['anterior'].iloc[i])
			
		users_ref.child(l_COUNT).set({
			'Fecha' : str(l_TIME),
			'PAIS' : str(l_COUNTRY),
			'IMPACTO' : str(l_IMPACTO),
			'DESC' : str(l_DESCRIPCION),
			'ACT' : str(l_ACT),
			'PREV' : str(l_PREV),
			'ANT' : str(l_ANT)
		})
		

	print("Se guardo en el firebase.")

def setData(name_table):

	# Mayor Performance 

	#sheet_name = "cap_mega,exch_nasd,idx_dji,sh_avgvol_o2000"

	l_Finviz = getData()

	df_finviz = pd.read_json(StringIO(l_Finviz))

	setFirebase(df_finviz,name_table)

def executed():
	
	global global_validate
	
	utc=pytz.UTC

	current_time = datetime.datetime.now(pytz.timezone('US/Eastern'))
	
	year = current_time.year
	
	month = current_time.month
	
	day = current_time.day
	
	numero_semana = datetime.date(year, month, day).isocalendar()[1]
	
	hour = current_time.hour
	
	minute = current_time.minute
	
	second = current_time.second
	
	l_month = ""
	
	if(month <= 9):
		l_month = "0"+str(month)
	else:
		l_month = str(month)

	#name_table = str(year) + "_" + str(l_month)

	name_table = str(numero_semana)

	connectFirebase()
	setData(name_table)

	#global_validate = "SI"

#threading_start = threading.Thread(target=executed, args=())

# Lo lanzo

#threading_start.start()

# Resto de mi código que se ejecutará de forma paralela
while True:
	#if(global_validate == "SI"):
		#break
	executed()
	time.sleep(60)