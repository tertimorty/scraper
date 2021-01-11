
### this is file with mysql database
### 
import firebase_admin
import mysql.connector
from firebase_admin import credentials
from firebase_admin import firestore
import requests
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
from colorama import *
import datetime
from datetime import *

import time 

mydb = mysql.connector.connect(
			host="127.0.0.2",
			user="root",
			password="Lazanjavirs1",
			database="testschema"
			)
print(Fore.YELLOW + "Starting your application For data" + Style.RESET_ALL)





iforstingnumber=1

# starting data mining from web for first time
while True:

	my_url = f'https://www.ss.com/lv/transport/moto-transport/motorcycles/honda/sell/'
	
	uClient = uReq(my_url) 
	page_html = uClient.read()
	uClient.close()
	page_soup = soup(page_html, "html.parser")
	#print(f'{page_soup}')
	
	pageCount = page_soup.find("a", {"class": "navi"})
	pageCountLink = str(pageCount['href'])
	#print(f'pagecount link {pageCountLink}')
	pageCount = pageCountLink.replace('/lv/transport/moto-transport/motorcycles/honda/sell/page','')
	#print(f'page count {pageCount}')
	pageCount = int(pageCount.replace('.html','').strip())
	print(f'pageCount {pageCount}')
	superlist = []
	z = 1
	while z <= pageCount: #pageCount
		
		my_url = f'https://www.ss.com/lv/transport/moto-transport/motorcycles/honda/sell/page{z}.html'
		uClient = uReq(my_url) 
		page_html = uClient.read()
		uClient.close()
		page_soup = soup(page_html, "html.parser")
		containers = page_soup.findAll("a", {"class": "am"})
	
		print(Fore.GREEN + f'stradaju pie lapas:{z} no {pageCount} ar {len(containers)} sludinajumiem' + Style.RESET_ALL)
		z += 1
		
		skaitsNo = len(containers)
		for num, each in enumerate(containers , start=1):
			if num > skaitsNo:  
				break

			links = f'https://www.ss.com{each["href"]}'
			uClient2 = uReq(links)
			page2_html = uClient2.read()
			uClient2.close()
			page_soup2 = soup(page2_html, "html.parser")
			
			headLink = page_soup2.find('script',{'id':'contacts_js'})
			#print(f"statslink: {headLink} /n")

#'seit ir veiktas kautkadas izaminas datubazee no kuras iegust informacioju'
			statsLink = f'https://www.ss.com{str(headLink["src"])}'
			#print(f"clean stats link: {statsLink} /n")
			cookies = dict(sid='000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000')
			r = requests.get(statsLink, cookies=cookies)
			counterLink1 = r.text


			#print(Fore.CYAN + f'counterlink {counterLink1}'+ Style.RESET_ALL)
			
			counterList_2 = counterLink1.split(";")	
			counter_for_link = counterLink1.index(';')
			#print(f"counter is: {counter_for_link}")
			counterLink1 = counterLink1[ :counter_for_link]
			counterList = counterLink1.split(",")
			counterName = counterList[4]
			counterNumber = int(counterName[0:-1])
			counter_name_2 = counterList_2[1]
			counter_name_2 = counter_name_2.replace('\r\n','')
			counter_name_2 = counter_name_2.replace('OPEN_STAT_LNK="','')
			counter_name_2 = counter_name_2.replace('="','')
			uniqueNumber = counter_name_2
			#print(Fore.GREEN + f'counter_name_2 {counter_name_2}'+ Style.RESET_ALL)



			bildesLinksHTML = page_soup2.findAll("div", {"class": "pic_dv_thumbnail"})

			kopaBildes = ''
			if bildesLinksHTML != []:
				for each in bildesLinksHTML:
					kopaBildes += f", {str(each.a['href'])}"

			ievietosanasDatumsHTML = page_soup2.findAll("td", {"class": "msg_footer"})


			ievietosanasDatums = ievietosanasDatumsHTML[2].text
			ievietosanasDatums = ievietosanasDatums.strip().replace('Datums: ','')

			ievietosanasDatums = ievietosanasDatums[0:10]
			print(ievietosanasDatums)

			specifics = page_soup2.find("div", {"id": "msg_div_msg"})

			specs = specifics.text
			specs = specs.strip()
			specs = specs.replace('Aprēķināt apdrošināšanu OCTA.LV','')
			markaIndex = specs.rfind('Marka')
			modelisIndex = specs.rfind('Modelis')
			izlaidumaGadsIndex = specs.rfind('Izlaiduma gads')
			motoraTilpumsIndex = specs.rfind('Motora tilpums, cm3')
			cenaIndex = specs.rfind('Cena')

			markaCrude = specs[markaIndex:modelisIndex]
			markaText = markaCrude.replace('Marka:', '')

			modelisCrude = specs[modelisIndex:izlaidumaGadsIndex]
			modelisText = modelisCrude.replace('Modelis:', '')

			izlaidumaGadsCrude = specs[izlaidumaGadsIndex:motoraTilpumsIndex]
			izlaidumaGadsText = izlaidumaGadsCrude.replace('Izlaiduma gads:', '')
			izlaidumaGadsInt = int(izlaidumaGadsText)

			motoraTilpumsCrude = specs[motoraTilpumsIndex:cenaIndex]
			motoraTilpumsText = int(motoraTilpumsCrude.replace('Motora tilpums, cm3:', ''))

			cenaCrude = specs[cenaIndex:]
			cenaText = cenaCrude.replace('Cena:', '')
			cenaText = cenaText.replace(' €Aprēķināt apdrošināšanu', '')
			cenaText = cenaText.replace(' ', '')
			cenaText = float(cenaText)
			cenaText = int(round(cenaText))
			
			specs = specs[:markaIndex] + ''
			specs_short = specs[:295]
			print(f"pic: {len(kopaBildes)}")

			statusLink = 1
			datums = date.today().strftime('%Y-%m-%d')
			iforstingnumber=iforstingnumber+1
			stringvalue_doctext1 = f'{uniqueNumber}'
			


			#saliek datus prieks SQLun nosuta uz funkciju kas izpilda SQL sutisanu

			

			myJSON1 = {
	
				'NO_ID' : uniqueNumber, 
				'DATE' : datums,
 				'STATUS' : 'true',
 				'LINK' : links,
 				'TEXT' : specs,
 				'MARK' : markaText,
 				'MODEL' : modelisText,
 				'YEAR' : izlaidumaGadsInt,
 				'SIZE' : motoraTilpumsText,
 				'PRICE' : cenaText,
 				'IMPORT_DATE' : ievietosanasDatums,
 				'UNIQUE_VISITS' : counterNumber,
 				'PICTURE_LINKS' : kopaBildes
			}
			

			

			mycursor = mydb.cursor()
		
			sql = f'INSERT INTO Jamaha (NO_ID, DATE, MARK) VALUES ("{uniqueNumber}","{datums}","Jamaha");'
			print(f"{sql}")
			mycursor.execute(sql)
			mydb.commit()
			print(mycursor.rowcount, "record inserted.")
			


			#print(myJSON1)
	break