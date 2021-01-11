
### this is file with mysql database
### 
#  #import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import requests
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
from colorama import *
import datetime
from datetime import *

import time 
print(Fore.YELLOW + "Starting your application For data" + Style.RESET_ALL)


# Use a service account
cred = credentials.Certificate('./service_account_key.json')
firebase_admin.initialize_app(cred)

db = firestore.client()



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
			#print("statslink: /n")

#'seit ir veiktas kautkadas izaminas datubazee no kuras iegust informacioju'
			statsLink = f'https://www.ss.com{str(headLink["src"])}'
			#print(statsLink)
			cookies = dict(sid='000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000')
			r = requests.get(statsLink, cookies=cookies)
			counterLink1 = r.text


			#print(Fore.CYAN + f'counterlink {counterLink1}'+ Style.RESET_ALL)
			counterLink1 = counterLink1.replace('ADS_STAT=[-1,-1,-1,-1,','')
			counterLink1 = counterLink1.replace('ADS_STAT=[0,0,0,0,','')
			counterLink1 = counterLink1.replace('];','')
			counterLink1 = counterLink1.replace('=";open_stat_lnk( \'lv\' );PH_c = "_show_phone(0);";eval(PH_c)','')
			

			counterLink1 = counterLink1.strip()
			uniqueNumber = counterLink1[::-1]
			uniqueNumber = uniqueNumber[0:11:]
			uniqueNumber = uniqueNumber[::-1]
			print(Fore.CYAN + f'counterlink1 {uniqueNumber}'+ Style.RESET_ALL)
			counterLink1 = counterLink1.replace(uniqueNumber,'')
			counterLink1 = counterLink1.replace('OPEN_STAT_LNK="','')
			counterLink1 = counterLink1.strip()
			counterLink1 = int(counterLink1)
			print(counterLink1)


			bildesLinksHTML = page_soup2.findAll("div", {"class": "pic_dv_thumbnail"})

			kopaBildes = ''
			if bildesLinksHTML != []:
				for each in bildesLinksHTML:
					kopaBildes += f", {str(each.a['href'])}"

			ievietosanasDatumsHTML = page_soup2.findAll("td", {"class": "msg_footer"})


			ievietosanasDatums = ievietosanasDatumsHTML[2].text
			ievietosanasDatums = ievietosanasDatums.strip().replace('Datums: ','')

			ievietosanasDatums = ievietosanasDatums[0:10]
			

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
			izlaidumaGadsText = int(izlaidumaGadsText)

			motoraTilpumsCrude = specs[motoraTilpumsIndex:cenaIndex]
			motoraTilpumsText = int(motoraTilpumsCrude.replace('Motora tilpums, cm3:', ''))

			cenaCrude = specs[cenaIndex:]
			cenaText = cenaCrude.replace('Cena:', '')
			cenaText = cenaText.replace(' €', '')
			cenaText = cenaText.replace(' ', '')
			cenaText = float(cenaText)
			cenaText = int(round(cenaText))
			
			specs = specs[:markaIndex] + ''

			statusLink = 1
			datums = date.today().strftime('%d/%m/%Y')
			iforstingnumber=iforstingnumber+1
			stringvalue_doctext1 = f'{uniqueNumber}'
			


			#saliek datus prieks SQLun nosuta uz funkciju kas izpilda SQL sutisanu

			doc_ref = db.collection(u'SSmoto').document(stringvalue_doctext1)
			doc_ref.set({

			    'NO_ID' : uniqueNumber, 
				'DATE' : datums,
 				'STATUS' : 'true',
 				'LINK' : links,
 				'TEXT' : specs,
 				'MARK' : markaText,
 				'MODEL' : modelisText,
 				'YEAR' : izlaidumaGadsText,
 				'SIZE' : motoraTilpumsText,
 				'PRICE' : cenaText,
 				'IMPORT_DATE' : ievietosanasDatums,
 				'UNIQUE_VISITS' : counterLink1,
 				'PICTURE_LINKS' : kopaBildes
			})

			myJSON1 = {
	
				'NO_ID' : uniqueNumber, 
				'DATE' : datums,
 				'STATUS' : 'true',
 				'LINK' : links,
 				'TEXT' : specs,
 				'MARK' : markaText,
 				'MODEL' : modelisText,
 				'YEAR' : izlaidumaGadsText,
 				'SIZE' : motoraTilpumsText,
 				'PRICE' : cenaText,
 				'IMPORT_DATE' : ievietosanasDatums,
 				'UNIQUE_VISITS' : counterLink1,
 				'PICTURE_LINKS' : kopaBildes
			}
	break