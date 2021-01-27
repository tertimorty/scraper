### this file collects data to local mysql database in seperate tables ### 
import mysql.connector
import requests
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
from colorama import *
from datetime import *
import time 
import test # test is the file where login information for database is kept
print(test.user)
mydb = mysql.connector.connect(
			host=test.host,
			user=test.user,
			password=test.password,
			database=test.database
			)
print(Fore.YELLOW + "Starting your application For data" + Style.RESET_ALL)
motonamelist = ['yamaha','honda']

	
# starting data mining from web for first time
for each in motonamelist:
	print(f'{each}')
	motoname = each

	my_url = f'https://www.ss.com/lv/transport/moto-transport/motorcycles/{motoname}/sell/'
	print(my_url)
	uClient =  uReq(my_url) 
	page_html = uClient.read()
	uClient.close()
	page_soup = soup(page_html, "html.parser")
	#print(f'{page_soup}')
	
	pageCount = page_soup.find("a", {"class": "navi"})
	pageCountLink = str(pageCount['href'])
	#print(f'pagecount link {pageCountLink}')
	pageCount = pageCountLink.replace(f'/lv/transport/moto-transport/motorcycles/{motoname}/sell/page','')
	#print(f'page count {pageCount}')
	pageCount = int(pageCount.replace('.html','').strip())
	print(f'pageCount {pageCount}')
	superlist = []
	z = 1
	while z <= pageCount: #pageCount
		
		my_url = f'https://www.ss.com/lv/transport/moto-transport/motorcycles/{motoname}/sell/page{z}.html'
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

			#this is workaround to get unique visits information, since it needs cookie
			statsLink = f'https://www.ss.com{str(headLink["src"])}'
			cookies = dict(sid='000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000')
			r = requests.get(statsLink, cookies=cookies)
			counterLink1 = r.text
			#extracting and formating unique visits information
			counterList_2 = counterLink1.split(";")	
			counter_for_link = counterLink1.index(';')
			counterLink1 = counterLink1[ :counter_for_link]
			counterList = counterLink1.split(",")
			counterName = counterList[4]
			counterNumber = int(counterName[0:-1])
			counter_name_2 = counterList_2[1]
			counter_name_2 = counter_name_2.replace('\r\n','')
			counter_name_2 = counter_name_2.replace('OPEN_STAT_LNK="','')
			counter_name_2 = counter_name_2.replace('="','')
			uniqueNumber = counter_name_2

			# extracting pictures links information and putting all links in one string
			bildesLinksHTML = page_soup2.findAll("div", {"class": "pic_dv_thumbnail"})

			kopaBildes = ''
			if bildesLinksHTML != []:
				for each in bildesLinksHTML:
					kopaBildes += f", {str(each.a['href'])}"

			#extracting date when informatin is inserted in webpage
			ievietosanasDatumsHTML = page_soup2.findAll("td", {"class": "msg_footer"})
			ievietosanasDatums = ievietosanasDatumsHTML[2].text
			ievietosanasDatums = ievietosanasDatums.strip().replace('Datums: ','')
			ievietosanasDatums = ievietosanasDatums[0:10]
			ievietosanasDatums = ievietosanasDatums[6:]+"-"+ievietosanasDatums[3:5]+"-"+ievietosanasDatums[0:2]

			#extracting model, type, price, etc. information
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
			modelisText = modelisText[:20]


			izlaidumaGadsCrude = specs[izlaidumaGadsIndex:motoraTilpumsIndex]
			izlaidumaGadsText = izlaidumaGadsCrude.replace('Izlaiduma gads:', '')
			izlaidumaGadsInt = int(izlaidumaGadsText)

			motoraTilpumsCrude = specs[motoraTilpumsIndex:cenaIndex]
			motoraTilpumsInt = float(motoraTilpumsCrude.replace('Motora tilpums, cm3:', ''))
			motoraTilpumsInt = round(motoraTilpumsInt,0)
			motoraTilpumsInt=int(motoraTilpumsInt)
			

			cenaCrude = specs[cenaIndex:]
			cenaText = cenaCrude.replace('Cena:', '')
			cenaText = cenaText.replace(' €Aprēķināt apdrošināšanu', '')
			cenaText = cenaText.replace(' ', '')
			cenaText = float(cenaText)
			cenaText = int(round(cenaText))
			
			specs = specs[:markaIndex] + ''
			specs_short = specs[:540]
			specs_short = specs_short.replace(",","/,")
			specs_short = specs_short.replace('"', '^')

			datums = date.today().strftime('%Y-%m-%d')


			myJSON1 = {
	
				'NO_ID' : uniqueNumber, 
				'INSERT_DATE' : datums,
 				'CURENT_STATUS' : 1,
 				'FIND_LINK' : links,
 				'CONTENTS_TEXT' : specs_short,
 				'MARK' : markaText,
 				'MODEL' : modelisText,
 				'YEAR' : izlaidumaGadsInt,
 				'ENGINE_SIZE' : motoraTilpumsInt,
 				'PRICE' : cenaText,
 				'IMPORT_DATE' : ievietosanasDatums,
 				'UNIQUE_VISITS' : counterNumber,
 				'PICTURE_LINKS' : kopaBildes
			}	
			mycursor = mydb.cursor()
		
			sql = f'INSERT INTO {motoname} ( \
				NO_ID, INSERT_DATE, CURENT_STATUS, FIND_LINK, CONTENTS_TEXT, MARK, MODEL, YEAR_CREATED, \
				ENGINE_SIZE, PRICE, IMPORT_DATE, UNIQUE_VISITS, PICTURE_LINKS)  \
			VALUES ("{uniqueNumber}","{datums}",1,"{links}","{specs_short}","{markaText}","{modelisText}" \
				,"{izlaidumaGadsInt}","{motoraTilpumsInt}","{cenaText}","{ievietosanasDatums}","{counterNumber}", \
					"{kopaBildes}");'
			#print(f"{sql}")
			mycursor.execute(sql)
			mydb.commit()
			print(mycursor.rowcount, "record inserted.")


	print(Fore.YELLOW + f'FINISHED WORKING WITH: {motoname}' + Style.RESET_ALL)	
print(Fore.GREEN + f'FINISHED WORKING' + Style.RESET_ALL)
	




