import requests
from bs4 import BeautifulSoup
import mysql.connector

#Database Config Started
dataBase = mysql.connector.connect(
    host ="localhost",
    user ="jalilgroup_balu",
    passwd ="B@lmca28",
    database = "balu_iikData")
cursorObject = dataBase.cursor()

#Database Config Ended

#Single Page Data Fetching and Inserting Start
def singlepagedata(url):
                #url = "https://www.indiansinkuwait.com/iikclassified/show/2549059/Looking-for-Single-Bedroom-with-attach-washroom-";
                details = requests.get(url)
                soup = BeautifulSoup(details.content, 'html5lib')
                if soup.find('span', attrs = {'id':'ctl00_ContentPlaceHolder1_lblID'}).text is None:
                    return         
                else: id = soup.find('span', attrs = {'id':'ctl00_ContentPlaceHolder1_lblID'}).text;
                location = soup.find('span', attrs = {'id':'ctl00_ContentPlaceHolder1_lblLocation'}).text 
                title = soup.find('h1', attrs = {'class':'classifiedtitle'}).text
                description = soup.find('span', attrs = {'id':'ctl00_ContentPlaceHolder1_lblMatter'}).text
                phone = soup.find('span', attrs = {'id':'ctl00_ContentPlaceHolder1_lblPhone2'}).text
                location2 = soup.find('span', attrs = {'id':'ctl00_ContentPlaceHolder1_lblLocation2'}).text
                #phone2 = soup.find('span', attrs = {'id':'ctl00_ContentPlaceHolder1_lblPhone,'},some_attribute=True).a.text
                
                sql = "SELECT * FROM data WHERE id = %s"
                val = (id, )
                cursorObject.execute(sql, val)
                # if the record does not exist, insert a new record
                if cursorObject.fetchone() is None:
                    sql = "INSERT INTO data (id, title, description, phone, location2,phone2,location,completed,url,jobs) VALUES (%s, %s, %s,%s, %s, %s,%s,%s,%s,%s)"
                    val = (id,title,description,phone,location2,phone,location,1,url,0)
                    cursorObject.execute(sql, val)
                    dataBase.commit()
                    print("Record inserted successfully")
                
                else:
                
                    print("Record already exists")
                
                #Single Page Data Fetching and Inserting Start

def mainpage():
          #main Page Scrapper Started
          URL = "https://www.indiansinkuwait.com/iikclassified/"
          r = requests.get(URL)
          soup = BeautifulSoup(r.content, 'html5lib')
          quotes=[]  # a list to store quotes
          main = soup.find('table', attrs = {'id':'ctl00_ContentPlaceHolder1_grdTodaysAd'}) 
          for row in main.findAll('div', attrs = {'class':'p-1 text-break'}):
            quote = {}
            id=row.div.span.text;
            quote['id'] = row.div.span.text
            quote['title'] = row.find('a').text
            quote['url'] = 'https://www.indiansinkuwait.com'+row.a['href']
            url='https://www.indiansinkuwait.com'+row.a['href']
            sql = "SELECT * FROM data WHERE id = %s AND completed=0"
            val = (id, )
            cursorObject.execute(sql, val)
            # if the record does not exist, insert a new record
            if cursorObject.fetchone() is None:
                singlepagedata(url)
            else:
                print("Record already exists")

#main Page Scrapper 
def jobpage():
            #main Page Scrapper Started
        URL = "https://www.indiansinkuwait.com/iikclassified/category/Situation-Vacant/"
        r = requests.get(URL)
        soup = BeautifulSoup(r.content, 'html5lib')
        for row in soup.findAll('span', attrs = {'class':'pr-2 pl-2 '}):
            id=row.div.span.text
            sql = "SELECT * FROM data WHERE id = %s AND jobs=0"
            val = (id, )
            cursorObject.execute(sql, val)
            # if the record does not exist, insert a new record
            if cursorObject.fetchone() is None:
                sql ="UPDATE data SET jobs = 1 WHERE id = %s"
                val= (id, )
                cursorObject.execute(sql, val)
                dataBase.commit()
                print("Record Updated successfully")
            else:
                print("Record already Updated")

try:
  mainpage()
  jobpage()
  print("Data Scraping Completed")
except Exception as error:
  print("An error occurred:", type(error).__name__) 
dataBase.close()