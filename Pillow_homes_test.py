
# coding: utf-8

# In[53]:

import psycopg2
import sys
import pandas as pd

import folium
import urllib
import requests
import urllib2
from bs4 import BeautifulSoup

import time
import random
import json




def run_test():
    conn_string = "host = 'ec2-52-53-200-58.us-west-1.compute.amazonaws.com' port = 5432 dbname = 'postgres' user = 'luke' password = 'luke_pillow'"
    print "Connecting to database\n ->%s" % (conn_string)
    conn = None
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        print "Connected!\n"
        
            
        #1.How many listings are unique? (Filter out duplicates if any)
        get_unique_record(conn)
        
        #2.What country has the highest median price for a one bedroom?
        get_highest_median_price(conn)
        
        #3.Find two unique listings that have the shortest distance (euclidean distance/orthodromic distance)
        get_shortest_distance(conn)
        
        #4.Visualize all US listings on a map
        visualize(conn)
        
        #5.Find out the total number of active listings
        get_total_active(conn)
        
        #Part3 Web scraping
        web_scraping(conn)
        
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
        
        

#1. How many listings are unique? (Filter out duplicates if any)
def get_unique_record(conn):
    
    sql = 'SELECT COUNT(DISTINCT(id, bedrooms, bathrooms, city, country, is_location_exact,     lat, lng, price, description, picture_urls, picture_captions, star_rating, recent_review))     FROM case_study_data_short_term_rentals'
    
    df = pd.read_sql(sql, conn)
    print "The number of unique records is {}".format(df['count'].values[0])
    
    
#2. What country has the highest median price for a one bedroom?
def get_highest_median_price(conn):
    
    sql = 'SELECT t.country, t.bedrooms, t.price FROM case_study_data_short_term_rentals AS t'
    df = pd.read_sql(sql, conn)
    country = df[df['bedrooms'] == 1].groupby('country').aggregate({'price': 'mean'})['price'].idxmax()
    #highest_price = df[df['bedrooms'] == 1].groupby('country').aggregate({'price': 'mean'}).max()
    print '{} has the highest median price for a one bedroom'.format(country)
    #print 'The highest median price is {}'.format(highest_price)
    
      
    
#3. Find two unique listings that have the shortest distance (euclidean distance/orthodromic distance)
def get_shortest_distance(conn):
    sql = 'SELECT t1.id AS listing_a, t2.id AS listing_b,     |/((t1.lat - t2.lat) ^ 2 + (t1.lng - t2.lng) ^ 2) AS distance     FROM case_study_data_short_term_rentals AS t1     INNER JOIN case_study_data_short_term_rentals AS t2     ON t1.id < t2.id ORDER BY distance LIMIT 1'
    df = pd.read_sql(sql, conn)
    listing_a, listing_b = df['listing_a'].values[0], df['listing_b'].values[0]
    print 'Listing {} and listing {} have the shortest distance'.format(listing_a, listing_b)
    
    
#4. Visualize all US listings on a map
def visualize(conn):
    sql = "SELECT lat, lng, country FROM case_study_data_short_term_rentals WHERE country = 'United States'"
    df = pd.read_sql(sql, conn)
    
    #use Folium to implement visualization
    map_osm = folium.Map(location=[40, -80], zoom_start=2)
    df.apply(lambda row: folium.Marker([row["lat"], row["lng"]], icon = folium.Icon(icon = 'cloud')).add_to(map_osm), axis = 1)
    map_osm.save('map.html')

    
#5. Find out the total number of active listings (Active means you can still find it on
#Airbnb at the time you do these exercises. Hint: Id is a unique identifier of Airbnb, 
#https://www.airbnb.com/rooms/13051179  will lead you to an Airbnb listing. You will get an error if the page is no longer active)
def get_total_active(conn):
    sql = "SELECT * FROM case_study_data_short_term_rentals" 
    df = pd.read_sql(sql, conn)
    
    number = 0
    
    #option1: spend less time
    for i in range(0, df['id'].count()):
        #id
        temp = df['id'][i]
        url = "https://www.airbnb.com/rooms/%s" %temp 
        try: 
            urllib.urlopen(url)
            number = number + 1
        except urllib.HTTPError, e:
            print(e.code)
        except urllib.URLError, e:
            pirnt(e.args)
    
    
    '''
    #option2: Let my program sleep for a random time so that my program will not be blocked by the server 
    #In this way, the server will less likely identify my program as a virus attach or DDOS(distributed denial-of-service) attack
    #Implementing by a random number generator 
    #But it costs several hours to run
    for i in range(0, df['id'].count()):
        #id
        temp = df['id'][i]
        url = "https://www.airbnb.com/rooms/%s" %temp 
                
        request = requests.get(url)
        if request.status_code == 200:
            #Web site exists
            print "https://www.airbnb.com/rooms/%s exists" %temp
            number = number + 1   
        else:
            print "https://www.airbnb.com/rooms/%s does not exist" %temp
        
        time.sleep(random.randint(0, 20))
        
    '''
         
    print "The number of active listings is %s" %number
    
#Part3: Web Scraping
#Airbnb API is not publicly available. 
#But I found some resources online and read other’s JavaScript code on GitHub.  
#Here’s the link. https://github.com/phamtrisi/airapi 
#Then I tried to use the check_availability_url = 'https://www.airbnb.com/api/v2/calendar_months' to get calendar information. 

def web_scraping(conn):
    sql = "SELECT * FROM case_study_data_short_term_rentals" 
    df = pd.read_sql(sql, conn)
    
    url = 'https://www.airbnb.com/api/v2/calendar_months'
    key='d306zoyjsyarp7ifhu67rjxn52tv0t20'
    
    for i in range(0, df['id'].count()):
        #id
        temp = df['id'][i]     
        parameters={
            'key': key,
            'currency': 'USD',
             'locale': 'en',
             'month': 5,
             'year': 2017,
             'count': 3,
             '_format': 'with_conditions',
             'listing_id':temp
        }
        head= {'Content-Type':'application/json'}
        ret = requests.post(url,params = parameters,headers = head)
        #The status_code is 404, which means url is not correct. I didn't have enough time to debug this question.
        #print ret.status_code  

    
if __name__ == "__main__":
    run_test()
    


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



