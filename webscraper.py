import mysql.connector # to connect python to a mysql database
import requests # will allow us to send HTTP requests to get HTML files
from requests import get
from bs4 import BeautifulSoup # will help us parse the HTML files
import pandas as pd # will help us assemble the data into a DataFrame to clean and analyze it
import numpy as np # will add support for mathematical functions and tools for working with arrays
import math # to check for nan float values

# To make sure we get English-translated titles from all the movies we scrape
headers = {"Accept-Language": "en-US, en;q=0.5"}

# Requesting the URL to get the contents of the page
url = "https://www.imdb.com/search/title/?groups=top_1000&ref_=adv_prv&ref"

results = requests.get(url, headers=headers)

# Make the content we grabbed easy to read by using BeautifulSoup
soup = BeautifulSoup(results.text, "html.parser")

# initialize empty lists where we will store our data
titles = []
years = []
time = []
imbd_ratings = []
metascores = []
votes = []
us_gross = []

# On the IMBD website, each movie div has the class lister-item mode-advanced. 
# We will need the scraper to find all of the divs with this class
movie_div = soup.find_all('div', class_='lister-item mode-advanced')

# We need to loop the scraper to iterate for all movies
for container in movie_div:
    # Add the elements based on their tags in the div
    name = container.h3.a.text
    titles.append(name)

    year = container.h3.find('span', class_='lister-item-year').text
    years.append(year)

    runtime = container.find('span', class_='runtime').text if container.p.find('span', class_='runtime') else '-' # Find the runtime, or put a dash if no runtime is listed
    time.append(runtime)

    imbd = float(container.strong.text)
    imbd_ratings.append(imbd)

    m_score = container.find('span', class_='metascore').text if container.find('span', class_='metascore') else '-1' # I am setting -1 if there is no score because we need to convert this datatype to an int and can't do so with '-'
    metascores.append(m_score)

    # Some movies have votes and gross earnings, while others do not. This code block addresses this issue
    nv = container.find_all('span', attrs={'name': 'nv'})

    vote = nv[0].text
    votes.append(vote)

    grosses = nv[1].text if len(nv) > 1 else '-'
    us_gross.append(grosses)

# We will build a DataFrame using pandas to store our data in a table.
movies = pd.DataFrame({
    'movie': titles,
    'year': years,
    'timeMin': time,
    'imbd': imbd_ratings,
    'metascore': metascores,
    'votes': votes,
    'us_grossMillions': us_gross,
})

# Cleaning up the data as everything except for imbd ratings is being stored as an object.

# Converting years to an int.
movies['year'] = movies['year'].str.extract('(\d+)').astype(int)

# Converting time to an int.
movies['timeMin'] = movies['timeMin'].str.extract('(\d+)').astype(int)

# Converting metascore to an int.
movies['metascore'] = movies['metascore'].astype(int)

# Converting votes to an int and removing commas
movies['votes'] = movies['votes'].str.replace(',', '').astype(int)

# Converting gross data to float and removing the dollar signs and the Ms.
movies['us_grossMillions'] = movies['us_grossMillions'].map(lambda x: x.lstrip('$').rstrip('M'))

movies['us_grossMillions'] = pd.to_numeric(movies['us_grossMillions'], errors='coerce')

# movies.to_csv('top50movies.csv')

# Adding the movies to a mysql database

# Inistializing the connection to the mysql db

mydb = mysql.connector.connect(
  host="localhost",
  user="james",
  password="be7crh",
  database="webscraperdb"
)

mycursor = mydb.cursor()

# Give each movie a rank. Need this as the rank is the ID in the database.
movierank = []
rank_count = 0
for x in movies['movie']:
    movierank.append(rank_count)
    rank_count += 1

""" # catch all us_grossMillions that show nan
for x in movies['us_grossMillions']:
    if math.isnan(x):
        movies['us_grossMillions'][x] = float(-1) """

""" for x in movierank:
    print("INSERT INTO top_50_movies (place, name, years, timeMin, imbd, metascore, votes, us_grossMillions) VALUES ({}, {}, {}, {}, {}, {}, {}, {})".format(x, movies['movie'][x], movies['year'][x], movies['timeMin'][x], movies['imbd'][x], movies['metascore'][x], movies['votes'][x], movies['us_grossMillions'][x]))
 """
for x in movierank:


    if math.isnan(movies['us_grossMillions'][x]):
        sql = "INSERT INTO top_50_movies (place, name, years, timeMin, imbd, metascore, votes, us_grossMillions) VALUES ({}, '{}', {}, {}, {}, {}, {}, {})".format(x, movies['movie'][x], movies['year'][x], movies['timeMin'][x], movies['imbd'][x], movies['metascore'][x], movies['votes'][x], -1)
    else:
        sql = "INSERT INTO top_50_movies (place, name, years, timeMin, imbd, metascore, votes, us_grossMillions) VALUES ({}, '{}', {}, {}, {}, {}, {}, {})".format(x, movies['movie'][x], movies['year'][x], movies['timeMin'][x], movies['imbd'][x], movies['metascore'][x], movies['votes'][x], movies['us_grossMillions'][x])
    
    mycursor.execute(sql)

    mydb.commit()

    print(mycursor.rowcount, "record inserted.")  