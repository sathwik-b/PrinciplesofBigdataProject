
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.context import SparkContext
from pyspark.sql import functions
from pyspark.sql import types
from datetime import date, timedelta, datetime
# from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import time
import gmplot
import sys

def convert_to_dict(output, indexName, valueName):
    genderDf = output.toPandas()
    result = genderDf.set_index(indexName).to_dict()
    return result.get(valueName)

# def barchart(inputdict, title, xlabel, ylabel):
#     fig = plt.figure(figsize=(6, 4))
#     fig.suptitle(title, fontsize=16)
#     ax = fig.add_axes([0.1, 0.2, 0.75, 0.5])
#     ax.set_xlabel(xlabel)
#     ax.set_ylabel(ylabel)
#     labels = inputdict.keys()
#     values = inputdict.values()
#     ax.bar(labels, values, width=0.5)
#     plt.show()

def piechart(diction):
    label = diction.keys()
    tweet_count = diction.values()
    colors = ['#ff9999', '#66b3ff']
    fig1, ax1 = plt.subplots()
    ax1.pie(tweet_count, colors=colors, labels=label,
            autopct='%1.1f%%', startangle=90)
    # # draw circle
    # centre_circle = plt.Circle((0, 0), 0.70, fc='white')
    # fig = plt.gcf()
    # fig.gca().add_artist(centre_circle)
    # # Equal aspect ratio ensures that pie is drawn as a circle
    # ax1.axis('equal')
    # plt.tight_layout()
   
# -------------------query 1:what day retweet counts received----------------
def query1() :
    day = sc.sql("SELECT distinct substring(user.created_at,1,3) as day,count(user.description)as count from parquetFile where user.description like '%Apple%'GROUP BY day ORDER BY day DESC LIMIT 10")
    day.show(10)
    x = day.toPandas()["day"].values.tolist()[:15]
    y = day.toPandas()["count"].values.tolist()[:15]
    figure = plt.figure(figsize=(10, 10))
    axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x,y, color = 'red')
    plt.title("Day and retweet counts received")
    plt.ylabel("count")
    plt.xlabel("day")
    # plt.show()
    plt.savefig('twitterdata\\static\\images\\query1.png')

# # ------------------------------query 2:User name and statuses count where apple is used-------------------------------------
def query2() :
    likes = sc.sql("SELECT user.name as name, user.statuses_count as cunt from parquetFile where user.description like '%Apple%' ORDER BY cunt DESC LIMIT 10")
    #likes.show() 
    x = likes.toPandas()["name"].values.tolist()
    y = likes.toPandas()["cunt"].values.tolist()
    figure = plt.figure(figsize=(10, 10))
    axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x,y, color = 'green')
    plt.title("Username and statuses count where apple is used")
    plt.ylabel("count")
    plt.xlabel("name")
    # plt.show()
    plt.savefig('twitterdata\\static\\images\\query2.png')


# # ---------------------------------query 3:number of languages comments received--------------------------------
def query3():
    langcomm= sc.sql("SELECT lang , Count(lang) as Count from parquetFile where id is NOT NULL and lang is not null and text like '%Apple%' group by lang order by Count DESC")
    #langcomm.show()
    x =langcomm.toPandas()["lang"].values.tolist()
    y = langcomm.toPandas()["Count"].values.tolist()
    figure = plt.figure(figsize=(10, 10))
    axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x,y, color = 'blue')
    plt.title("Number of languages comments received")
    plt.ylabel("count")
    plt.xlabel("language")
    #plt.show()
    plt.savefig('twitterdata\\static\\images\\query3.png')

# # -------------------------query 4: country and tweets count------------------------
def query4():
    country = sc.sql("SELECT distinct place.country country , count(*) as count FROM parquetFile where place.country is not null " + "GROUP BY place.country ORDER BY count DESC LIMIT 10")
    #country.show()
    x =country.toPandas()["country"].values.tolist()
    y = country.toPandas()["count"].values.tolist()
    figure = plt.figure(figsize=(10, 10))
    axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x,y, color = 'green')
    plt.title("country and tweets count")
    plt.ylabel("count")
    plt.xlabel("country")
    #plt.show()
    plt.savefig('twitterdata\\static\\images\\query4.png')
# # -------------------------------query 5: user with followers count--------------------------
def query5():
    morefollowers = sc.sql("SELECT user.name as name, user.followers_count as count from parquetFile where retweeted_status.text like '%iphone%' ORDER BY count DESC LIMIT 10")
    #morefollowers.show()
    x =morefollowers.toPandas()["name"].values.tolist()[:10]
    y =morefollowers.toPandas()["count"].values.tolist()
    figure = plt.figure(figsize=(10, 10))
    axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x,y, color = 'red')
    plt.title("Users with followers count")
    plt.ylabel("count")
    plt.xlabel("username")
   # plt.show()
    plt.savefig('twitterdata\\static\\images\\query5.png')
# # -----------------------query 6:User location with number of listed counts --------------
def query6():
    location = sc.sql("SELECT user.location as loc,user.listed_count as count from parquetFile where user.followers_count>1000 AND user.location is not null LIMIT 10")
    #location.show(10)
    x = location.toPandas()["loc"].values.tolist()[:10]
    y = location.toPandas()["count"].values.tolist()[:10]
    figure = plt.figure()
    axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x,y, color = 'blue')
    plt.title("User location with number of listed counts")
    plt.ylabel("Count")
    plt.xlabel("Location")
    # plt.show()
    plt.savefig('twitterdata\\static\\images\\query6.png')
    # results = convert_to_dict(user_retweet_count, 'Retweet', 'Count')
    # barchart(results, 'Top 10 users with retweets on corona','Users', 'No.of tweets')
# # ---------------------------query 7: Number of tweets on particular date------------------------------
def query7():
    numdate = sc.sql("SELECT SUBSTR(created_at, 0,10) daday, COUNT(1) cun FROM  parquetFile where SUBSTR(created_at, 0,3) is not null GROUP BY SUBSTR(created_at, 0,3) ORDER BY COUNT(1) DESC LIMIT 5")
    #numdate.show(10)
    x = numdate.toPandas()["daday"].values.tolist()
    y = numdate.toPandas()["cun"].values.tolist()
    figure = plt.figure(figsize=(10, 10))
    axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x,y, color = 'green')
    plt.title("Number of tweets on particular date")
    plt.ylabel("counts")
    plt.xlabel("dates")
    # plt.show()
    plt.savefig('twitterdata\\static\\images\\query7.png')

# #--------------------------query 8:Keywords in Hashtags and their counts-------------------------------
def query8() :
    x=sc.sql("SELECT CASE WHEN entities.hashtags[0].text like '%Apple%' THEN 'Apple' WHEN entities.hashtags[0].text like '%iphone%' THEN 'iphone' WHEN entities.hashtags[0].text like '%nike%' THEN 'nike' WHEN entities.hashtags[0].text like '%amazon%' THEN 'amazon'WHEN entities.hashtags[0].text like '%trump%'THEN 'trump' END AS names FROM parquetFile")
    x.createOrReplaceTempView("stringnames")
    y=sc.sql("SELECT names,count(names) as count from stringnames where names is NOT NULL group by names order by count DESC")
    #y.show(10)
    labels = y.toPandas()["names"].values.tolist()
    sizes = y.toPandas()["count"].values.tolist()
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%', shadow=False, startangle=90)
    ax1.axis('equal') 
    plt.title(" Keywords in Hashtags and their counts")
    plt.savefig('twitterdata\\static\\images\\query8.png')

# # ------------------------query 9:Count of keywords appeared in twitter data----------------------------
def query9():
     a=sc.sql("SELECT 'iphone' as brand1,count(text) as count from parquetFile where text like '%iphone%'")
     b=sc.sql("SELECT 'fossil' as brand1,count(text) as count from parquetFile where text like '%fossil%'")
     c=sc.sql("SELECT 'nike' as brand1,count(text) as count from parquetFile where text like '%nike%'")
     d=sc.sql("SELECT 'Apple' as brand1,count(text) as count from parquetFile where text like '%Apple%'")
     e=a.union(b).union(c).union(d)
     x = e.toPandas()["brand1"].values.tolist()
     y = e.toPandas()["count"].values.tolist()
     figure = plt.figure(figsize=(10, 10))
     axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
     plt.rcParams.update({'axes.titlesize': 'small'})
     plt.barh(x,y, color = 'green')
     plt.title("Count of keywords appeared in twitter data")
     plt.ylabel("count")
     plt.xlabel("brands")
     plt.savefig('twitterdata\\static\\images\\query9.png')


# #-------------------------query 10:Twitter accounts created in a month----------------------------------
def query10():
    #month1= sc.sql("SELECT substring(created_at,1,3) as month, count(1) as count from parquetFile GROUP BY month")
    monthcun= sc.sql("SELECT substring(created_at,1,10)  month, count(1) count from parquetFile where substring(created_at,1,10) is not null GROUP BY substring(created_at,1,10)")
    monthcun.show(10)
    x = monthcun.toPandas()["month"].values.tolist()
    y = monthcun.toPandas()["count"].values.tolist()
    figure = plt.figure(figsize=(10, 10))
    axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x,y, color = 'red')
    plt.title("Twitter accounts created in a month")
    plt.ylabel("count")
    plt.xlabel("month")
    # plt.show()
    plt.savefig('twitterdata\\static\\images\\query10.png')

def query11():
    #month1= sc.sql("SELECT substring(created_at,1,3) as month, count(1) as count from parquetFile GROUP BY month")
    time= sc.sql("select SUBSTRING(user.created_at, 27,4) as year, count(user.id) as count from parquetFile where user.created_at is not null group by year order by year desc")
    time.show(10)
    x = time.toPandas()["year"].values.tolist()
    y = time.toPandas()["count"].values.tolist()
    figure = plt.figure(figsize=(10, 10))
    axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x,y, color = 'red')
    plt.title("user created yaer and count")
    plt.ylabel("count")
    plt.xlabel("year")
    # plt.show()
    plt.savefig('twitterdata\\static\\images\\query11.png')
    #"SELECT SUBSTRING(created_at,12,5) as time_in_hour, COUNT(*) AS count FROM parquetFile GROUP BY time_in_hour ORDER BY time_in_hour")


if __name__ == "__main__":
    queryNumber = sys.argv[1]
    print("running")
    sc = SparkSession.builder.appName("PysparkExample").config("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize", "5g").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()
    print("Spark session start")
    total_data = sc.read.json('C:\\sandeep\\tweetsdata.txt')
    total_data.registerTempTable("parquetFile")
    print("table loaded")
   # total_tweets_data = sc.sql("SELECT user.name as UserName,user.location as location,text,created_at,user.verified as userVerified,retweet_count,place.country_code as Country,user.location as state,extended_tweet.entities.hashtags.text AS Hashtags,coordinates.coordinates as coordinates from datatable where place.country_code is not null AND (text like '%Corona%' OR text like '%corona%' OR text like '%coronavirus%' OR text like '%Coronavirus%')")
    #total_tweets_data.registerTempTable("totaltweetsdata")
    #print("tweets filtered")
    if queryNumber == '1':
        query1()
    if queryNumber == '2':
        query2()
    if queryNumber == '3':
        query3()
    if queryNumber == '4':
        query4()
    if queryNumber == '5':
        query5()
    if queryNumber == '6':
        query6()
    if queryNumber == '7':
        query7()
    if queryNumber == '8':
        query8()
    if queryNumber == '9':
        query9()
    if queryNumber == '10':
        query10()   
    if queryNumber == '11':
        query11()  
    sc.stop()
    print("PySpark completed")