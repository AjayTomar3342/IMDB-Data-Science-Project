#Important import statements; Not required in pyspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType, IntegerType
from pyspark.sql.types import ArrayType,DoubleType,BooleanType
from pyspark.sql.functions import array_contains

#Create a formal spark session with specific configurations helpful for the whole analytics session
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

#Read csv file with it's headers and it's pre-defined column data types
df=spark.read.option("header",True).options(inferSchema='True').csv("C:/Users/Ajay/PycharmProjects/Web_Scraping/Python-cleaned-file.csv")

# ###Getting count of movie titles starting with a specific characters###
#
# #Create a list of all alphabets
# alphabets=['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']
#
# #Create an empty list to store count of all alphabets
# alphabets_count=[]
#
# #Required to use col
# import pyspark.sql.functions as sf
#
# #Get all titles that start with a character
# for i in alphabets:
#     k = df.filter(sf.col("Title").startswith(i)).count()
#     alphabets_count.append(k)
#
# #Add Others for non-alphabet starting characters
# alphabets.append('Others')
#
# #Append count of other movie starting characters
# alphabets_count.append(df.count() - sum(alphabets_count))
#
# #Create a dataframe with both lists
# count_of_first_character_in_movie_titles=sqlContext.createDataFrame(zip(alphabets, alphabets_count), schema=['Alphabets', 'Occurence Count'])


#Filter out some rows for proper analysis
#df2=df.filter( (df["User Rating"] >6) & (df["Votes"]>7500) )


# ###Movies Count year wise###
#
# #Group by Release Year and get count year wise
# df2.groupBy('Release Year').count().show()


# ###Average runtime of movies by year###
#
# #Trim the whitespaces from column for easy access
# tempList = [] #Edit01
#     for col in df.columns:
#         new_name = col.strip()
#         new_name = "".join(new_name.split())
#         new_name = new_name.replace('.','') # EDIT
#         tempList.append(new_name) #Edit02
# print(tempList) #Just for the sake of it #Edit03
#
# df = df.toDF(*tempList)
#
# #Change data type of Movie runtime column to int
# from pyspark.sql.types import StringType,BooleanType,DateType
#
# df2 = df2.withColumn("Duration(Min)",df2["Duration(Min)"].cast('int')
#
# #Group by Release year and get yearly average movie runtime
# k=df2.groupBy('Release Year').mean('Duration(Min)')
#
# #Surprisingly there are some string values in k. So we remove them
# k=k.na.drop()
#
# #Rename the dataframe
# average_runtime_of_movies=k

#
# ###Top movies proprotion by genre by year###
#
# #Get count of genre by Year
# k = df.groupBy('ReleaseYear','Genre').count()
#
# #Remove the null values
# k=k.na.drop()
#
# #Sort by year and genre
# k=k.sort("ReleaseYear","Genre")
#
# #Store as a differentiating name
# year_wise_movie_genre_count=k


# ###Votes increase/decrease per year###
#
# #Get year wise total votes
# k = df2.groupBy('ReleaseYear').sum('Votes')
#
# #Sort by Release Year
# k=k.sort('ReleaseYear')
#
# #Store using a differentiating name
# votes_count_year_wise=k
#
#
# ###Year Wise Top rated movie###
#
# #Get top rated movie year wise
# k=df2.groupBy('ReleaseYear','Title').max('UserRating')
#
# #Sort according to year
# k=k.sort('ReleaseYear')
#
# #Store as a differentiating name
# top_movie_year_wise=k


# # ##Genre Wise Movie Count###
# #
# # Get genre wise movie count
# genre_wise_movie_count=df.groupBy('Genre').count()


# ###Best Primary actor with at least 50 movies###
#
# #Get count of movies by primary actor
# k = df.groupBy('Actor #1').count()
#
# #Remove null values
# k=k.na.drop()
#
# #Filter out actors with less than 50 movies
# import pyspark.sql.functions as sf   #Required to use col
# k = k.filter(sf.col("count") > 50).show(truncate=False)
#
# #Use join to get rows which have selected actors in both dataframes
# k2 = df.join(k, ["Actor #1"], "rightouter")
#
# #Get average user review for the selected actors with more than 50 movies
# k3=k2.groupBy('Actor #1').mean('User Rating')
#
# #Sort in decreasing order
# k3=k3.sort(sf.col("avg(User Rating)").desc())
#
# #Store as a differentiating name
# best_primary_actor=k3
#
#
# ###Best Secondary actor with at least 30 movies###
#
# #Get count of movies by Secondary actor
# k = df.groupBy('Actor #2').count()
#
# #Remove null values
# k=k.na.drop()
#
# #Filter out actors with less than 30 movies
# import pyspark.sql.functions as sf   #Required to use col
# k = k.filter(sf.col("count") > 50)
#
# #Use join to get rows which have selected actors in both dataframes
# k2 = df.join(k, ["Actor #2"], "rightouter")
#
# #Get average user review for the selected actors with more than 30 movies
# k3=k2.groupBy('Actor #2').mean('User Rating')
#
# #Sort in decreasing order
# k3=k3.sort(sf.col("avg(User Rating)").desc())
#
# #Store as a differentiating name
# best_secondary_actor=k3

# ###Best Supporting Secondary actor with at least 30 movies###
#
# #Get count of movies by Supporting Secondary actor
# k = df.groupBy('Actor #3').count()
#
# #Remove null values
# k=k.na.drop()
#
# #Filter out actors with less than 30 movies
# import pyspark.sql.functions as sf   #Required to use col
# k = k.filter(sf.col("count") > 30)
#
# #Use join to get rows which have selected actors in both dataframes
# k2 = df.join(k, ["Actor #3"], "rightouter")
#
# #Get average user review for the selected actors with more than 30 movies
# k3=k2.groupBy('Actor #3').mean('User Rating')
#
# #Sort in decreasing order
# k3=k3.sort(sf.col("avg(User Rating)").desc())
#
# #Store as a differentiating name
# best_supporting_secondary_actor=k3

# ###Best Director with at least 30 movies###
#
# #Get count of movies by Director
# k = df.groupBy('Director').count()
#
# #Remove null values
# k=k.na.drop()
#
# #Filter out directors with less than 30 movies
# import pyspark.sql.functions as sf   #Required to use col
# k = k.filter(sf.col("count") > 30)
#
# #Use join to get rows which have selected actors in both dataframes
# k2 = df.join(k, ["Director"], "rightouter")
#
# #Get average user review for the selected directors with more than 30 movies
# k3=k2.groupBy('Director').mean('User Rating')
#
# #Sort in decreasing order
# k3=k3.sort(sf.col("avg(User Rating)").desc())
#
# #Store as a differentiating name
# best_director=k3

# ###Biography genre movie count since 1915###
#
# #Get Biography genre movies
#
# #Get yearwise count of all genre movies
#
# df3=df.groupBy('Release Year','Genre').count()
#
# #Remove null values
#
#df3=df3.na.drop()
#
# #Filter out biography movies from the table
#
# df4 = df3.filter(sf.col("Genre") == 'Biography')
#
# #Store as a differentiating name
# biography_movie_genre_count_year_wise=df4


###Crime genre movie count since 1915###
# #Filter out crime movies from the table
#
# df4 = df3.filter(sf.col("Genre") == 'Crime')
#
# #Store as a differentiating name
# crime_movie_genre_count_year_wise=df4

###Comedy genre movie count since 1915###
# #Filter out comedy movies from the table
#
# df4 = df3.filter(sf.col("Genre") == 'Comedy')
#
# #Store as a differentiating name
# comedy_movie_genre_count_year_wise=df4

###Drama genre movie count since 1915###
# #Filter out Drama movies from the table
#
# df4 = df3.filter(sf.col("Genre") == 'Drama')
#
# #Store as a differentiating name
# drama_movie_genre_count_year_wise=df4

###Fantasy genre movie count since 1915###
# #Filter out Fantasy movies from the table
#
# df4 = df3.filter(sf.col("Genre") == 'Fantasy')
#
# #Store as a differentiating name
# fantasy_movie_genre_count_year_wise=df4

###Horror genre movie count since 1915###
# #Filter out Horror movies from the table
#
# df4 = df3.filter(sf.col("Genre") == 'Horror')
#
# #Store as a differentiating name
# horror_movie_genre_count_year_wise=df4

###Mystery genre movie count since 1915###
# #Filter out Myster movies from the table
#
# df4 = df3.filter(sf.col("Genre") == 'Mystery')
#
# #Store as a differentiating name
# mystery_movie_genre_count_year_wise=df4

###War genre movie count since 1915###
# #Filter out War movies from the table
#
# df4 = df3.filter(sf.col("Genre") == 'War')
#
# #Store as a differentiating name
# war_movie_genre_count_year_wise=df4

###Action genre movie count since 1915###
# #Filter out Action movies from the table
#
# df4 = df3.filter(sf.col("Genre") == 'Action')
#
# #Store as a differentiating name
# action_movie_genre_count_year_wise=df4

###Sci-fi genre movie count since 1915###
# #Filter out Sci-f- movies from the table
#
# df4 = df3.filter(sf.col("Genre") == 'Sci-fi')
#
# #Store as a differentiating name
# scifi_movie_genre_count_year_wise=df4

###Thriller genre movie count since 1915###
# #Filter out Thriller movies from the table
#
# df4 = df3.filter(sf.col("Genre") == 'Thriller')
#
# #Store as a differentiating name
# thriller_movie_genre_count_year_wise=df4


###Animation genre movie count since 1915###
# #Filter out Animation movies from the table
#
# df4 = df3.filter(sf.col("Genre") == 'Animation')
#
# #Store as a differentiating name
# animation_movie_genre_count_year_wise=df4


# ###Decade Wise movie count###
#
# #Create new column `Decade` on basis of Release Year
# df2=df2.withColumn('Decade',sf.when((sf.col("Release Year") >1910) & (sf.col("Release Year") <=1920),"1910-1920").when((sf.col("Release Year")>1920) & (sf.col("Release Year")<=1930),"1920-1930").when((sf.col("Release Year")>1930) & (sf.col("Release Year")<=1940),"1930-1940").when((sf.col("Release Year")>1940) & (sf.col("Release Year")<=1950),"1940-1950").when((sf.col("Release Year")>1950) & (sf.col("Release Year")<=1960),"1950-1960").when((sf.col("Release Year")>1960) & (sf.col("Release Year")<=1970),"1960-1970").when((sf.col("Release Year")>1970) & (sf.col("Release Year")<=1980),"1970-1980").when((sf.col("Release Year")>1980) & (sf.col("Release Year")<=1990),"1980-1990").when((sf.col("Release Year")>1990) & (sf.col("Release Year")<=2000),"1990-2000").when((sf.col("Release Year")>2000) & (sf.col("Release Year")<=2010),"1920-1930").when((sf.col("Release Year")>2010) & (sf.col("Release Year")<=2020),"2010-2020").otherwise("na"))
#
# #Get decade wise movie count
# k=df2.groupBy('Decade').count()
#
# #Store as a differentiating name
# decade_wise_movie_count=k

