// Databricks notebook source
//********************************************************************
//
// Author: Sujung Choi
// Course name: Data Analytics (CSC 735-Sec 1)
// Assignment: HW3
// Date of Creation: October 10, 2023
//
//********************************************************************

// COMMAND ----------

//load the dataset
val df1 = spark.read.option("header", "true") 
 .option("inferSchema","true")
 .csv("/FileStore/tables/movies.csv")
 .na.drop()
df1.createOrReplaceTempView("movies_table") 

val df2 = spark.read.option("header", "true") 
 .option("inferSchema","true")
.csv("/FileStore/tables/movie_ratings.csv")
df2.createOrReplaceTempView("movie_reviews_table")



// COMMAND ----------

//3) DataFrame-based Spark code to find the number of distinct movies in the file movies.csv. 

// Use countDistinct function to count the distinct movies from df1.
import org.apache.spark.sql.functions._
df1.select(countDistinct("title")).show()

// COMMAND ----------

//4) DataFrame-based Spark code to find the titles of the movies that appear in the file movies.csv but do not have a rating in the file movie_ratings.csv.

// Left anti join results in rows from the left dataset(df1) only if there is no matching row in the right dataset(df2) for the column name "title".
df1.join(df2, df1.col("title") === df2.col("title"), "left_anti").show()

// COMMAND ----------

//5)  DataFrame-based Spark code to find the number of movies that appear in the ratings file (i.e., movie_ratings.csv) but not in the movies file (i.e., movies.csv). 

// Left anti join results in rows from the left dataset(df2) only if there is no matching row in the right dataset(df1) for the column name "title".
// By using count function, it returns the number of movies satisfying the condition.
df2.join(df1, df1.col("title") === df2.col("title"), "left_anti").count()

// COMMAND ----------

//6) DataFrame-based Spark code to find the total number of distinct movies that appear in either movies.csv, or movie_ratings.csv, or both. 

// outer join is used to include all rows from both df1 and df2 where the join condition matches as well as nonmatching rows.
// 'coalesce' function is used to merge the two sets of columns into one (because 'title' and 'year' are in both df1 and df2), selecting the non-null value when there is a match and selecting the available value when there is no match. 
df1.join(df2, Seq("title", "year"), "outer").select(coalesce(df1("title"), df2("title")), coalesce(df1("year"), df2("year"))).distinct().count()

// COMMAND ----------

//7) DataFrame-based Spark code to find the title and year for movies that were remade. These movies appear more than once in the ratings file with the same title but different years. The output will be sorted by title.

// create a new df containing the "title" column that was counted more than once in df2 using filter function.
val remadeDf = df2.groupBy("title").count().filter($"count" > 1).select("title")

// inner join the remadeDf with df2 to find the rows that contain the same "title".
df2.join(remadeDf, "title").select("title", "year").orderBy("title").show


// COMMAND ----------

//8) DataFrame-based Spark code to find the rating for every movie that the actor "Branson, Richard" appeared in. Schema of the output is (title, year, rating)

// create a new df to find the movies that "Branson, Richard" was in from df1
val bransonDf = df1.select("title", "year").where(col("actor") === "Branson, Richard")

// inner join the bransonDf with df2 to find the rating of every movie from bransonDf that also exist in df2
df2.join(bransonDf, Seq("title", "year")).select("title", "year", "rating").show


// COMMAND ----------

//9) DataFrame-based Spark code to find the highest-rated movie per year and include all the actors in that movie. The output will have only one movie per year, and it contains four columns: year, movie title, rating, and a list of actor names. The output will be sorted by year. 

// find the highest-rated movie for each year
val highestRated = df2.groupBy("year").agg(max("rating").as("rating"))

// inner join highestRated with df2 based on rows where they have the same year and rating
val df3 = highestRated.join(df2, Seq("year", "rating"))

// option 1: use left outer join to include all movies from df3 (65 rows in total). If the movie does not appear in df1, then the list of actor names for such movies will be empty.
//collect_list() function is used to return a list of objects, so it gather all actors for each movie into a list.
val allHighestRatedPerYear = df3.join(df1, Seq("title", "year"), "left_outer").groupBy("year", "title", "rating").agg(collect_list("actor").as("actors")).orderBy("year")
//show the result
allHighestRatedPerYear.show(false)

// option 2: use inner join to only include movies that appear in both df1 and df3, so it will exclude the movies that are not appeared in df1.
// it results in 5 rows that contains the list of actors.
val highestRatedWithActors = df3.join(df1, Seq("title", "year")).groupBy("year", "title", "rating").agg(collect_list("actor").as("actors")).orderBy("year")
//show the result
highestRatedWithActors.show(false)


// COMMAND ----------

//10) DataFrame-based Spark code to determine which pair of actors worked together most. Working together is defined as appearing in the same movie. The output will have three columns: actor 1, actor 2, and count. The output should be sorted by the count in descending order.

// use 'alias' for DataFrame to return a new df with an alias set
// use 'where' function to remove the duplicates and to keep only one set of each pair (e.g. (actor A, actor B) and (actor B, actor A) are considered the same so keep only one of them) by ensuring the first actor's name is smaller than the second actor's name in alphabetical order.
val pairedActors = df1.alias("first").join(df1.alias("second"), Seq("title", "year")).where(col("first.actor") < col("second.actor")).select(col("first.actor").as("actor1"), col("second.actor").as("actor2"))

// count the number of times each pair of actors worked together and sort it in descending order, so the first top one in the result is the pair of actors who worked together the most.
pairedActors.groupBy("actor1", "actor2").count().orderBy(col("count").desc).show()
