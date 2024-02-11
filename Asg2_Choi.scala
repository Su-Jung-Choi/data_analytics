// Databricks notebook source
//********************************************************************
//
// Author: Sujung Choi
// Course name: Data Analytics (CSC 735)
// Assignment: HW2
// Date of Creation: October 6, 2023
//
//********************************************************************

// COMMAND ----------

// load the dataset
val df = spark.read.option("header", "true")
 .option("inferSchema","true")
 .csv("/FileStore/tables/movies.csv")

// COMMAND ----------

// Register the DataFrame as an SQL table
df.createOrReplaceTempView("movies_table") 

// Drop rows where it has a missing value
val updatedDf = df.na.drop()

// Register the updated DataFrame as a temp view
updatedDf.createOrReplaceTempView("movies_table")


// COMMAND ----------

// 11. SQL-based Spark code to compute the number of movies produced in each year and order it in ascending order by year. The output will show two columns for year and count.
//It counts distinct movie titles in each year because the dataset contains multiple redundant movie titles based on their association with different actors.
spark.sql("""SELECT year, COUNT(DISTINCT title)
          FROM movies_table
          GROUP BY year
          ORDER BY year ASC""").show

// COMMAND ----------

// 12. DataFrame-based Spark code to compute the number of movies produced in each year and order it in ascending order by year. The output will show two columns for year and count. 
// import pacakage to use countDistinct function
import org.apache.spark.sql.functions._
updatedDf.groupBy("year").agg(countDistinct("title")).orderBy("year").show()

// COMMAND ----------

//13. SQL-based Spark code to find the five top most actors who acted in the most number of movies. Output will be (actor, number_of_movies).
// It groups by each actor and count the number of movies an actor was in. Then, it selects the top five by ordering the number of movies in descending order and renames the count column to be number_of_movies.
spark.sql("""SELECT actor, COUNT(title) AS number_of_movies
          FROM movies_table
          GROUP BY actor
          ORDER BY number_of_movies DESC
          LIMIT 5""").show

// COMMAND ----------

//14. DataFrame-based Spark code to find the five top most actors who acted in the most number of movies. Output will be (actor, number_of_movies).
// groupBy actor and count the number of movies each actor was in using aggregation function. Then by sorting the number of movies in descending order and limiting to show only the first five in the list, it shows the top five actors who appeared in the most number of movies. 
updatedDf.groupBy("actor").agg(count("title").as("number_of_movies")).orderBy(desc("number_of_movies")).show(5)

// COMMAND ----------

//15. DataFrame-based Spark code to find the title and year for every movie that Tom Hanks acted in. 
// The output is sorted in ascending order by year. And the schema of the output will be (title, year). 
// By using where function, it filters the rows of the DF to only include rows that their value in the "actor" column is euqal to "Hanks, Tom."
 updatedDf.select("title", "year").where(col("actor") === "Hanks, Tom").sort(asc("year")).show()
