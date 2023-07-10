package sct

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import sp._

import java.sql
import java.util.Date

object SparkCovidWho {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Spark Covid WHO")
      .master("local[*]")
      .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2")
      .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017")
      .config("spark.mongodb.read.database", "COVID-19_WHO_Excel")
      .config("spark.mongodb.read.collection", "who-repo4covid-biblio.xlsx")
      .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017")
      .config("spark.mongodb.write.database", "COVID-19_WHO_CSV")
      .config("spark.mongodb.write.collection", "TESTE")
      .config("spark.mongodb.write.idFieldList", "Refid")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val df: DataFrame = spark.read.format("mongodb").load()
    df.printSchema()
    df.show(20, truncate = true)

    val date: sql.Date = new java.sql.Date(new Date().getTime)

    val df1: DataFrame =
      df.withColumn("Abstract", Abstract._udf(col("Abstract")))
      .withColumn("Accession number", AccessionNumber._udf(col("Accession number")))
      .withColumn("AlternateId", AlternateId._udf(col("AlternateId")))
      .withColumn("Author", Authors._udf(col("Author"))).withColumnRenamed("Author", "Authors")
      .withColumn("Refid", Abstract._udf(col("Refid"))).withColumnRenamed("Refid", "CovNum")
      .withColumn("Database", Database._udf(col("Database")))
      .withColumn("D date", Ddate._udf(col("D date")))
      .withColumn("Doi", DOI._udf(col("Doi")))
      .withColumn("Issue", Issue._udf(col("Issue")))
      .withColumn("Journal", Journal._udf(col("Journal")))
      .withColumn("Keywords", Keywords._udf(col("Keywords")))
      .withColumn("Language", Language._udf(col("Language")))
      .withColumn("Pages", Pages._udf(col("Pages")))
      .withColumn("Published Month", PublishedMonth._udf(col("Publishdate")))
      .withColumn("Published Year", PublishedYear._udf(col("Year")))
      .withColumn("Title", Title._udf(col("Title")))
      .withColumn("Volume", Volume._udf(col("Volume")))
      .withColumn("_upddSrc", UpddSrc._udf(col("_updd")))
      .withColumn("_updd", lit(date))
      .withColumn("PMID", PMID._udf(col("Accession number")))
      .withColumn("WOS", WOS._udf(col("Accession number")))
      .withColumn("KJD", KJD._udf(col("Accession number")))
      .withColumn("SCIELO", SCIELO._udf(col("Accession number")))
      .withColumn("UNKNOWN", UNKNOWN._udf(col("Accession number")))
    df1.printSchema()

    val df2: DataFrame =
      df1.withColumn("FulltextLink", FulltextLink._udf(col("Doi"), col("PMID")))
    df2.printSchema()

    val df3: DataFrame = df2.select("Abstract", "Accession Number", "AlternateId", "Authors", "CovNum",
      "Database", "D date", "DOI", "FulltextLink", "Issue", "Journal", "Keywords", "KJD", "Language", "Pages", "PMID",
      "PublishDate", "Published Month", "Published Year", "SCIELO", "Tags", "Title", "UNKNOWN", "Volume", "WOS", "_updd",
      "_upddSrc").withColumnRenamed("D date", "Date Added")
    df3.printSchema()
    df3.show(3, truncate = true)

    val windowSpec = Window.partitionBy("CovNum").orderBy(col("Date Added").asc)
    val dfWithRowNumber = df3.withColumn("rowNumber", row_number().over(windowSpec))
    val dfWithoutDuplicates = dfWithRowNumber.filter(col("rowNumber") === 1).drop("rowNumber")

    print("Writing content to MongoDB... ")
    dfWithoutDuplicates.write.format("mongodb").mode("overwrite").save()
    print("OK.\nFinishing... ")

    spark.close()
    spark.stop()
    println("OK\n\n\n")
  }
}