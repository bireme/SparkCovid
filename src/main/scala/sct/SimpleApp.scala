package sct

import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import scala.util.{Failure, Success, Try}

object SimpleApp {
  private val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  private val sdf3 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // 2022-12-26T18:10:30.742Z
  private val sdf4 = new SimpleDateFormat("EEE MMM dd HH:mm:ssz yyyy", Locale.US) // "Mon Feb 27 15:10:24 BRT 2023"

  def main(args: Array[String]): Unit = {
      /*val logFile = "/home/heitor/Documentos/ChatGPT em 20230317.txt" // Should be some file on your system
      val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
      val logData = spark.read.textFile(logFile).cache()
      val numAs = logData.filter(line => line.contains("a")).count()
      val numBs = logData.filter(line => line.contains("b")).count()
      println(s"Lines with a: $numAs, Lines with b: $numBs")
      spark.stop()*/

    val str = "2022-11-15T18:10:15.803Z" //"2023-01-30T18:10:36.806Z" //"2022-10-20T18:10:13.509Z" //"2022-12-26T18:10:28.763Z"
    val date = fix(str)

    println(s"date=$date")
  }

  def fix(in: String): java.sql.Date = {
    val inT: String = Option(in).getOrElse("").trim

    val t1 = Try(sdf1.parse(inT))
    val t2 = Try(sdf2.parse(inT))
    val t3 = Try(sdf3.parse(inT))
    val t4 = Try(sdf4.parse(inT))

    val date = t1.orElse(t2).orElse(t3).orElse(t4) match {
      case Success(dt) =>
        dt
      case Failure(exception) =>
        //exception.printStackTrace()
        println(s"################ Error during date string: $inT exception=$exception")
        new Date()
    }
    Try(new java.sql.Date(date.getTime)) match {
      case Success(dt) =>
        dt
      case Failure(exception) =>
        println(s"@@@@@@@@@@@@@@@@ Error during date sql:$inT ${exception.toString}")
        new java.sql.Date(0)
    }
  }
}
