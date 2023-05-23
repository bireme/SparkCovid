package sct

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkCovid1  {
  def main(args: Array[String]) {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark Excel example")
    .config("spark.master", "local")
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017")
    .config("spark.mongodb.write.database", "spark")
    .config("spark.mongodb.write.collection", "covid_who")
    .config("spark.mongodb.write.idFieldList", "Refid")
    //.config("spark.mongodb.write.writeConcern.w", "2")
    //.config("spark.mongodb.write.writeConcern.wTimeoutMS", "1")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  //sc.setLogLevel("WARN")

  val df: DataFrame = spark.read
    .format("com.crealytics.spark.excel") // Or .format("excel") for V2 implementation
    //.option("dataAddress", "'My Sheet'!B3:C35") // Optional, default: "A1"
    .option("header", "true") // Required
    .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
    //.option("emptyValues", "") // sets the string representation of an empty value. If None is set, it use the default value, ""
    //.option("setErrorCellsToFallbackValues", "true") // Optional, default: false, where errors will be converted to null. If true, any ERROR cell values (e.g. #N/A) will be converted to the zero values of the column's data type.
    .option("usePlainNumberFormat", "false") // Optional, default: false, If true, format the cells without rounding and scientific notations
    .option("inferSchema", "false") // Optional, default: false
    //.option("addColorColumns", "true") // Optional, default: false
    .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
    //.option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files (will fail if used with xls format files)
    //.option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
    //.option("workbookPassword", "pass") // Optional, default None. Requires unlimited strength JCE for older JVMs
    //.schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
    .load("./who-repo4covid-biblio.xlsx")

  df.show()

  df.printSchema()

  val df1 = df.withColumn("Author_1",col("Author"))

  df1.show()

  df1.describe("Year").show()
  df1.groupBy("Year").count().show()

  val df2: DataFrame = df1.groupBy("Journal").count()
  df2.printSchema()
  df2.select("count").show()

  df2.select("Journal", "count").orderBy(col("count").desc).foreach(row => println(s"+++ [${row.get(0)} | ${row.get(1)}]"))

  import spark.implicits._
  val df3 = df2.filter($"count" > 2 )

  //val df3 = df2.filter(col("count").gt(2))
  df3.show()

  val df4 = df3.groupBy("count").count()
  df4.show()

  df4.sort("count").show()

  val columns = Seq("Seqno", "Quote")
  val data = Seq(("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy.")
  )
  val xdf = data.toDF(columns: _*)
  xdf.show(false)

  val convertCase =  (strQuote:String) => {
    val arr = strQuote.split(" ")
    arr.map(f => f.substring(0, 1).toUpperCase + f.substring(1, f.length)).mkString(" ")
  }

  val convertUDF: UserDefinedFunction = udf(convertCase)

  xdf.select(col("Seqno"),
    convertUDF(col("Quote")).as("Quote")).show(false)

//See fill(value : scala.Predef.String) : org.apache.spark.sql.DataFrame
  def adjustString(in: String,
                   convertToNull: Boolean): String = {
    if (in == null) {
      if (convertToNull) in else ""
    } else {
      val str = in.trim
      if (str.isEmpty) {
        if (convertToNull) null else str
      } else str
    }
  }

  def ad(convertToNull: Boolean): UserDefinedFunction = udf((in: String) => adjustString(in, convertToNull))

  val df5: DataFrame = df.schema.fields.foldLeft[DataFrame](df) {
    case (dFrame, field) =>
      val colName = field.name
      //dFrame.withColumn(colName, adjustStrUDF(dFrame.col(colName)))
      dFrame.withColumn(colName, ad(false)(dFrame(colName)))
  }
  df5.show(true)

  println("Gravando conteúdo do dataframe no MongoDB ...")
  df5.write.format("mongodb").mode("overwrite").save()
  println("Tafefas finalizadas!")

  //sc.stop()

  //df.write.format("mongodb").mode("overwrite").save()
  df.write.format("mongodb").mode("append").save()
  spark.stop()
}
}
