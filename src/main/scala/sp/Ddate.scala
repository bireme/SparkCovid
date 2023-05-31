package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Ddate {
  def fix(in: String): String = {

    val parts: Array[String] = in.split("-")

    val day = parts(0)
    val month = parts(1)
    val year = "20" + parts(2)

    s"$day/$month/$year"
  }

  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}