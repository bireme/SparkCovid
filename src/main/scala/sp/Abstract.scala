package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Abstract {

  val pattern = """\s*\|\s*""".r
  def fix(in: String): String = Option(pattern.replaceAllIn(in.replaceAll("[\n\t]", " ").replaceAll(";", "|"), "|")).getOrElse("").trim

  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}