package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Abstract {
  def fix(in: String): String = Option(in.replaceAll("[\n\t]", " ").replaceAll(";", "|")).getOrElse("").trim

  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
