package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object UNKNOWN {
  def fix(in: String): String = GeneralFix.fix(in)

  def change(in: String): String = {
    val text: String = Option(in).getOrElse("").trim

    if (text.contains("-") && !text.contains("SCIELO")) text
    else ""
  }

  val _udf: UserDefinedFunction = udf((in: String) => fix(change(in)))
}