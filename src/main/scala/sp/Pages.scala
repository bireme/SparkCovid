package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Pages {
  def fix(in: String): String = Option(in).getOrElse("").trim

  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
