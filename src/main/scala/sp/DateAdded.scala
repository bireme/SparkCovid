package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object DateAdded {
  def fix(in: String): String = ???

  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
