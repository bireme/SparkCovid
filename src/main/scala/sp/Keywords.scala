package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Keywords {
  def fix(in: String): String = GeneralFix.fix(in)

  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
