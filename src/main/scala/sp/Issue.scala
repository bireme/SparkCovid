package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Issue {
  def fix(in: String): String = GeneralFix.fix(in, isNumber = true)

  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
