package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Language {
  def fix(in: String): String = in.trim //???

  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
