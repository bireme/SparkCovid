package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Keywords {
  def fix(in: String): String = {
    if (in.startsWith("\"") && in.endsWith("\""))
      GeneralFix.fix(in.substring(1, in.length - 1).replaceAll("\"\"", "\""))
    else
      GeneralFix.fix(in)
  }
  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
