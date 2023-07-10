package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object AlternateId {

  def fix(in: String): String = {
    if (in.equals("_")) ""
    else in.trim
  }

  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
