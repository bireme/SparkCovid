package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Title {
  def fix(in: String): String = {
    if (in == null) ""
    else {
      val pattern = """\s*[|;]\s*""".r
      pattern.replaceAllIn(in.trim(), _ => "|")
    }
  }

  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
