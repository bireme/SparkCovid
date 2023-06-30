package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Title {
  def fix(in: String): String = {
    if (in == null) ""
    else {
      val pattern = """\s*[|;]\s*""".r
      val pIn = pattern.replaceAllIn(in.trim(), _ => "|")
      if (pIn.startsWith("\"") && pIn.endsWith("\""))
        pIn.substring(1, pIn.length - 1)
      else
        pIn
    }
  }

  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
