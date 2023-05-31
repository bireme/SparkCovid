package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Authors {
  def fix(in: String): String = {

    val pattern = """\s*\|\s*""".r
    pattern.replaceAllIn(in.trim.replace(";", "|").replace(".,", ".|"), "|")
  }
  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
