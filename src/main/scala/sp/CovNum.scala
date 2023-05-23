package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object CovNum {
  def fix(in: String): String = {
    if (in == null) ""
    else {
      val inT = in.trim()
      if (inT.nonEmpty) inT
      else inT.replace(".0", "")
    }
  }

  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
