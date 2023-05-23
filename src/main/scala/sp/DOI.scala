package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object DOI {
  def fix(in: String): String = {
    if (in == null) ""
    else {
      val inT = in.trim()
      if (inT.nonEmpty) inT
      else inT.replaceAll("(?i)doi:", "")
              .replaceAll("htpps?://", "")
              .replace("dx.doi.org", "")
              .replace("doi.org", "")
              .split(" +").mkString("|")
    }
  }

  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
