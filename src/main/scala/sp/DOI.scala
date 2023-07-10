package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object DOI {
  def fix(in: String): String = {
    if (in == null) ""
    else {
      val inT = in.replaceAll("~", "-").trim()
      if (inT.nonEmpty) inT.split(" ")(0)
      else inT.replaceAll("(?i)doi:", "")
              .replaceAll("htpps?://", "")
              .replace("dx.doi.org", "")
              .replace("doi.org", "")
              .split(" +").mkString("|").split(" ")(0)
    }
  }
  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
