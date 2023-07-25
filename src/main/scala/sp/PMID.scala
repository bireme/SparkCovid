package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object PMID {
  def fix(in: String): String = GeneralFix.fix(in)

  def change(in: String): String = {
    val textIn: String = Option(in).getOrElse("").trim

    val pmid = if (textIn.contains("-")) ""
    else if (textIn.contains(":")) {
      if (textIn.contains("MEDLINE")) textIn.replace("MEDLINE:", "")
      else ""
    } else textIn

    pmid.trim
  }
  val _udf: UserDefinedFunction = udf((in: String) => fix(change(in)))
}