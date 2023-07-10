package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object PMID {
  def fix(in: String): String = GeneralFix.fix(in)

  def change(in: String): String = {
    val textIn: String = Option(in).getOrElse("").trim
    val text = if (textIn.contains("|")) textIn.split("|")(0) else textIn

    val pmid = if (text.contains("-")) ""
    else if (text.contains(":")) {
      if (text.contains("MEDLINE")) text.replace("MEDLINE:", "")
      else ""
    } else text

    if (pmid.contains("E")) pmid.split("E")(0).replace(".", "").trim else pmid.trim
  }
  val _udf: UserDefinedFunction = udf((in: String) => fix(change(in)))
}
