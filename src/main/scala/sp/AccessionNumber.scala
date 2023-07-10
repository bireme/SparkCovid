package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object AccessionNumber {

  val regex = "(?<!\\s)\\.(?!\\s|$)(?=[A-Za-z])".r

  def fix(in: String): String ={

    val str3 = GeneralFix.fix(regex.replaceAllIn(in, ""))
    val regexE = ".*\\d+E\\d+.*".r
    val hasNumbersAroundE = regexE.matches(str3)

    if (hasNumbersAroundE) str3.split("E")(0).replace(".", "") else str3.trim
  }
  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}