package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.{Failure, Success, Try}

object AccessionNumber {

  def fix(in: String): String = {

    val regex = "(?<!\\s)\\.(?!\\s|$)(?=[A-Za-z])".r
    val str3 = GeneralFix.fix(regex.replaceAllIn(in, ""))

    val regexE = ".*\\d+E\\d+.*".r
    val hasNumbersAroundE = regexE.matches(str3)

    if (hasNumbersAroundE)
      str3.toDoubleOption match {
        case Some(value) => Try(value.toInt) match {
          case Success(value) => value.toString
          case Failure(_) => in
        }
        case None => in
      }
    else str3.trim
  }
  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}