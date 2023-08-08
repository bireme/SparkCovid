package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.{Failure, Success, Try}

object Pages {
  def fix(in: String): String = {

    val str = Option(in).getOrElse("").trim

    val regexE = ".*\\d+E\\d+.*".r
    val hasNumbersAroundE = regexE.matches(str)

    if (hasNumbersAroundE) {
      str.toDoubleOption match {
        case Some(value) => Try(value.toLong) match {
          case Success(value) => value.toString
          case Failure(_) => in
        }
        case None => in
      }
    }
    else str.trim
  }

  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
