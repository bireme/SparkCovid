package sp

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object CovNum {
  def fix(in: String): String = {
    if (in == null) ""
    else {
        in.toDoubleOption match {
          case Some(value) => Try(value.toInt) match {
            case Success(value) => value.toString
            case Failure(_) => in
          }
          case None => in
        }
    }
  }
  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
