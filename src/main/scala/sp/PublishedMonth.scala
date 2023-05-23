package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.joda.time.DateTime

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.util.{Failure, Success, Try}

object PublishedMonth {
  private def fix(in: String): String = GeneralFix.fix(in)

  private def change(in: String): String = {
    Try(new SimpleDateFormat("dd/MM/yyyy").parse(in.trim))
      .orElse(Try(new SimpleDateFormat("MM/dd/yyyy").parse(in.trim))) match {
      case Success(pDate) =>
        val now: Date = Calendar.getInstance.getTime
        if (pDate.after(now)) ""
        else {
          val datetime: DateTime = new DateTime(pDate)
          datetime.toString("MM")
        }
      case Failure(_) => ""
    }
  }

  val _udf: UserDefinedFunction = udf((in: String) => fix(change(in)))
}