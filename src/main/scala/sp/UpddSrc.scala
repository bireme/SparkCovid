package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.util.{Date, Locale}
import java.text.SimpleDateFormat
import scala.util.{Failure, Success, Try}


object UpddSrc {
  /*private val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  private val sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  private val sdf3 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS'Z'")          // 2022-12-26T18:10:30.74Z
  private val sdf4 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")          // 2022-12-26T18:10:30.742Z
  private val sdf5 = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss.SSS'Z'")          // 2022-12-26T18-10-30.742Z
  private val sdf6 = new SimpleDateFormat("EEE MMM dd HH:mm:ssz yyyy", Locale.US)  // "Mon Feb 27 15:10:24 BRT 2023"
  private val sdf7 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.US)  // "Mon Feb 27 15:10:24 BRT 2023"
*/
  def fix(in: String): java.sql.Date = {
    lazy val inT: String = Option(in).getOrElse("").trim
    lazy val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    lazy val sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    lazy val sdf3 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS'Z'") // 2022-12-26T18:10:30.74Z
    lazy val sdf4 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") // 2022-12-26T18:10:30.742Z
    lazy val sdf5 = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss.SSS'Z'") // 2022-12-26T18-10-30.742Z
    lazy val sdf6 = new SimpleDateFormat("EEE MMM dd HH:mm:ssz yyyy", Locale.US) // "Mon Feb 27 15:10:24 BRT 2023"
    lazy val sdf7 = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.US) // "Mon Feb 27 15:10:24 BRT 2023"

    lazy val t1 = Try(sdf1.parse(inT))
    lazy val t2 = Try(sdf2.parse(inT))
    lazy val t3 = Try(sdf3.parse(inT))
    lazy val t4 = Try(sdf4.parse(inT))
    lazy val t5 = Try(sdf5.parse(inT))
    lazy val t6 = Try(sdf6.parse(inT))
    lazy val t7 = Try(sdf7.parse(inT))

    val date = t1.orElse(t2).orElse(t3).orElse(t4).orElse(t5).orElse(t6).orElse(t7) match {
      case Success(dt) => dt
      case Failure(exception) =>
        //exception.printStackTrace()
        println(s"################ Error during date string: $inT exception=$exception")
        new Date()
    }
    Try(new java.sql.Date(date.getTime)) match {
      case Success(dt) => dt
      case Failure(exception) =>
        println(s"@@@@@@@@@@@@@@@@ Error during date sql:$inT ${exception.toString}")
        new java.sql.Date(0)
    }
  }

  val _udf: UserDefinedFunction = udf((in: String) => fix(in))
}
