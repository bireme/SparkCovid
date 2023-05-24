package sp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object FulltextLink {
  def fix(doi: String,
          pmid:String): String = {
    val DOI = Option(doi).getOrElse("").trim
    val flt = DOI.replace("DOI:", "").trim
    val PMID = Option(pmid).getOrElse("").trim

    if (flt.isEmpty) {
      if (PMID.isEmpty) ""
      else "https://pesquisa.bvsalud.org/portal/resource/en/mdl-" + PMID
    } else if (flt.contains("http")) flt
      else if (flt.contains("doi.org")) "https://" + DOI
      else "https://doi.org/" + DOI
  }

  val _udf: UserDefinedFunction = udf((doi: String, pmid: String) => fix(doi, pmid))
}
