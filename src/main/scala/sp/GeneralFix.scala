package sp

object GeneralFix {
  def fix(in: String,
          isNumber: Boolean = false): String = {
    Option(in).getOrElse("").trim match {
      case "" => ""
      case str =>
        val str2 = if (isNumber) str.replace(".0", "") else str
        str2.replaceAll(" *[;|] *", "|")
    }
  }
}