package util

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.Row

object HotelUtils {

  val NO_CHILDREN = "no children"
  val WITH_CHILDREN = "with children"

  val SHORT_STAY = "short stay"
  val STANDARD_STAY = "standard stay"
  val STANDARD_EXTENDED_STAY = "standard extended stay"
  val LONG_STAY = "long stay"
  val INCORRECT_STAY = "incorrect stay"

  val short_stay_id: Char = '0'
  val standard_stay_id = '1'
  val standard_extended_stay_id = '2'
  val long_stay_id = '3'
  val incorrect_stay_id = '5'

  val visit_types = Map[Char, String](
    short_stay_id -> SHORT_STAY,
    standard_stay_id -> STANDARD_STAY,
    standard_extended_stay_id -> STANDARD_EXTENDED_STAY,
    long_stay_id -> LONG_STAY,
    incorrect_stay_id -> INCORRECT_STAY
  )

  val no_value = "-"

  val date_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def createPair(rec: Row): (String, Double) = {
    val hotel_id_n = 1 //hotel_id
    val srch_children_cnt_n = 2 //srch_children_cnt
    val srch_ci_n = 3 //srch_ci
    val srch_co_n = 4 //"rch_co
    val hid = rec.get(hotel_id_n)
    val child = rec.get(srch_children_cnt_n)
    val ci = rec.get(srch_ci_n)
    val co = rec.get(srch_co_n)

    createPair(hid, child, ci, co)
  }

  def createPair(rec: GenericRecord): (String, Double) = {
    val hid = rec.get("hotel_id")
    val child = rec.get("srch_children_cnt")
    val ci = rec.get("srch_ci")
    val co = rec.get("srch_co")

    createPair(hid, child, ci, co)
  }

  def createPairInt(rec: Row): (String, Int) = {
    val tuple = createPair(rec)
    (tuple._1, 1)
  }

  def createPairInt(rec: GenericRecord): (String, Int) = {
    val tuple = createPair(rec)
    (tuple._1, 1)
  }

  def createPair(hid: Any, child: Any, ci: Any, co: Any) =
    (createKey(hid, child, ci, co), 1.0)

  def createKey(hid: Any, child: Any, ci: Any, co: Any): String = {
    val chKey = child match {
      case iv: Integer => if (iv > 0) "1" else "0"
      case st: String => if (st.toInt > 0) "1" else "0"
      case _ => "0"
    }

    def dateDiffKey(oldDate: LocalDate, newDate: LocalDate) = {
      val diff = newDate.toEpochDay() - oldDate.toEpochDay()
      val res = if (diff < 0) incorrect_stay_id
        else if (diff <= 2) short_stay_id
          else if (diff <= 7) standard_stay_id
            else if (diff <= 14) standard_extended_stay_id
              else if (diff <= 28) long_stay_id else incorrect_stay_id

      res
    }

    val stayKey = (ci, co) match {
      case (cis: CharSequence, cos: CharSequence) =>
        val oldDate = LocalDate.parse(cis, date_formatter)
        val newDate = LocalDate.parse(cos, date_formatter)
        dateDiffKey(oldDate, newDate)
      /*case (cis: String, cos: String) => {
        val oldDate = LocalDate.parse(cis, date_formatter)
        val newDate = LocalDate.parse(cos, date_formatter)
        dateDiffKey(oldDate, newDate)
      }*/
      case _ => incorrect_stay_id
    }

    hid + "_" + stayKey + "_" + chKey
  }

  def createKey(row: Row): String = {
    val hotel_id_n = 0 //hotel_id
    val srch_children_cnt_n = 16 //srch_children_cnt
    val srch_ci_n = 13 //srch_ci
    val srch_co_n = 14 //"rch_co
    val hid = row.get(hotel_id_n)
    val child = row.get(srch_children_cnt_n)
    val ci = row.get(srch_ci_n)
    val co = row.get(srch_co_n)

    createKey(hid, child, ci, co)
  }

  def getChildrenDesc(x: String): String = {
    if (x == null) no_value else if (x.length < 1) no_value
    else x.charAt(x.length - 1) match {
      case '0' => NO_CHILDREN
      case '1' => WITH_CHILDREN
      case _ => no_value
    }
  }

  def getVisittypeDesc(x: String): String = {
    if (x == null) no_value else if (x.length < 3) no_value
    else
      visit_types.get(x.charAt(x.length - 3)).getOrElse(no_value)
  }

  def getHotelId(x: String): String = {
    if (x == null) no_value else if (x.length < 4) no_value
    else x.substring(0, x.length - 4)
  }
}
