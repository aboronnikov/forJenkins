import org.scalatest.FunSuite
import util.HotelUtils

class TestHotelUtils extends FunSuite {

  val hotel_key_example1 = "1254130450434_0_0"
  val hotel_key_example2 = "3109556322314_1_1"

  test("HotelUtils.getChildrenDesc") {
    assert(HotelUtils.getChildrenDesc(hotel_key_example1) == util.HotelUtils.NO_CHILDREN)
    assert(HotelUtils.getChildrenDesc(hotel_key_example2) == util.HotelUtils.WITH_CHILDREN)
    assert(HotelUtils.getChildrenDesc(null) == util.HotelUtils.no_value)
  }

  test("HotelUtils.getVisittypeDesc") {
    assert(HotelUtils.getVisittypeDesc(hotel_key_example1) == util.HotelUtils.SHORT_STAY)
    assert(HotelUtils.getVisittypeDesc(hotel_key_example2) == util.HotelUtils.STANDARD_STAY)
    assert(HotelUtils.getVisittypeDesc("uuu_2_0") == util.HotelUtils.STANDARD_EXTENDED_STAY)
    assert(HotelUtils.getVisittypeDesc("uuu_3_0") == util.HotelUtils.LONG_STAY)
    assert(HotelUtils.getVisittypeDesc("uuu_5_0") == util.HotelUtils.INCORRECT_STAY)
    assert(HotelUtils.getVisittypeDesc("w") == util.HotelUtils.no_value) //length < 2 - incorrect format
  }

  test("HotelUtils.getHotelId") {
    assert(HotelUtils.getHotelId(hotel_key_example1) == "1254130450434")
    assert(HotelUtils.getHotelId("12") == util.HotelUtils.no_value) //length < 4 - incorrect format
  }
}
