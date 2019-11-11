import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Row, SparkSession}

object HotelVisitsTransformer {
  val log: Logger = Logger.getLogger(HotelVisitsTransformer.getClass)

  def main(args: Array[String]): Unit = {
    val sess = buildSession()

    val startData = sess.sqlContext.sql("select max(count) as vCount, hotelKey from expedia_visits_stats1 group by hotelKey")

    val getChildrenDesc = udf((x: String) => util.HotelUtils.getChildrenDesc(x))
    val getVisittypeDesc = udf((x: String) => util.HotelUtils.getVisittypeDesc(x))
    val getHotelId = udf((x: String) => util.HotelUtils.getHotelId(x))
    val transcriptedData = startData.withColumn("children", getChildrenDesc(col("hotelKey")))
      .withColumn("visitType", getVisittypeDesc(col("hotelKey")))
      .withColumn("hotelId", getHotelId(col("hotelKey")))

    transcriptedData.createOrReplaceTempView("hotel_visit_types")

    sess.sqlContext.sql("insert into table expedia_visit_results select hotelId, visitType, children, vCount from hotel_visit_types")

    transcriptedData.show()

    sess.stop()
  }

  /**
   * Builds the spark session for processing the DataFrame.
   *
   * @return new SparkSession.
   */
  def buildSession(): SparkSession = {
    SparkSession.builder()
      .appName("SparkExpedia")
      .config("spark.sql.warehouse.dir", "/apps/spark/warehouse")
      .enableHiveSupport()
      .getOrCreate()
  }
}
