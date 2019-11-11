import HotelVisitsTransformer.buildSession
import org.apache.log4j.Logger

object HotelVisitsDescriber {
  val log: Logger = Logger.getLogger(HotelVisitsDescriber.getClass)

  def main(args: Array[String]): Unit = {
    val sess = buildSession()

    val results = sess.sqlContext.sql("select *, row_number() OVER (PARTITION BY hotel_id, children ORDER BY vCounts desc) as rn " +
     "from expedia_visit_results order by hotel_id, children, vCounts")

   log.info("Now showing visit type stats:")
   results.show()
   results.createOrReplaceTempView("type_results")

    sess.stop()
  }
}
