import java.util.Properties

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.Logger
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.StatCounter
import org.apache.spark.{SparkConf, SparkContext}
import util.HotelUtils

object HotelStatsFromStream {
  val log: Logger = Logger.getLogger(HotelStatsFromStream.getClass)

  val broker_list = "sandbox-hdp.hortonworks.com:6667"
  val kafka_output_topic = "spark_streaming_expedia"

  val inputDir: String = "/user/maria_dev/parquetTemp"

  val folderWithExpedia2016Data = "/user/maria_dev/expedia_patitioned_true/year_in=2016"

  def main(args: Array[String]): Unit = {

    val sess = buildSession()
    val ssc = new StreamingContext(sess.sparkContext, Seconds(10))
    ssc.checkpoint("/user/maria_dev/checkpt")

    log.info("We are starting transforms")

    ssc.sparkContext.hadoopConfiguration.set("parquet.read.support.class", "org.apache.parquet.avro.AvroReadSupport")

    val weatherDF = sess.sqlContext.sql("select * from hotels_and_weather_externl where avg_tmpr_c > 0")

    val dataStream = prepareExpediaStream(sess, ssc)

    val rddInit = createInitialExpediaRdd(sess, weatherDF)

    val state = dataStream.mapWithState(StatefulStats.state.initialState(rddInit))
    state.print()

    outputToKafka(state)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

  /**
   * Outputs collected visit stats to special kafka-topic
   * @param state
   */
  def outputToKafka(state: MapWithStateDStream[String, Double, StatCounter, (String, StatCounter)]): Unit = {
    state.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val props: Properties = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker_list)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)
        partition.foreach(record => {
          val data = "{\"count\":\"" + record._2.count.toString() + "\", \"hotelKey\":\"" + record._1.toString() + "\"}"
          val message = new ProducerRecord[String, String](kafka_output_topic, null, data)
          producer.send(message)
        })
        producer.close()
      })
    }
    )
  }

  /**
   * Reads expedia data from 2016 year as static data for initial stats rdd.
   * This rdd becomes a parameter for method org.apache.spark.streaming.StateSpec#initialState(org.apache.spark.api.java.JavaPairRDD)
   *
   * @param sess
   * @param weatherDF
   * @return
   */
  def createInitialExpediaRdd(sess: SparkSession, weatherDF: DataFrame): RDD[(String, StatCounter)] = {

    import sess.implicits._

    def createAndMergeCounter(tupl: (String, Int)): (String, StatCounter) = {
      val sc = new StatCounter(List(1.0))
      //there is no way to increment counter in StatCounter except merge method. So we call merge n times to set count to n.
      //Strarting from 2 because every merge gives increment for 1. But 1 should not be incremented!
      2 to tupl._2 foreach { _ => sc.merge(1.0) }

      (tupl._1, sc)
    }

    val expedia2016 = sess.sqlContext.read.parquet(folderWithExpedia2016Data)
    val filtered2016 = expedia2016.join(weatherDF, "hotel_id").where($"wthr_date" === $"srch_ci")

    val pairs = filtered2016.map(rec => (HotelUtils.createKey(rec), 1)).rdd.reduceByKey((x, y) => x + y)
    pairs.toDF().show(35)
    val rddInit = pairs.map(rec => createAndMergeCounter(rec))

    rddInit
  }

  /**
   * Reads expedia data for 2017 year as spark streaming data
   *
   * @param sess
   * @param ssc
   * @return
   */
  def prepareExpediaStream(sess: SparkSession, ssc: StreamingContext): DStream[(String, Double)] = {
    val stream = ssc.fileStream[Void, GenericRecord, ParquetInputFormat[GenericRecord]](inputDir, { path: Path => path.toString.endsWith("c000") }, false,
      ssc.sparkContext.hadoopConfiguration)

    stream.transform(rdd => rdd.map(tuple => tuple._2))
    val schema = StructType(
      StructField("id", LongType, true) ::
        StructField("user_id", IntegerType, true) ::
        StructField("srch_ci", StringType, true) ::
        StructField("srch_co", StringType, true) ::
        StructField("srch_adults_cnt", IntegerType, true) ::
        StructField("srch_children_cnt", IntegerType, true) ::
        StructField("hotel_id", LongType, true) :: Nil)

    stream.print()

    import sess.implicits._
    stream.transform(rdd => {
      val rdd1 = rdd.map(record => record.toString)
      val sqlContext = SQLContextSingleton.getInstance(rdd1.sparkContext)

      sqlContext.read.schema(schema).json(rdd1.toDS()).rdd
    }
    )

    val dataStream = stream.transform(rdd => rdd.map(rec => HotelUtils.createPair(rec._2)))

    dataStream
  }

  /**
   * Builds the spark session for processing the DataFrame.
   *
   * @return new SparkSession.
   */
  def buildSession(): SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName("HotelsStreamTask").setMaster("local[4]")
      .set("spark.sql.warehouse.dir", "/apps/spark/warehouse")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.avro.generic.GenericData.Record],
        classOf[org.apache.spark.util.StatCounter]
      ))
    SparkSession.builder()
      .config(sparkConf).enableHiveSupport().getOrCreate()
  }

  /**
   * Lazily instantiated singleton instance of SQLContext.
   */
  object SQLContextSingleton {
    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }
}



