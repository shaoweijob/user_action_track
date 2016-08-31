package org.tonypacer.product


import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.tonypacer.conf.ConfigurationManager
import org.tonypacer.constant.Constants
import org.tonypacer.util.AdClickRealTimeUtils


/**
  * Created by apple on 16/8/30.
  */
object AdClickRealTimeStatSpark {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PRODUCT)
      .setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    /**
      * create StreamingContext, entry of SparkStreaming
      *
      * 第二个参数batch interval
      *   每隔多少时间收集一次数据源中的数据,然后进行处理
      *
      */
    val ssc = new StreamingContext(sc, Seconds(5))

    /**
      *  obtain data from Kafka with direct api
      *
      */
    val brokerList = Constants.KAFKA_METADATA_BROKER_LIST

      // kafka 相关参数

    val kafkaParams = Map(brokerList -> ConfigurationManager.getProperty(brokerList))
    val topics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS)
      .split(",").toSet
    val adRealTimeLogDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,kafkaParams , topics).map(_._2)
    // 过滤黑名单用户
    val filteredAdRealTimeLogDStream = AdClickRealTimeUtils.filterByBlackList(adRealTimeLogDStream,sqlContext)

    // 将过滤好的数据 统计 并写入MySQL
    val dailyUserAdClickCountDStream = AdClickRealTimeUtils.CountAdClickRealTime(filteredAdRealTimeLogDStream)
    AdClickRealTimeUtils.AdClickRealTimeCount2MySQL(dailyUserAdClickCountDStream,sqlContext)

    // 更新黑名单
    AdClickRealTimeUtils.updateBlackList(dailyUserAdClickCountDStream,sqlContext)

    /**
      * 计算每天 各省 各城市 各广告的 点击量
      */
    ssc.checkpoint("hdfs://ctrl:9000/spark/checkpoint")
    val adRealTimeStatDStream = AdClickRealTimeUtils.calculateRealTimeStat(filteredAdRealTimeLogDStream)
    AdClickRealTimeUtils.calculateProvinceTop3Ad(adRealTimeStatDStream)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
