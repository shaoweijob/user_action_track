package org.tonypacer.util

import java.sql.{SQLException, DriverManager}
import java.util.{Date, Properties}

import org.apache.spark.sql.types.{LongType, StructField, StringType, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.dstream.DStream
import org.tonypacer.conf.ConfigurationManager
import org.tonypacer.constant.Constants
import org.tonypacer.jdbc.JDBCHelper

/**
  * Created by apple on 16/8/30.
  */
object AdClickRealTimeUtils{

  /**
    * 过滤 黑名单
    *
    * @param adRealTimeLogDStream
    * @param sqlContext
    * @return
    */
  def filterByBlackList(adRealTimeLogDStream: DStream[String], sqlContext: SQLContext): DStream[String] = {
    // 首先,从MySQL中查询黑名单,将其转换为RDD
    val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
    val prop = new Properties()
    prop.setProperty(Constants.JDBC_USER,ConfigurationManager.getProperty(Constants.JDBC_USER))
    prop.setProperty(Constants.JDBC_PASSWORD,ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
    val mappedBlackListRdd = sqlContext.read.jdbc(url,"black_list",prop).rdd.map(row => (row.getString(0),true))
    val mappedLogDStream = adRealTimeLogDStream.map(log => (log.split(" ")(3).toString,log))
    mappedLogDStream.transform(_.leftOuterJoin(mappedBlackListRdd))
        .filter(_._2._2.isEmpty)
        .map(_._2._1)
  }

  /**
    * 计算过滤好的 Dstream 点击次数,并映射字段,写入mysql
    * log格式: timestamp  province  city  userId  adId
    */
  def CountAdClickRealTime(filteredAdRealTimeLogDStream: DStream[String]): DStream[(String,Long)] ={
    // 将log数据 映射为 (date_userId_adId,1L) 这样的二元组
    // 将数据整理为(yyyyMMDD_userid_adid,1L)
    val dailyUserAdClickDStream = filteredAdRealTimeLogDStream.map(log => {
        val logSplited = log.split(" ")
        val timestamp = logSplited(0)
        val date = DateUtils.formatDate(new Date(timestamp.toLong))
        val userId = logSplited(3)
        val adId = logSplited(4)
        //concat
        (date + "_" + userId+ "_" + adId, 1L)
      })

    // 统计用户广告点击次数
    dailyUserAdClickDStream.reduceByKey(_ + _)
  }

  def AdClickRealTimeCount2MySQL(dailyUserAdClickCountDStream: DStream[(String,Long)],sqlContext: SQLContext): Unit = {
    val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
    val prop = new Properties()
    prop.setProperty(Constants.JDBC_USER,ConfigurationManager.getProperty(Constants.JDBC_USER))
    prop.setProperty(Constants.JDBC_PASSWORD,ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
    val rsDF = sqlContext.read.jdbc(url,"ad_user_click_count",prop)
  //========= 将统计好的数据存储到 MySQL
    //========= 表的结构:
    //========= 表名: ad_user_click_count 字段: date userId adId click_count stamps
    //========= primary key(date, userId, adId)
    dailyUserAdClickCountDStream.foreachRDD(_.foreach(tuple => {
      val splitedStr = tuple._1.split("_")
      val date = splitedStr(0)
      val userId = splitedStr(1)
      val adId = splitedStr(2)
      val count = tuple._2
      val rsCount = rsDF.filter("date =" + date + " AND user_id = " + userId +" AND adId = " + adId).select("click_count")
        .toString().toLong
      val totalCount = count + rsCount
      val sqlStr =
        """
          REPLACE INTO ad_user_click_count(date,userId,adId,click_count,stamps)
          VALUES(?,?,?,?,unix_timestamp(now()) )
        """
      val params = (date::userId::adId::adId::totalCount::Nil).toArray[AnyRef]
      JDBCHelper.getInstance().executeUpdate(sqlStr,params)
    }))
  }

  /**
    * 将新的 恶意用户 更新到黑名单(MySQL)
    */
  def updateBlackList(dailyUserAdClickCountDStream: DStream[(String,Long)],sqlContext: SQLContext): Unit ={
    val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
    val prop = new Properties()
    prop.setProperty(Constants.JDBC_USER,ConfigurationManager.getProperty(Constants.JDBC_USER))
    prop.setProperty(Constants.JDBC_PASSWORD,ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
    val rsDF = sqlContext.read.jdbc(url,"ad_user_click_count",prop)
    // 筛选出 新产生的 恶意用户(DStream 的 Count数 + MySQL的 Count数 >= 100)
    val blackListDStream = dailyUserAdClickCountDStream.filter(tuple => {
      val splitedStr = tuple._1.split("_")
      val date = splitedStr(0).toLong
      val userId = splitedStr(1)
      val adId = splitedStr(2)
      val rsCount = rsDF.filter("date =" + date + " AND user_id = " + userId +" AND adId = " + adId).select("click_count")
        .toString().toLong
      if (tuple._2.toLong + rsCount >= 100) true else false
    })
    val distinctedBlackListUserIdDStream = blackListDStream.map(_._1.split("_")(1).toLong).transform(_.distinct())
    distinctedBlackListUserIdDStream.foreachRDD(_.foreach(userId => {
      val sqlStr =
        """
          INSERT INTO black_list(user_id) VALUES(?)
        """
      val params = (userId::Nil).toArray[AnyRef]
      JDBCHelper.getInstance().executeUpdate(sqlStr,params)
    }))
  }

  def calculateRealTimeStat(filteredAdRealTimeLogDStream: DStream[String]): DStream[(String, Long)]={
    val mappedDStream = filteredAdRealTimeLogDStream.map(log => {
      val logSplited = log.split(" ")
      val timestamp = logSplited(0)
      val date = new Date(timestamp.toLong)
      val dateKey = DateUtils.formatDateKey(date)
      val province = logSplited(1)
      val city = logSplited(2)
      val adId = logSplited(4).toLong
      (dateKey + "_" + province +"_" + city + "_" +adId, 1L)
    })

    val aggregatedDStream = mappedDStream
      .updateStateByKey((values: Seq[Long], state: Option[Long]) => {
        // 传输进来的值，累加
        val currentCount = values.sum

        // 以前状态的值
        val previousCount = state.getOrElse(0L)

        // return
        Some(currentCount + previousCount)
      })

    aggregatedDStream.foreachRDD(_.foreachPartition(iterator => {
      // 使用 jdbc 或 dataframe存储
    }))
    // return
    aggregatedDStream
  }

  /**
    * 计算每天各省份的Top 3 热门广告
    * (使用技术 SparkSQL ROW_NUMBER)
    */
  def calculateProvinceTop3Ad(adRealTimeStatDStream: DStream[(String,Long)]): Unit ={
    /**
      * DStream[(dateKey + "_" + province +"_" + city + "_" +adId, sum)]
      *
      * 计算出每省 广告点击 Top3
      *
      * 并 存入数据库
      *
      */
    val rowsDStrem = adRealTimeStatDStream.transform(rdd => {
      // 将RDD 映射为 (date_province_adId, sum)形式 的二元组 Tuple2[Stirng,Long]
      val mappedRdd = rdd.map(tuple => {
        val strSplited = tuple._1.split("_")
        val date = strSplited(0)
        val province =strSplited(1)
        val adId = strSplited(3)
        val sum = tuple._2
        (date+ "_" + province + "_" + adId, sum)
      })

      // reduceByKey 进行统计
      val dailyAdClickCountByProvinceRdd = mappedRdd.reduceByKey(_ + _)

      // 将上述计算的RDD转换为DataFrame,使用SparkSQL完成

      val rowRdd = dailyAdClickCountByProvinceRdd.map(tuple => {
        val keySplited = tuple._1.split("_")
        val date = keySplited(0)
        val province = keySplited(1)
        val adId = keySplited(2).toLong
        val clickCount = tuple._2
        Row(date, province, adId,clickCount)
      })

      val schema = StructType(
          StructField("date",StringType,true)::
          StructField("province",StringType,true)::
          StructField("ad_id",LongType,true)::
          StructField("click_count",LongType,true)::Nil
      )

      // create SQLContext
      val sqlContext = new SQLContext(rdd.context)
      import sqlContext.implicits
      val dailyAdClickCountByProvinceDF = sqlContext.createDataFrame(rowRdd,schema)
      dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov")

      //使用SparkSQL 执行 SQL 语句,配合使用窗口函数,统计各省份的Top3热门广告\
      val sqlStr =
        """
          SELECT
            date, province, ad_id, click_count
          FROM(
            SELECT
              date, province, ad_id, click_count,
              ROW NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) rank
            FROM
              tmp_daily_ad_click_count_by_prov
          ) t
          WHERE
            t.rank <=3
        """
      val provinceTop3DF = sqlContext.sql(sqlStr)
      provinceTop3DF.rdd

      // 如果使用SparkSQL中的外部数据源的方式,在此就可以将结果查询到RDBMS表中
      // provinceTop3DF.write.save("overwirte").jdbc(url,table,properties)
    })

    /**
      * rowsDStream
      * 是每次刷新出来的各个省份最热门的Top3广告,批量插入到RDBMS表中
      *
      * 表的结构:
      *   ad_province_top3
      *     date
      *     province
      *     ad_id
      *     click_count
      *
      */
    rowsDStrem.foreachRDD(rdd => {
      // JDBC 方式插入
      rdd.foreachPartition(iters => {
        // ...

        classOf[com.mysql.jdbc.Driver]
        val connStr =  "jdbc:mysql://localhost:3306/DBNAME?user=DBUSER&password=DBPWD"
        val sqlStr =
          """
             REPLACE TABLE ad_province_top3 values(?,?,?,?)
          """
        val conn = DriverManager.getConnection(connStr)
        val ps = conn.prepareStatement(sqlStr)
        try {
          for (iter <- iters) {
            val date = iter.getString(0)
            val province = iter.getString(1)
            val adId = iter.getLong(2)
            val clickCount = iter.getLong(3)
            ps.setString(1, date)
            ps.setString(2, province)
            ps.setLong(3, adId)
            ps.setLong(4, clickCount)
            ps.addBatch()
          }
          ps.executeBatch()
          conn.commit()
        } catch {
          case SQLException  => new SQLException().printStackTrace()
        } finally {
          ps.close()
          conn.close()
        }
      })
    })

  }
}