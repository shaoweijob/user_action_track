package org.tonypacer.product

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.tonypacer.conf.ConfigurationManager
import org.tonypacer.constant.Constants
import org.tonypacer.dao.factory.DAOFactory
import org.tonypacer.util.ParamUtils

import scala.collection.mutable.ListBuffer

/**
  * Created by apple on 16/8/29.
  */
object AreaTopProductSpark {

  def main(args: Array[String]) {

    /**
      * 01. 获取 Spark Application 执行时,客户从页面传递的参数,解析成JSONObject格式
      */

    // 首先获取taskId
    val taskId = ParamUtils.getTaskIdFromArgs(args)
    // 获取Task任务的参数
    val taskDao = DAOFactory.getTaskDAO
    val task = taskDao.findByTaskId(taskId)
    // 解析JSON格式数据
    val taskParam = ParamUtils.getTaskParam(task)



    /**
      * 02. 创建SparkContext SparkSQL
      */
    val conf = new SparkConf()
      .setAppName(Constants.SPARK_APP_NAME_PRODUCT)
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)    //HiveContext
    import sqlContext.implicits._           //隐式转换 把 RDD转换为DataFrame

    /**
      * 注意,在企业中通常在创建SQLContext实例以后,就注册自定义的函数(UDF/UDAF)
      */
    sqlContext.udf.register(
      "concat_long_string",
      (cityId: Long, cityName: String, delimiter: String) => cityId + delimiter + cityName
    )

    sqlContext.udf.register(
      "group_concat_distinct",
      GroupConcatDistinct
    )

    sqlContext.udf.register(
      "get_json_object",
      (json: String,field: String) => JSON.parseObject(json).getString(field)
    )
    /**
      * 03. 查询用户日期范围内的点击行为数据
      */
    val startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)
    val cityId2ClickActionRdd = getActionRddByDateRange(sqlContext, startDate, endDate)

    /**
      * 04. 从MySQL中查询城市信息表city_info
      */
    val cityId2CityInfoRdd = getCityInfoRdd(sqlContext)

    /**
      * 05. 对 用户访问行为数据 和 城市信息数据 进行关联
      *
      * RDD 之间,如果进行 join的话, 其类型必须是(key,value),其实key就是关联的字段
      * join之后,注册为一个临时表
      */
    generateTempClickProductBasicTable(sqlContext,cityId2ClickActionRdd,cityId2CityInfoRdd)

    /**
      * 06. 统计出各个区域商品点击次数
      */
    generateTempAreaProductClickCountTable(sqlContext: SQLContext)

    /**
      * 07. join出商品的完成信息
      */
    generateTempAreaFullProductClickCountTable(sqlContext: SQLContext)

    /**
      * 08. 获取各区域Top10热门商品
      *
      */
    val areaTop10ProductRdd = getAreaTop10ProductRdd(sqlContext)

    /**
      * 09. 将结果集持久化到外部存储系统中,比如MySql/HBase/Redis,
      * 这里持久化到mysql
      */
    persistAreaTop10Product(taskId,areaTop10ProductRdd.collect(),sc)

    sc.stop()
  }

  //==================================================================================
  /**
    * 通过日期范围 得到一个RDD
    *
    * @param sqLContext
    * @param startDate
    * @param endDate
    * @return
    */
  //==================================================================================

  def getActionRddByDateRange(sqLContext: SQLContext, startDate: String, endDate: String): RDD[(Long,Row)] = {
    // 此时,要注意,click_product_id不可以为空,
    // 这样可以避免数据倾斜
    val sqlStr =
    """
       SELECT
         city_id, click_product_id product_id
       FROM
         user_visit_action
       WHERE
         click_product_id IS NOT NULL
       AND
         click_product_id != 'null'
       AND
         click_product_id != "NULL"
    """ +
    "action_time >= '" + startDate + "' AND " +  "action_time <= '" + endDate +"'"
    //查询
    val click_action_df = sqLContext.sql(sqlStr)
    click_action_df.map(row => (row.getLong(0), row))
  }

  //==================================================================================
  /**
    * 从MySQL中读取表 city_info, 将city_info的数据以 RDD[(Long, Row)]的格式返回
    *
    * @param sqLContext
    * @return
    */
  def getCityInfoRdd(sqLContext: SQLContext): RDD[(Long, Row)] = {

    val url = ConfigurationManager.getProperty(Constants.SPARK_SQL_JDBC_URL)
    val properties  = new Properties()
    val city_info_df = sqLContext.read.jdbc(url,"city_info",properties)
    city_info_df.map(row => (row.getLong(0), row))
  }

  //==================================================================================
  /**
    * 进行join,生成点击商品信息的临时表
    *
    * @param sqLContext
    * @param cityId2ClickActionRdd
    * @param cityId2CityInfoRdd
    */

  def generateTempClickProductBasicTable(sqLContext: SQLContext,
                                         cityId2ClickActionRdd: RDD[(Long,Row)],
                                         cityId2CityInfoRdd: RDD[(Long,Row)]): Unit ={
    /**
      * 依据city_id进行join
      * join之后, RDD[(cityId,(clickActionRow,cityInfoRow))]
      */
    val joinedRdd = cityId2ClickActionRdd
                    .join(cityId2CityInfoRdd)
                    .map(tuple => {
                      val cityId = tuple._1
                      val clickAction = tuple._2._1
                      val cityInfo = tuple._2._2

                      val productId = clickAction.getString(1)
                      val cityName = cityInfo.getString(1)
                      val area = cityInfo.getString(2)

                      Row(cityId,cityName,area,productId)
                    })

    /**
      * 得到了RDD后, 将RDD -> DataFrame,(通过自定义schema)
      */
    val schema = StructType(
        StructField("city_id",LongType,true)::
        StructField("cityName",StringType,true)::
        StructField("area",StringType,true)::
        StructField("productId",LongType,true)::Nil
    )

    /**
      * 创建DataFrame
      */
    val df = sqLContext.createDataFrame(joinedRdd, schema)

    // 将DataFrame注册为临时表,以便使用SQL进行分析
    df.registerTempTable("temp_clk_prod_basic")
  }

  /**
    * 生成各区域各商品点击次数
    *
    * @param sqlContext
    */
  def generateTempAreaProductClickCountTable(sqlContext: SQLContext): Unit ={
    /**
      * 分析:
      *     按照area 和 productId 进行分组,统计
      *
      *     city_id,city_name -> 组合
      *     例如:
      *       1,上海  -> 1:上海
      *       2,北京  —> 2:北京
      *
      *       聚合函数
      *         多 对 一
      *
      *      实现上述步骤,分两步走:
      *      第一步: concat_long_string: 将city_id 和 city_name 进行合并在一起(UDF)
      *      第二步: group_concat_distinct 组合所有的城市信息,并且有个去重(UDAF)
      *
      *
      *
      */
    val sqlStr =
      """
        SELECT
          area, product_id,
          COUNT(*) click_count
          group_concat_distinct(concat_long_string(city_id,city_name,";")) city_infos
        FROM
          temp_clk_prod_basic
        GROUP BY
          area,product_id
      """

    /**
      * SparkSql 执行
      */
    val df = sqlContext.sql(sqlStr)
    df.registerTempTable("temp_area_product_click_count")
  }

  /**
    * 生成各区域商品点击次数临时表,包含了完成商品信息
    * temp_area_product_click_count 与 商品信息表尽心join
    *
    * @param sqlContext
    */
  def generateTempAreaFullProductClickCountTable(sqlContext: SQLContext): Unit ={
    /**
      * 商品信息表
      *   存储在hive表中
      *   product_info: product_id \ product_name \ extend_info:product_status
      *
      *   通过UDF 解析JSON格式数据(extend_info),得到product_status,UDF,返回的是0 或者 1
      *   0 自营商品
      *   1 第三方商品
      *   此时,我们要使用一个函数if()
      *     if(condition, true-value, false-value)
      */
    // 编写SQL语句
    val sqlStr =
      """
        SELECT
          tapcc.area,
          tapcc.product_id,
          tapcc.click_count,
          tapcc.city_infos,
          pi.product_name,
          if(get_json_object(pi.extend_info.'product_status') = 0,'自营商品','第三方商品') product_status

          product_status
        FROM
          temp_area_product_click_count tapcc
        JOIN
          product_info pi
        ON
          tapcc.product_id = pi.product_id
      """
    val df = sqlContext.sql(sqlStr)
    df.registerTempTable("temp_area_fullprod_click_count")
  }
  def getAreaTop10ProductRdd(sqlContext: SQLContext): RDD[Row] ={
    /**
      * 窗口分析函数 ROW_NUMBER
      *   SELECT 子查询
      *
      *   针对不同的area,进行划分区域等级,再次使用CASE...WHEN...THEN
      */
    // 编写SQL语句
    val sqlStr =
      """
         SELECT
              area,area_level,product,click_count,city_infos,product_name,product_status
         FORM(
            SELECT
              area,
              CASE
                WHEN area = '华北' OR '华东' THEN 'A级'
                WHEN area = '华南' OR '华中' THEN 'B级'
                WHEN area = '西南' OR '西北' THEN 'C级'
                WHEN area = '东北' THEN 'D级'
                ELSE 'E级'
              END area_level,
              product_id,
              click_count,
              city_infos,
              product_name,
              pro_status,
              ROW_NUMBER() OVER(PARTITION BY area ORDER BY click_count DESC) rank
            FROM
              temp_area_fullprod_click_count
              ) t
            WHERE
              t.rank <=10
      """

    // SparkSQL 执行
    val df =sqlContext.sql(sqlStr)
    df.rdd
  }

  /**
    * 持久化结果到MySQL数据库中
    *
    * @param taskId
    * @param rows
    */
  def persistAreaTop10Product(taskId: Long,rows: Array[Row],sc: SparkContext): Unit ={
    /**
      * 结果集表的结构
      * area_top10_product
      *   task_id
      * 0.area,
      * 1.area_level
      * 2.product,
      * 3.click_count,
      * 4.city_infos,
      * 5.product_name,
      * 6.product_status
      * 使用联合主键
      *
      * 此处有两种方式实现:
      *   方式一:
      *     直接使用传统的方式,JDBC(原生态,ORM框架:hibernate/mybatis)
      *
      *   方式二:
      *     SparkSQL 外部数据源
      *     dataframe.write.mode("append").jdbc(url,table,properties)
      */
//    val listBuffer = ListBuffer[List[Any]]()
//    for(row <- rows){
//      val newRow = taskId::
//                  row.getString(0)::
//                  row.getString(1)::
//                  row.getString(2)::
//                  row.getInt(3)::
//                  row.getString(4)::
//                  row.getString(5)::
//                  row.getString(6):: Nil
//      listBuffer.append(newRow)
//    }
//    sc.parallelize(listBuffer)

  }
}



/**
  * ===============================================================================================
  */

/**
  * 技术点一: 集成Hive,从Hive表中读取数据
  *
  * 技术点二: 外部数据源,从MySQL中读取数据
  *
  * 技术点三: DataFrame与RDD的相互转换与操作(集成SparkCore辅助分析)
  *
  * 技术点四: 使用SQL分析,自定义UDF和UDAF
  *
  *
  *
  * 如果你的开发是本地模式的话,就使用模拟的数据,否则使用集群数据
  */


