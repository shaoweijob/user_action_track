package org.tonypacer.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce._


/**
  * Spark如何将数据写入HBase
  *
  *
  * Created by apple on 16/8/30.
  */
object WriteData2HBase {
  def main (args: Array[String] ) {

    val sparkConf = new SparkConf().setAppName("WriteData2HBase")
                .setMaster("local[2]")   //  --master local[2]   |  yarn-client
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc) //  HiveContext
    //RDD -> Dataframe
    import sqlContext.implicits._

    /**
      * write data of RDD to HBase
      *   read:
      *     newAPIHadoopRDD : RDD[(rowkey,result)] -> (ImmutableBytesWritable,Result)
      *
      *   write:
      *     type of The RDD should be Tuple Type when write data to hbase.
      *     RDD[(rowkey,result)]
      *
      */
    val rdd = sc.parallelize(("hello",10)::("hadoop",20)::("spark",15)::Nil)
    val conf = HBaseConfiguration.create()    //此处注意,需要将集群中的配置文件放入classpath
    conf.set("mapreduce.job.outputformat.class",
              "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
    conf.set("mapreduce.output.fileoutputformat.outputdir",
              "/user/apple/hbasewrite-output")
    conf.set(TableOutputFormat.OUTPUT_TABLE,"htb_wordcount")

    /**
      * HBase & MapReduce ,TextData -> HFILE -> bulkload
      */

    /**
      * example:  word count
      *   HBase Table:
      *     htb_wordcount
      *       info:count
      *     rowkey:
      *       word
      */
    rdd.map(tuple => {
      val word = tuple._1
      val count = tuple._2.toLong
      val put = new Put(Bytes.toBytes(word))

      // add Column Family , column data
      put.add(Bytes.toBytes("info"),
        Bytes.toBytes("count"),
        Bytes.toBytes(count)
      )

      (new ImmutableBytesWritable(Bytes.toBytes(word)),put)
    })
          .saveAsNewAPIHadoopDataset(conf)





        sc.stop()
}

}
