package org.tonypacer.product

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructField, StringType, StructType, DataType}

/**
  * Created by apple on 16/8/29.
  */
object GroupConcatDistinct extends UserDefinedAggregateFunction{
  /**
    * input schema
    * 指定输入数据   的字段和类型
    *
    * @return
    */
  override def inputSchema: StructType = StructType(
    StructField("cityInfo",StringType,true) ::Nil
  )

  /**
    * 指定 buffer schema
    * @return
    */
  override def bufferSchema: StructType = StructType(
    StructField("bufferCityInfo",StringType,true)::Nil
  )


  /**
    * 指定唯一性
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 最终返回的数据
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
  /**
    * 指定返回数据类型
    * @return
    */
  override def dataType: DataType = StringType

  /**
    * step 1: 初始化,缓存数据(临时缓冲数据)的值
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,"")
  }

  /**
    * step 2: 更新缓冲数据的值
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 获取 缓冲数据
    var bufferCityInfo = buffer.getString(0)
    bufferCityInfo.contains()
    // 获取 输入数据
    val cityInfo = input.getString(0)

    // 去重的操作,涵盖在此: if the buffer cotains input data, no execute any operator
    if(!bufferCityInfo.contains(cityInfo)){
      if("".equals(bufferCityInfo)){
        bufferCityInfo = cityInfo
      }else{
        bufferCityInfo += "," + cityInfo
      }
    }

    buffer.update(0,bufferCityInfo)
  }

  /**
    * step 3: 合并不同分区中的缓冲数据的值
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferCityInfo1 = buffer1.getString(0)
    var bufferCityInfo2 = buffer2.getString(0)

    for(cityInfo <- bufferCityInfo2.split(",")){
      if(!bufferCityInfo1.contains(cityInfo)){
        if("".equals(bufferCityInfo1)){
          bufferCityInfo1 = cityInfo
        }else{
          bufferCityInfo1 += "," + cityInfo
        }
      }
    }
    // 更新数据
    buffer1.update(0,bufferCityInfo1)
  }

}



