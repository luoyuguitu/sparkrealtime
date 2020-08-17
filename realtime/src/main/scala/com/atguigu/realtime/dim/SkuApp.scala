package com.atguigu.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.realtime.Bean.{ProvinceInfo, SkuInfo}
import com.atguigu.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._

object SkuApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("kafka_hbase_sku_app").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //根据偏移量获取数据流
    val topic = "ODS_SKU_INFO"
    val groupId = "sku_group"
    //从redis读取偏移量
    val offset: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offset.size > 0 && offset != null) {
      recordInputStream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
    } else {
      recordInputStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //得到本批次offset结束位置用于更新redis中得偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    //写入到hbase中
    inputGetOffsetDstream.foreachRDD { rdd =>
      val skuInfoRDD: RDD[SkuInfo] = rdd.map { record =>
        val jsonString: String = record.value()
        val skuInfo: SkuInfo = JSON.parseObject(jsonString, classOf[SkuInfo])
        skuInfo
      }


      skuInfoRDD.filter(_!=null).saveToPhoenix("sku_info0105", Seq("SKU_ID", "SPU_ID","TM_ID","CATEGORY3_ID"),new Configuration,Some("hadoop108,hadoop109,hadoop110:2181"))
      //   provinceInfoRDD.foreach(println)

      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }



    ssc.start()
    ssc.awaitTermination()

  }

}
