package com.atguigu.realtime.ods


import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object BaseDbMaxwell {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("base_db_maxwell_app")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topic = "GMALL0105_DB_M"
    val groupId = "base_db_maxwell"
    //从redis读取偏移量
    val offset: Map[TopicPartition, Long] = OffsetManager.getOffset(topic,groupId)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offset.size>0&&offset!=null){
      recordInputStream  = MyKafkaUtil.getKafkaStream(topic,ssc,offset,groupId)
    } else {
      recordInputStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //得到本批次offset结束位置用于更新redis中得偏移量
    var offsetRanges:Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    //取出业务字段，分流推回kafka
    inputGetOffsetDstream.foreachRDD{rdd =>
      rdd.foreach{record =>
        val jsonStr: String = record.value()
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        val jsonstring: String = jsonObj.getString("data")
        val tableName: String = jsonObj.getString("table")
        val topic = "ODS_"+tableName.toUpperCase

        MyKafkaSink.send(topic,jsonstring)

      }
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }





    ssc.start()
    ssc.awaitTermination()


  }

}
