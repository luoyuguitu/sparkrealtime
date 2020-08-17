package com.atguigu.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.realtime.Bean.DauInfo
import com.atguigu.realtime.util.{EsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DauApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("dauApp").setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(3))

    val groupId = "GMALL_DAU_CONSUMER"
    val topic = "GMALL_STARTUP_0105"


    var startInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    //从redis获取偏移量，根据获取情况判断获取kafkaDstream的方式
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
    if (kafkaOffsetMap != null && kafkaOffsetMap.size > 0) {
      startInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, kafkaOffsetMap, groupId)
    } else {
      startInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }


    //获取本批次的偏移量结束位置，用于更新redis中的偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = startInputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    //spark消费kafka，添加需要的字段
    val jsonObjDstream: DStream[JSONObject] = inputGetOffsetDstream.map { record =>
      val str: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(str)
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
      val hour: String = simpleDateFormat.format(jsonObj.getLong("ts"))
      val hours = hour.split(" ")
      val dt = hours(0)
      val hr = hours(1)
      jsonObj.put("dt", dt)
      jsonObj.put("hr", hr)
      jsonObj
    }

    //保存到redis去重
    val filtedDstream: DStream[JSONObject] = jsonObjDstream.mapPartitions { itr =>
      val list: List[JSONObject] = itr.toList
      val listBuffer: ListBuffer[JSONObject] = ListBuffer[JSONObject]()
      println("去重前：" + list.size + ":条")
      val jedis: Jedis = RedisUtil.getJedis()
      for (elem <- list) {
        val dtStr: String = elem.getString("dt")
        val keyStr: String = "dau:" + dtStr
        val midStr: String = elem.getJSONObject("common").getString("mid")
        jedis.expire(keyStr, 3600 * 24)
        val noExist: lang.Long = jedis.sadd(keyStr, midStr)
        if (noExist == 1L) {
          listBuffer.append(elem)
        }
      }
      jedis.close()
      println("去重后：" + listBuffer.size + ":条")
      listBuffer.toIterator
    }
    //将过滤后得数据写入es中
    filtedDstream.foreachRDD { rdd =>
      rdd.foreachPartition { jsonItr =>
        val list: List[JSONObject] = jsonItr.toList
        //把源数据 转换成为要保存的数据格式
        val dauList: List[DauInfo] = list.map { jsonObj =>
          val commonJSONObj: JSONObject = jsonObj.getJSONObject("common")
          DauInfo(commonJSONObj.getString("mid"),
            commonJSONObj.getString("uid"),
            commonJSONObj.getString("ar"),
            commonJSONObj.getString("ch"),
            commonJSONObj.getString("vc"),
            jsonObj.getString("dt"),
            jsonObj.getString("hr"),
            "00",
            jsonObj.getLong("ts")
          )
        }
        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        EsUtil.bulkDoc(dauList, "gmall0105_dau_info_" + dt)
      }

      //提交偏移量
      OffsetManager.saveOffset(topic, groupId, offsetRanges)
    }


    ssc.start()
    ssc.awaitTermination()
  }

}
