package com.atguigu.realtime.ads

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.realtime.Bean.OrderDetailWide
import com.atguigu.realtime.util.{MyKafkaUtil, OffsetManager, OffsetManagerMysql}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

object TrademarkStatApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("trademark_stat_app").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //根据偏移量获取数据流
    val topic = "DWS_DETAIL_WIDE"
    val groupId = "trademark_stat_group"
    //从mysql读取偏移量
    val offset: Map[TopicPartition, Long] = OffsetManagerMysql.getOffset(topic, groupId)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offset != null && offset.size > 0) {
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

    //转化数据结构
    val jsonObjDstream: DStream[OrderDetailWide] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderDetailWide: OrderDetailWide = JSON.parseObject(jsonString, classOf[OrderDetailWide])
      orderDetailWide
    }

    //jsonObjDstream.print(100)
    val amountWithTmDstream: DStream[(String, Double)] = jsonObjDstream.map(orderDetailWide => (orderDetailWide.tm_id + ":" + orderDetailWide.tm_name, orderDetailWide.final_detail_amount))
    val amountTotalByTmDstream: DStream[(String, Double)] = amountWithTmDstream.reduceByKey(_ + _)

    ///////////存储到mysql/////////////
    amountTotalByTmDstream.foreachRDD { rdd =>

      val amountArray: Array[(String, Double)] = rdd.collect()
      if (amountArray != null && amountArray.size > 0) {
        val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        DBs.setup()
        DB.localTx(implicit session => {
          //sql1
          for (amount <- amountArray) {
            //写数据库
            val tmArr: Array[String] = amount._1.split(":")
            val tmId = tmArr(0)
            val tmName = tmArr(1)
            val statTime: String = formatter.format(new Date())

            println("数据写入 执行")
            SQL("insert into trademark_amount_stat values (?,?,?,?) ").bind(statTime, tmId, tmName, amount._2).update().apply()
          }

          //sql2提交偏移量
          for (offsetRange <- offsetRanges) {
            val partitionId: Int = offsetRange.partition
            val untilOffset: Long = offsetRange.untilOffset
            println("执行提交偏移量")
            SQL("REPLACE INTO  offset_0105(group_id,topic,partition_id,topic_offset)  VALUES(?,?,?,?)").bind(groupId, topic, partitionId, untilOffset).update().apply()


          }


        })

      }


    }


    ssc.start()
    ssc.awaitTermination()
  }
}
