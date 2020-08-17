package com.atguigu.realtime.dws

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.realtime.Bean.{OrderDetail, OrderDetailWide, OrderInfo}
import com.atguigu.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


object OrderDetailWideApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("dws_order_wide_app").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //根据偏移量获取数据流
    val topicOrderInfo = "DWD_ORDER_INFO"
    val groupIdOrderInfo = "dws_order_info_group"
    val topicOrderDetail = "DWD_ORDER_DETAIL"
    val groupIdOrderDetail = "dws_order_detail_group"

    //获取orderInfo流数据
    val offsetOrderInfo: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderInfo, groupIdOrderInfo)
    var orderInfoRecordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetOrderInfo.size > 0 && offsetOrderInfo != null) {
      orderInfoRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc, offsetOrderInfo, groupIdOrderInfo)
    } else {
      orderInfoRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc, groupIdOrderInfo)
    }

    //获取orderDetail流数据
    val offsetOrderDetail: Map[TopicPartition, Long] = OffsetManager.getOffset(topicOrderDetail, groupIdOrderDetail)
    var orderDetailRecordInputStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsetOrderDetail.size > 0 && offsetOrderDetail != null) {
      orderDetailRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderDetail, ssc, offsetOrderDetail, groupIdOrderDetail)
    } else {
      orderDetailRecordInputStream = MyKafkaUtil.getKafkaStream(topicOrderDetail, ssc, groupIdOrderDetail)
    }

    //得到本批次offset结束位置用于更新redis中得偏移量(OrderInfo)
    var orderInfoOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderInfoInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoRecordInputStream.transform { rdd =>
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    //得到本批次offset结束位置用于更新redis中得偏移量(OrderDetail)
    var orderDetailOffsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val orderDetailInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDetailRecordInputStream.transform { rdd =>
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    //调整流结构，转为样例类，方便获取字段
    val orderInfoDstream: DStream[OrderInfo] = orderInfoInputGetOffsetDstream.map { record =>
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      orderInfo
    }.filter(_.operate_time != null)

    val orderDetailDstream: DStream[OrderDetail] = orderDetailInputGetOffsetDstream.map { record =>
      val oederDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      oederDetail
    }
    //   orderDetailDstream.print(100)
    //   orderInfoDstream.print(100)

    //转换成k-v结构，方便以后join
    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailWithKeyDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail => (orderDetail.order_id, orderDetail))

    //无法保证数据在同一个批次中，join可能丢数据，需要开窗口再join，最后去重
    //1.开窗
    val orderInfoWithKeyWindowDstream: DStream[(Long, OrderInfo)] = orderInfoWithKeyDstream.window(Seconds(10), Seconds(5))
    val orderDetailWithKeyWindowDstream: DStream[(Long, OrderDetail)] = orderDetailWithKeyDstream.window(Seconds(10), Seconds(5))

    //2.join
    val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyWindowDstream.join(orderDetailWithKeyWindowDstream)

    //orderJoinedDstream.print(100)
    //3.去重
    val orderJoinedNewDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderJoinedDstream.mapPartitions { orderJoinedItr =>
      val jedis: Jedis = RedisUtil.getJedis()
      val key = "order_join_keys"
      val orderJoinedNewList = new ListBuffer[(Long, (OrderInfo, OrderDetail))]()
      for ((orderId, (orderInfo, orderDetail)) <- orderJoinedItr) {
        val ifNew: lang.Long = jedis.sadd(key, orderDetail.id.toString)

        if (ifNew == 1L) {
          orderJoinedNewList.append((orderId, (orderInfo, orderDetail)))
        }
      }
      jedis.close()
      orderJoinedNewList.toIterator
    }
    // orderJoinedNewDstream.print(100)
    //将jion流转化成宽表
    val orderDetailWideDStream: DStream[OrderDetailWide] = orderJoinedNewDstream.map { case (orderId, (orderInfo, orderDetail)) => new OrderDetailWide(orderInfo, orderDetail) }
    //orderDetailWideDStream.print(100)
    //实时需求，计算分摊金额，并填充字段
    // 思路 ：：
    //    每条明细已有        1  原始总金额（original_total_amount） （明细单价和各个数的汇总值）
    //    2  实付总金额 (final_total_amount)  原始金额-优惠金额+运费
    //    3  购买数量 （sku_num)
    //    4  单价      ( order_price)
    //
    //    求 每条明细的实付分摊金额（按明细消费金额比例拆分）
    //
    //    1  33.33   40    120
    //    2  33.33   40    120
    //    3   ？     40    120
    //
    //    如果 计算是该明细不是最后一笔
    //      使用乘除法      实付分摊金额/实付总金额= （数量*单价）/原始总金额
    //      调整移项可得  实付分摊金额=（数量*单价）*实付总金额 / 原始总金额
    //
    //    如果  计算时该明细是最后一笔
    //      使用减法          实付分摊金额= 实付总金额 - （其他明细已经计算好的【实付分摊金额】的合计）
    //    1 减法公式
    //      2 如何判断是最后一笔
    //      如果 该条明细 （数量*单价）== 原始总金额 -（其他明细 【数量*单价】的合计）
    //
    //
    //    两个合计值 如何处理
    //      在依次计算的过程中把  订单的已经计算完的明细的【实付分摊金额】的合计
    //    订单的已经计算完的明细的【数量*单价】的合计
    //    保存在redis中 key设计
    //    type ?   hash      key? order_split_amount:[order_id]  field split_amount_sum ,origin_amount_sum    value  ?  累积金额

    //  伪代码
    //    1  先从redis取 两个合计    【实付分摊金额】的合计，【数量*单价】的合计
    //    2 先判断是否是最后一笔  ： （数量*单价）== 原始总金额 -（其他明细 【数量*单价】的合计）
    //    3.1  如果不是最后一笔：
    // 用乘除计算 ： 实付分摊金额=（数量*单价）*实付总金额 / 原始总金额

    //    3.2 如果是最后一笔
    // 使用减法 ：   实付分摊金额= 实付总金额 - （其他明细已经计算好的【实付分摊金额】的合计）
    //    4  进行合计保存
    //  hincr
    //              【实付分摊金额】的合计，【数量*单价】的合计
    val orderWideWithSplitDstream: DStream[OrderDetailWide] = orderDetailWideDStream.mapPartitions { orderDetailWideItr =>

      val orderDtailWideList: List[OrderDetailWide] = orderDetailWideItr.toList
      for (orderDetailWide <- orderDtailWideList) {
        //取出原始总金额
        val original_total_amount: Double = orderDetailWide.original_total_amount
        //取出明细中商品数量
        val sku_num: Long = orderDetailWide.sku_num
        //取出明细中商品单价
        val sku_price: Double = orderDetailWide.sku_price
        //取出明细中实付总额
        val final_total_amount: Double = orderDetailWide.final_total_amount

        val jedis: Jedis = RedisUtil.getJedis()
        var toNowSplitSum: String = jedis.hget("order_split_amount" + orderDetailWide.order_id, "split_amount_sum")
        var toNowOriginSum: String = jedis.hget("order_split_amount" + orderDetailWide.order_id, "origin_amount_sum")

        //判空
        if (toNowSplitSum == null) {
          toNowSplitSum = "0"
        }
        if (toNowOriginSum == null) {
          toNowOriginSum = "0"
        }


        if (sku_num * sku_price == (original_total_amount - toNowOriginSum.toDouble)) {
          //是最后一笔
          val newSplitAmount: Double = final_total_amount - toNowSplitSum.toDouble
          orderDetailWide.final_detail_amount = Math.round(newSplitAmount * 100D) / 100D
          //并保存到redis中
          jedis.hincrByFloat("order_split_amount" + orderDetailWide.order_id, "split_amount_sum", orderDetailWide.final_detail_amount)
        } else {
          //不是首单
          val newSplitAmount: Double = sku_num * sku_price * final_total_amount / original_total_amount
          //保留两位小数
          orderDetailWide.final_detail_amount = Math.round(newSplitAmount * 100D) / 100D
          //保存到redis中
          jedis.hincrByFloat("order_split_amount" + orderDetailWide.order_id, "split_amount_sum", orderDetailWide.final_detail_amount)
        }
        jedis.close()

        //推回kafka
        //MyKafkaSink.send("DWS_DETAIL_WIDE", orderDetailWide.order_id.toString, JSON.toJSONString(orderDetailWide,new SerializeConfig(true)))
      }
      orderDtailWideList.toIterator
    }
    orderWideWithSplitDstream.cache()
    //orderWideWithSplitDstream.print(1000)

    //写入到clickhouse中
    val sparkSession: SparkSession = SparkSession.builder().appName("order_detail_wide_spark_app").getOrCreate()

    import sparkSession.implicits._

    //推回kafka
    val orderWideKafkaSentDstream: DStream[OrderDetailWide] = orderWideWithSplitDstream.mapPartitions { orderDetailWideItr =>
      val orderDetailWideList: List[OrderDetailWide] = orderDetailWideItr.toList
      for (orderDetailWide <- orderDetailWideList) {
        MyKafkaSink.send("DWS_DETAIL_WIDE", orderDetailWide.order_id.toString, JSON.toJSONString(orderDetailWide, new SerializeConfig(true)))
      }

      orderDetailWideList.toIterator

    }

    //保存到clickhouse
    orderWideKafkaSentDstream.foreachRDD{rdd =>

      val df: DataFrame = rdd.toDF()



      df.write.mode(SaveMode.Append)
        .option("batchsize", "100")
        .option("isolationLevel", "NONE") // 设置事务
        .option("numPartitions", "1") // 设置并发
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver").jdbc("jdbc:clickhouse://hadoop108:8123/test0105","order_wide",new Properties())


      //保存偏移量
      OffsetManager.saveOffset(topicOrderInfo, groupIdOrderInfo, orderInfoOffsetRanges)
      OffsetManager.saveOffset(topicOrderDetail, groupIdOrderDetail, orderDetailOffsetRanges)
    }



    ssc.start()
    ssc.awaitTermination()

  }

}
