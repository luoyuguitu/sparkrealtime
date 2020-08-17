package com.atguigu.realtime.dwd

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.realtime.Bean.{Category3Info, OrderDetail, SkuInfo, SpuInfo, TmInfo}
import com.atguigu.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}


object OrderDetailApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ods_order_info_app").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //根据偏移量获取数据流
    val topic = "ODS_ORDER_DETAIL"
    val groupId = "order_detail_group"
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

    //转换流
    val orderDetailDstream: DStream[OrderDetail] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
      orderDetail
    }
    //  orderDetailDstream.print(100)

    //查sku表，得到关联其他表的字段

    val withAllFieldsDstream: DStream[OrderDetail] = orderDetailDstream.transform { rdd =>
      //查sku表
      val sql1 = "select  sku_id,spu_id,tm_id,category3_id from sku_info0105"
      val skuList: List[JSONObject] = PhoenixUtil.queryList(sql1)
      val skuMap: Map[String, SkuInfo] = skuList.map { jsonObj =>
        val skuInfo: SkuInfo = SkuInfo(
          jsonObj.getString("SKU_ID"),
          jsonObj.getString("SPU_ID"),
          jsonObj.getString("TM_ID"),
          jsonObj.getString("CATEGORY3_ID")
        )
        (skuInfo.id, skuInfo)
      }.toMap
      //广播变量
      val skuMapBC: Broadcast[Map[String, SkuInfo]] = ssc.sparkContext.broadcast(skuMap)

      //查spu表
      val sql2 = "select spu_id,spu_name from spu_info0105"
      val spuList: List[JSONObject] = PhoenixUtil.queryList(sql2)
      val spuMap: Map[String, SpuInfo] = spuList.map { jsonObj =>
        val spuInfo: SpuInfo = SpuInfo(
          jsonObj.getString("SPU_ID"),
          jsonObj.getString("SPU_NAME")
        )
        (spuInfo.id, spuInfo)
      }.toMap
      //广播变量
      val spuMapBC: Broadcast[Map[String, SpuInfo]] = ssc.sparkContext.broadcast(spuMap)

      //查tm表
      val sql3 = "select tm_id,tm_name from base_trademark0105"
      val tmList: List[JSONObject] = PhoenixUtil.queryList(sql3)
      val tmMap: Map[String, TmInfo] = tmList.map { jsonObj =>
        val tmInfo: TmInfo = TmInfo(
          jsonObj.getString("TM_ID"),
          jsonObj.getString("TM_NAME")
        )
        (tmInfo.tm_id, tmInfo)
      }.toMap
      //广播变量
      val tmMapBC: Broadcast[Map[String, TmInfo]] = ssc.sparkContext.broadcast(tmMap)

      //查category3表
      val sql4 = "select category3_id,category3_name from base_category30105 "
      val ctList: List[JSONObject] = PhoenixUtil.queryList(sql4)
      val ctMap: Map[String, Category3Info] = ctList.map { jsonObj =>
        val ctInfo: Category3Info = Category3Info(
          jsonObj.getString("CATEGORY3_ID"),
          jsonObj.getString("CATEGORY3_NAME")
        )
        (ctInfo.id, ctInfo)
      }.toMap
      //广播变量
      val ctMapBC: Broadcast[Map[String, Category3Info]] = ssc.sparkContext.broadcast(ctMap)


      val withAllFieldsRDD: RDD[OrderDetail] = rdd.map { orderDetail =>
        val skuMap: Map[String, SkuInfo] = skuMapBC.value
        val spuMap: Map[String, SpuInfo] = spuMapBC.value
        val tmMap: Map[String, TmInfo] = tmMapBC.value
        val ctMap: Map[String, Category3Info] = ctMapBC.value
        val skuInfo: SkuInfo = skuMap.getOrElse(orderDetail.sku_id.toString, null)

        val spuinfo: SpuInfo = spuMap.getOrElse(skuInfo.spu_id, null)
        val tmInfo: TmInfo = tmMap.getOrElse(skuInfo.tm_id, null)
        val category3Info: Category3Info = ctMap.getOrElse(skuInfo.category3_id, null)

        if (spuinfo != null) {
          orderDetail.spu_id = spuinfo.id.toLong
          orderDetail.spu_name = spuinfo.spu_name
        }
        if (tmMap != null) {
          orderDetail.tm_id = tmInfo.tm_id.toLong
          orderDetail.tm_name = tmInfo.tm_name
        }
        if (category3Info != null) {
          orderDetail.category3_id = category3Info.id.toLong
          orderDetail.category3_name = category3Info.name
        }
        orderDetail
      }
      withAllFieldsRDD
    }
    // withAllFieldsDstream.print(100)
    withAllFieldsDstream.foreachRDD { rdd =>
      rdd.foreachPartition { orderItr =>
        val orderList: List[OrderDetail] = orderItr.toList
        for (order <- orderList) {
          val orderDetail: String = JSON.toJSONString(order, new SerializeConfig(true))
          MyKafkaSink.send("DWD_ORDER_DETAIL", order.id.toString, orderDetail)
        }
      }
      OffsetManager.saveOffset(topic,groupId,offsetRanges)

    }


    ssc.start()
    ssc.awaitTermination()
  }

}
