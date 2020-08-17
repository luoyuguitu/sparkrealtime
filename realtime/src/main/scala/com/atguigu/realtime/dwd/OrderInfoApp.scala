package com.atguigu.realtime.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.realtime.Bean.{OrderInfo, ProvinceInfo, UserInfo, UserState}
import com.atguigu.realtime.util.{EsUtil, MyKafkaSink, MyKafkaUtil, OffsetManager, PhoenixUtil, UserUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderInfoApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ods_order_info_app").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //根据偏移量获取数据流
    val topic = "ODS_ORDER_INFO"
    val groupId = "order_info_group"
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

    //基本转换，填充时间字段
    val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      val createTime: String = orderInfo.create_time
      val createTimeArr: Array[String] = createTime.split(" ")
      orderInfo.create_date = createTimeArr(0)
      orderInfo.create_hour = createTimeArr(1).split(":")(0)
      orderInfo
    }

    //查询hbase中用户状态
    val orderInfoWithFirstFlagDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderInfoItr =>
      //分区操作
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if (orderInfoList.size > 0) {
        val userIdList: List[Long] = orderInfoList.map(_.user_id)
        val sql = "select user_id , if_consumed from user_state0105 where user_id in ('" + userIdList.mkString("','") + "')"
        val userStateList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userStateMap: Map[String, String] = userStateList.map { jsonObj =>
          (jsonObj.getString("USER_ID"), jsonObj.getString("IF_CONSUMED"))
        }.toMap
        for (orderInfo <- orderInfoList) {
          val if_consumed: String = userStateMap.getOrElse(orderInfo.user_id.toString, null)
          if (if_consumed != null && if_consumed == "1") {
            orderInfo.if_first_order = "0"
          } else {
            orderInfo.if_first_order = "1"
          }
        }
      }
      orderInfoList.toIterator
    }


    //如果同一批次出现同一个人下单数大于1，应该分别判断是否首单，按照上边的逻辑会把同一批次中的所有订单都置为首单的可能性
    // 处理办法： 1 同一批次 同一用户  2 最早的订单  3 标记首单
    //           1 分组： 按用户      2  排序  取最早  3 如果最早的订单被标记为首单，除最早的单据一律改为非首单
    //           1  groupbykey       2  sortWith    3  if ...

    //调整结构为k-v，为分组排序做准备
    val orderInfoWithKeyDstream: DStream[(Long, OrderInfo)] = orderInfoWithFirstFlagDstream.map(orderInfo => (orderInfo.user_id, orderInfo))

    //按用户分组
    val orderInfoGroupByUidDstream: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithKeyDstream.groupByKey()

    //组内排序，最早的订单被标记为首单，其余的改为非首单
    val orderInfoWithRealFirstDstream: DStream[OrderInfo] = orderInfoGroupByUidDstream.flatMap { case (uId, orderInfoItr) =>
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if (orderInfoList.size > 1) {
        //排序
        val orderInfoSortedList: List[OrderInfo] = orderInfoList.sortWith((orderInfo1, orderInfo2) => orderInfo1.create_time < orderInfo2.create_time)
        val isFirstFlag: String = orderInfoSortedList(0).if_first_order
        if (isFirstFlag == "1") {
          for (i <- 1 to orderInfoSortedList.size - 1) {
            orderInfoSortedList(i).if_first_order = "0"
          }
        }
        orderInfoSortedList
      } else {
        orderInfoItr.toList
      }
    }

    //关联维表，补充省份字段，使用广播变量
    val orderInfoWithProvinceDstream: DStream[OrderInfo] = orderInfoWithRealFirstDstream.transform { rdd =>
      //获取广播变量放到周期型执行的代码中
      val sql = "select  id,name,area_code,iso_code from gmall0105_province_info "
      val provinceInfoList: List[JSONObject] = PhoenixUtil.queryList(sql)
      //封装广播变量
      val provinceMap: Map[String, ProvinceInfo] = provinceInfoList.map { jsonObj =>
        val provinceInfo: ProvinceInfo = ProvinceInfo(
          jsonObj.getString("ID"),
          jsonObj.getString("NAME"),
          jsonObj.getString("AREA_CODE"),
          jsonObj.getString("ISO_CODE")
        )
        (provinceInfo.id, provinceInfo)
      }.toMap

      val provinceBC: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceMap)
      val orderInfoWithProvinceRDD: RDD[OrderInfo] = rdd.map { orderInfo =>
        val provinceMap: Map[String, ProvinceInfo] = provinceBC.value
        val provinceInfo: ProvinceInfo = provinceMap.getOrElse(orderInfo.province_id.toString, null)
        if (provinceInfo != null) {
          orderInfo.province_name = provinceInfo.name
          orderInfo.province_area_code = provinceInfo.area_code
          orderInfo.province_iso_code = provinceInfo.iso_code
        }
        orderInfo
      }
      orderInfoWithProvinceRDD
    }
    //orderInfoWithProvinceDstream.print(1000)

    //填充年龄及性别字段
    val orderInfoWithUserDstream: DStream[OrderInfo] = orderInfoWithProvinceDstream.transform { rdd =>
      val sql = "select  id,birthday,gender from gmall0105_user_info"
      val userList: List[JSONObject] = PhoenixUtil.queryList(sql)
      val userMap: Map[String, UserInfo] = userList.map(jsonObj => {
        val userInfo = UserInfo(jsonObj.getString("ID"),
          jsonObj.getString("BIRTHDAY"),
          jsonObj.getString("GENDER")
        )
        (userInfo.id, userInfo)
      }).toMap
      //用户数据量太大，而且密度低，不适合用广播变量，用上边查询用户状态的方式修改
      val userBC: Broadcast[Map[String, UserInfo]] = ssc.sparkContext.broadcast(userMap)
      //查hbase
      val withUserRDD: RDD[OrderInfo] = rdd.map { orderInfo =>
        val userMap: Map[String, UserInfo] = userBC.value
        val userJsonObj: UserInfo = userMap.getOrElse(orderInfo.user_id.toString, null)
        if (userJsonObj != null) {
          orderInfo.user_age_group = UserUtil.getAgeGroup(userJsonObj.birthday)
          orderInfo.user_gender = UserUtil.getGender(userJsonObj.gender)
        }
        orderInfo
      }
      withUserRDD
    }
    // orderInfoWithUserDstream.print(1000)

    //把首单的订单，更新到用户状态表中
    orderInfoWithUserDstream.cache()

    orderInfoWithUserDstream.foreachRDD { rdd =>
      val newConsumedUserRDD: RDD[UserState] = rdd.filter(_.if_first_order == "1").map(orderInfo => UserState(orderInfo.user_id.toString, "1"))
      newConsumedUserRDD.saveToPhoenix("USER_STATE0105", Seq("USER_ID", "IF_CONSUMED"),
        new Configuration, Some("hadoop108,hadoop109,hadoop110:2181"))

    }

    //保存到es中并推回kafka
    orderInfoWithUserDstream.foreachRDD { rdd =>
      rdd.foreachPartition { orderInfoItr =>
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        val orderInfoWithIdList: List[(String, OrderInfo)] = orderInfoList.map(orderInfo => (orderInfo.id.toString, orderInfo))
        val dateString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        EsUtil.bulkDoc(orderInfoWithIdList, "gmall0105_order_info_" + dateString)
        for (orderInfo <- orderInfoList) {
          val orderInfoJsonString: String = JSON.toJSONString(orderInfo, new SerializeConfig(true))
          MyKafkaSink.send("DWD_ORDER_INFO", orderInfo.id.toString, orderInfoJsonString)
        }

      }
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }


    ssc.start()
    ssc.awaitTermination()
  }

}
