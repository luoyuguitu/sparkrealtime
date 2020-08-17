package com.atguigu.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

object OffsetManager {
  def getOffset(topicName: String, groupId: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = RedisUtil.getJedis()
    val offsetKey = "offset:" + topicName + ":" + groupId
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    jedis.close()
    import scala.collection.JavaConversions._
    val kafkaOffsetMap: Map[TopicPartition, Long] = offsetMap.map { case (partitionId, offset) =>
      println("加载分区偏移量" + partitionId + ":" + offset)
      (new TopicPartition(topicName, partitionId.toInt), offset.toLong)


    }.toMap
    kafkaOffsetMap
  }

  def saveOffset(topicName: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    val offsetKey = "offset:" + topicName + ":" + groupId
    val offsetMap: java.util.Map[String, String] = new java.util.HashMap()

    for (offsetRange <- offsetRanges) {
      val partition: Int = offsetRange.partition
      val untilOffset: Long = offsetRange.untilOffset
      offsetMap.put(partition.toString, untilOffset.toString)
      println("写入分区" + partition + ":" + offsetRange.fromOffset + "---------->" + untilOffset)
    }

    //写入redis
    if (offsetMap != null && offsetMap.size() > 0) {
      val jedis: Jedis = RedisUtil.getJedis()
      jedis.hmset(offsetKey, offsetMap)
      jedis.close()
    }

  }

}
