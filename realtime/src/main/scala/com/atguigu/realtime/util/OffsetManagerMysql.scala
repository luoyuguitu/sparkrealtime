package com.atguigu.realtime.util

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition

object OffsetManagerMysql {

  def getOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    val sql = "SELECT partition_id , topic_offset FROM offset_0105 WHERE topic='" + topic + "' AND group_id='" + groupId + "'"
    val partitionOffsetList: List[JSONObject] = MysqlUtil.queryList(sql)
    if(partitionOffsetList!=null){
    val topicPartitionMap: Map[TopicPartition, Long] = partitionOffsetList.map { jsonObj =>
      val topicPartition: TopicPartition = new TopicPartition(topic, jsonObj.getIntValue("partition_id"))
      val offset: Long = jsonObj.getLongValue("topic_offset")
      (topicPartition, offset)
    }.toMap
    // List[(x,y)] .toMap->Map

    topicPartitionMap
  } else {
      null
    }


  }

}
