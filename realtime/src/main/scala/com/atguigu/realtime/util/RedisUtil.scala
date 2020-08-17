package com.atguigu.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
  var jedisPool:JedisPool = null
  def getJedis():Jedis = {
    if(jedisPool == null) {
      val config = PropertiesUtil.load("config.properties")
      val host = config.getProperty("redis.host")
      val port = config.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setBlockWhenExhausted(true)
      jedisPoolConfig.setMaxIdle(20)
      jedisPoolConfig.setMaxTotal(50)
      jedisPoolConfig.setMaxWaitMillis(5000)
      jedisPoolConfig.setMinIdle(20)
      jedisPoolConfig.setTestOnBorrow(true)

      jedisPool = new JedisPool(jedisPoolConfig,host,port.toInt)
    }
    jedisPool.getResource
  }



}
