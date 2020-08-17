package com.atguigu.realtime.util

import java.util.Properties

object PropertiesUtil {
  def main(args: Array[String]): Unit = {
   val properties: Properties = PropertiesUtil.load("config.properties")

    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertiesName:String):Properties ={
    val prop = new Properties()
    prop.load(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName))
    prop
  }





}
