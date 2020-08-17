package com.atguigu.realtime.util

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang3.time.DateUtils

object UserUtil {
  def main(args: Array[String]): Unit = {
    println(System.currentTimeMillis())
   println(getAgeGroup("1994-06-27"))
    println(getGender("M"))
  }
  def getAgeGroup(birth:String):String = {
    val today = new Date()
    //val formater = new SimpleDateFormat("yyyy-MM-dd")
     //val today: Date= formater.parse(System.currentTimeMillis().toString)
    //val birthday: Date = format.parse(birth)

    //val today: Date = DateUtils.parseDate(todayStr)
    val birthStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat("yyyy-MM-dd").parse(birth))
    val birthday: Date = DateUtils.parseDate(birthStr,"yyyy-MM-dd")


    if(DateUtils.addYears(birthday,20).after(today)){
        "20岁以下"
    }else if(DateUtils.addYears(birthday,50).after(today)){
      "20岁~50岁"
    } else {
      "50岁以上"
    }
  }

  def getGender(gender:String):String = {
    if("M".equals(gender)){
      "男"
    }else if("F".equals(gender)){
      "女"
    }else {
      null
    }
  }


}
