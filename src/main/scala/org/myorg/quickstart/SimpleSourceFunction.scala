package org.myorg.quickstart

import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import java.util

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

/**
  * Created by GR on 2019/5/7.
  * time datetime uid wm title url
  */
class SimpleSourceFunction extends SourceFunction[(Long, String, String, String, String, String)] {

  val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  var isRunning: Boolean = true

  var map = new util.HashMap[String, util.LinkedList[(Long, String, String, String, String, String)]]()
//  var map = new mutable.HashMap[String, List[(Long, String, String, String, String, String)]]()
  var count = 0

  var windowSize: Long = 5L
  var speed: Long = 10

  var wms: Array[String] = Array(
    "北京"
    , "上海"
    , "深圳"
    , "杭州"
    , "成都"
    , "内蒙古"
    , "天津"
    , "河北"
    , "重庆"
    , "厦门"
  )

  var urls: Array[String] = Array(
    "www.baidu.com"
    , "www.sina.com"
  )

  var titles: Array[String] = Array(
    "新闻"
    , "娱乐"
    , "视频"
    , "音乐"
    , "小视频"
  )


  override def cancel(): Unit = {
    isRunning = false
  }

  override def run(sourceContext: SourceFunction.SourceContext[(Long, String, String, String, String, String)]): Unit = {


    while (isRunning) {
      var date = new Date
      val lessTen = Random.nextInt(10)

      val wm = wms(lessTen)

      val strDate = format.format(date)
      val time = date.getTime

//      val uid = UUID.randomUUID().toString.replaceAll("-", "")
      val uid = "user" + lessTen
      val url = urls(Random.nextInt(2))
      val title = titles(Random.nextInt(5))


      val log = (time, strDate, uid, wm, title, url)
      import scala.collection.JavaConversions._
      sourceContext.collect(log)
      count += 1
      if(map.containsKey(uid)) {
        val maybeTuples = map.get(uid)
        maybeTuples.add(log)
      } else {
//        val tuples = List[(Long, String, String, String, String, String)]()
        val list = new util.LinkedList[(Long, String, String, String, String, String)]()
        list.add(log)
        map.put(uid, list)
      }

      if(count >= 10) {
        count = 0
        val keys = map.keySet()

        for((x,y) <- map) {
          for(l <- y) {
            println(l)
          }
        }
        map.clear()
      }



      val rate = Random.nextFloat() * 10 * 100
      Thread.sleep(rate.toInt)
    }
  }
}
