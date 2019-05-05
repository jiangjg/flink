package org.myorg.quickstart

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.wikiedits.{WikipediaEditEvent, WikipediaEditsSource}


/**
  * 维基百科分析
  */
object WikipediaAnalysis {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val edits = env.addSource(new WikipediaEditsSource())

    val keyedEdits = edits.keyBy(_.getUser)

    val value = keyedEdits.timeWindow(Time.seconds(5)).fold(("", 0))((r, record) => {
      val count = r._2 + record.getByteDiff
      (record.getUser, count)
    } )

    value.print()

    env.execute("WikiPedia ...")

  }

}
