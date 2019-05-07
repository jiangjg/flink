package org.myorg.quickstart

import java.util.{Comparator, Properties}

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
  * Created by GR on 2019/5/6.
  */
object NewUserCount {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val logStream = env.addSource(new SimpleSourceFunction)

    val keyedByUidStream = logStream.keyBy(2)

    val userCountStream = logStream.map(x => (x, 0))
    val keyedUserStream = userCountStream.keyBy(x => x._1._3)

//    val countUserPV = keyedUserStream.reduce(new ReduceFunction[((Long, String, String, String, String, String), Int)] {
//      override def reduce(t: ((Long, String, String, String, String, String), Int), t1: ((Long, String, String, String, String, String), Int)): ((Long, String, String, String, String, String), Int) = {
//        (t._1, t._2 + 1)
//      }
//    })
        val countUserPV = keyedUserStream.timeWindow(Time.seconds(10L)).reduce(new ReduceFunction[((Long, String, String, String, String, String), Int)] {
          override def reduce(t: ((Long, String, String, String, String, String), Int), t1: ((Long, String, String, String, String, String), Int)): ((Long, String, String, String, String, String), Int) = {
            (t._1, t._2 + 1)
          }
        })

    countUserPV.print

    env.execute("Word count example")
  }


}
