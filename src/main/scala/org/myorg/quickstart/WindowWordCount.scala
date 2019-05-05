package org.myorg.quickstart

import java.util.{Comparator, Properties}

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketScheme
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.util.Collector

/**
  * wordcount示例
  * 包含多种输入流，输出流
  */
object WindowWordCount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    // = 0. Kafka
//    val kafkaConsumer = buildKafkaConsumer()
//    val record = env.addSource(kafkaConsumer)
//    record.addSink(buildKafkaProducer())

//    val text = env.fromElements("hello jiang jiang", "hello world world", "ni hao", "hao", "hello")

    // 1. socket
    val text = env.socketTextStream("localhost", 9999)
//    text.writeToSocket("localhost", 9998, new SimpleStringSchema())

    // 2. file
//    val readme = env.readTextFile("/Users/GR/project/ScalaProjects/flink/readme.md")
//    readme.writeAsText("/Users/GR/project/ScalaProjects/flink/readme.md.bak").setParallelism(1)

    // 4. Create a DataStream from a list of elements
    val myInts = env.fromElements(1, 2, 3, 4, 5, 3)

    // 5. Create a DataStream from any Collection
//    val data: Seq[(String, Int)] = Seq[(String,Int)](("hello",1),("jiang",2))
//    val myTuples = env.fromCollection(data)

    // 6. Create a DataStream from an Iterator
//    val longIt: Iterator[Long] = Iterator(1000L, 2000L, 4000L)
//    val myLongs = env.fromCollection(longIt)


    /*val words = text.flatMap {_.split("\\W") filter {_.nonEmpty} }.map((_,1))
      .keyBy(0).timeWindow(Time.seconds(5)).sum(1)*/

    println("原始数据：")
    text.print("原始数据")
    val mapedStream = text.flatMap(x => {
      x.split("\\W")
    }).filter(_.nonEmpty)
      .map((_,1))

    mapedStream.print("maped stream:")

    val keyedStream = mapedStream.keyBy(0)

    keyedStream.print("keyedStream").name("keyedStream")

    var reduceStream = keyedStream.reduce(
      (x,y) => {(x._1, x._2 + y._2)}
    )

    reduceStream.print("reduceStream").name("reduceStream")

    //    val value = myInts.map((_,1)).keyBy(0).min(0)
    val minValue = keyedStream.min(1)
    val minByValue = keyedStream.minBy(1)
    minValue.print("min")
    minByValue.print("min by")
//    minValue.writeAsText("/Users/GR/project/ScalaProjects/flink/quickstart/readme.md.bak", FileSystem.WriteMode.OVERWRITE).setParallelism(1)
//    minByValue.writeAsText("/Users/GR/project/ScalaProjects/flink/quickstart/readme.md.bak", FileSystem.WriteMode.OVERWRITE).setParallelism(1)


    val value = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5L)))
    value.process(new KeyedProcessFunction[String, (String,Int), (String, Int)] {
    })


    val value1 = keyedStream.timeWindow(Time.seconds(5), Time.seconds(1))
    val value2 = keyedStream.countWindow(10, 1)
    keyedStream.timeWindowAll(Time.seconds(10))

    keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(4L)))

    env.execute("Word count example")
  }

  class MyComParator extends Comparator[String] {
    override def compare(o1: String, o2: String): Int = {
      o1.toInt - o2.toInt
    }
  }

  class TT extends KeyedProcessFunction[Tuple, String, String] {

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, String, String]#OnTimerContext, out: Collector[String]): Unit = super.onTimer(timestamp, ctx, out)

    val state: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("xx", classOf[String]))

    override def processElement(i: String, context: KeyedProcessFunction[Tuple, String, String]#Context, collector: Collector[String]): Unit = {

    }
  }

  class TestWindowFunction extends ProcessWindowFunction[String, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
    }
  }


  def buildKafkaProducer(): FlinkKafkaProducer010[String] = {
    val myProducer = new FlinkKafkaProducer010[String]("localhost:9092", "topic", new SimpleStringSchema())
    myProducer.setWriteTimestampToKafka(true)
    myProducer
  }

  def buildKafkaConsumer(): FlinkKafkaConsumer010[String] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val consumer = new FlinkKafkaConsumer010[String]("topic", new SimpleStringSchema(), properties)
//    consumer.setStartFromEarliest()      // start from the earliest record possible
//    consumer.setStartFromLatest()        // start from the latest record
//    consumer.setStartFromTimestamp(...)  // start from specified epoch timestamp (milliseconds)
//    consumer.setStartFromGroupOffsets()  // the default behaviour
    consumer
  }

}
