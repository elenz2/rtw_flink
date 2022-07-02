package com.lisr.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object FirstFlink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDataStream: DataStream[String] = env.socketTextStream("192.168.88.101",1322)

    val resultDataStream: DataStream[(String, Int)] = inputDataStream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    resultDataStream.print()

    //执行任务
    env.execute("StreamWordCount")
  }
}
