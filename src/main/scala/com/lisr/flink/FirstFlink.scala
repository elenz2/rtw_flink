package com.lisr.flink

import org.apache.flink.streaming.api.scala._

object FirstFlink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDataStream: DataStream[String] = env.socketTextStream("192.168.88.101",1322)

    val resultDataStream: DataStream[(String, Int)] = inputDataStream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    resultDataStream.print()

    env.execute("StreamWordCount")
  }
}
