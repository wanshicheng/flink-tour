package com.skillip.flink.source

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Socket {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("hadoop101", 10000)

//    stream.map(_ => (_, 1)).print.setParallelism(1)
    stream.print.setParallelism(1)
    env.execute()

  }
}
