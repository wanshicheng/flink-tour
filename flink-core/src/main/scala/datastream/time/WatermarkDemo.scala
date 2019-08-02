package datastream.time

import java.text.SimpleDateFormat

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WatermarkDemo {
  def main(args: Array[String]): Unit = {
    println("---------- start ----------")
    val params = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 并行度必须设置为1
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputDS = env.socketTextStream(host, port)


    val mapDS = inputDS.map(value => {
      val arr = value.split("\\W+")
      val code = arr(0)
      val time = arr(1).toLong
      (code,time)
    })



    val watermark = mapDS.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String,Long)] {
      var currentMaxTimestamp = 0L
      val maxOutOfOrderness = 10000L  //最大允许的乱序时间是10s

      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(t: (String,Long), l: Long): Long = {
        val timestamp = t._2
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        println("(" + t._1 +","+ t._2 + ")|" +format.format(t._2) +", Current Max TS: ("+  currentMaxTimestamp + "|"+ format.format(currentMaxTimestamp) + ")"+ getCurrentWatermark)
        timestamp
      }
    })

    val window: DataStream[(String, Int, String, String, String, String)] = watermark
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new WindowFunction[(String,Long),(String, Int, String, String, String, String),String,TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int, String, String, String, String)]): Unit = {
          val list = input.toList.sortBy(_._2)
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
          out.collect(key,input.size,"ele start:" + format.format(list.head._2),"ele end:" + format.format(list.last._2),
            "win start:" + format.format(window.getStart),"win end:" + format.format(window.getEnd))
        }
      })

    window.print()
    
    env.execute()
  }
}


/*

测试时间
000001,1461756862000
000001.1461756866000
000001.1461756872000
000001.1461756873000
000001.1461756874000

000001,1461756862000

000001.1461756884000
 */

