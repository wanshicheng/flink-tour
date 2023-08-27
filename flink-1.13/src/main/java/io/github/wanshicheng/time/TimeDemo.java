package io.github.wanshicheng.time;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class TimeDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<String> is = env.socketTextStream("localhost", 9999);

        // 转换成SensorReading类型，分配时间戳和watermark
        is.map(line -> {
            String[] arr = line.split(",");
            return new SensorReading(arr[0], new Long(arr[1]), new Double(arr[2]));
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.of(3, ChronoUnit.SECONDS))
                                .withTimestampAssigner(((element, recordTimestamp) -> element.getTimestamp()))
//                        .withTimestampAssigner(((element, recordTimestamp) -> ((SensorReading) element).getTimestamp()))
                      )
                .keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<SensorReading, Object, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<Object> out) throws Exception {
                        System.out.println("window: ["+ window.getStart() + ", "+ window.getEnd()+"]");
                        double sum = 0.0D;
                        int cnt = 0;
                        for (SensorReading sensorReading : input) {
                            sum += sensorReading.getTemperature();
                            cnt++;
                        }
                        out.collect(sum / cnt);
                    }
                }).print();
        env.execute();
    }


}
