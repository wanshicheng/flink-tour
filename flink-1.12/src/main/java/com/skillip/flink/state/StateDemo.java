package com.skillip.flink.state;

import com.skillip.flink.bean.WaterSensor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class StateDemo {
    private static final OutputTag<String> SIDE_OUTPUT = new OutputTag<String>("side-output") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment()
                .setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<WaterSensor> mapStream = source
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        int len = StringUtils.split(value, ',').length;
                        if (len < 3) {
                            return false;
                        }
                        return true;
                    }
                })
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = StringUtils.split(value, ',');
                        if (split.length < 3) {
                            return null;
                        }
                        return new WaterSensor(split[0], Long.parseLong(split[1]) * 1000, Integer.parseInt(split[2]));
                    }
                });

        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs();
                    }
                });
        SingleOutputStreamOperator<WaterSensor> singleOutputStreamOperator = mapStream.assignTimestampsAndWatermarks(watermarkStrategy);

        KeyedStream<WaterSensor, String> keyedStream = singleOutputStreamOperator.keyBy(WaterSensor::getId);

        DataStream<String> sideOutput = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            private Map<String, Long> tsMap = new HashMap<>();
            private Map<String, Integer> vcMap = new HashMap<>();

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                String id = value.getId();
                long currentTime = ctx.timerService().currentProcessingTime();

                if (!tsMap.containsKey(id) && vcMap.getOrDefault(id, Integer.MIN_VALUE) < value.getVc()) {
                    ctx.timerService().registerProcessingTimeTimer(currentTime + 10000L);
                    tsMap.put(id, currentTime);

                } else if (vcMap.getOrDefault(value.getId(), Integer.MIN_VALUE) < value.getVc()) {
                    ctx.timerService().deleteProcessingTimeTimer(tsMap.get(id) + 10000L);
                    tsMap.remove(id);
                }
                vcMap.put(id, value.getVc());
            }
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                ctx.output(SIDE_OUTPUT, "监控水位在10秒内连续不降");
            }
        }).getSideOutput(SIDE_OUTPUT);

        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<WaterSensor> result = window.sum("vc");


        sideOutput.print("side");
        result.print("main");

        env.execute();
    }
}
