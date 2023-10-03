package io.github.wanshicheng.flink.time;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.tools.util.SocketServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;

/**
 * 水位和窗口结束时间不一定准，可能相差几毫秒
 */
public class DataLatenessTour {
    private static void initSocketServer(Integer port) {
        List<SensorRecord> records = new LinkedList<>();
        records.add(new SensorRecord("sensor01", 20D, 0L));
        records.add(new SensorRecord("sensor01", 20D, 4000L));
        records.add(new SensorRecord("sensor01", 22D, 6800L));
        /**
         * 水位以内的数据
         * sensor01 window: [0, 5000] temperature:21.0
         */
        records.add(new SensorRecord("sensor01", 23D, 4900L));
        records.add(new SensorRecord("sensor01", 22D, 8001L));
        records.add(new SensorRecord("sensor01", 22D, 8100L));
        // 迟到数据
        records.add(new SensorRecord("sensor01", 25D, 4950L));
        records.add(new SensorRecord("sensor01", 22D, 8200L));

        records.add(new SensorRecord("sensor01", 22D, 10001L));
        // 迟到数据
//        records.add(new SensorRecord("sensor01", 25D, 4950L));
        records.add(new SensorRecord("sensor01", 22D, 15001L));

        // 迟到数据
        records.add(new SensorRecord("sensor01", 25D, 4950L));
        records.add(new SensorRecord("sensor01", 22D, 15120L));
        try {
            ServerSocket server = new ServerSocket(port);
            Socket client = server.accept();
            OutputStream os = client.getOutputStream();
            PrintWriter printWriter = new PrintWriter(os);
            long lastTimestamp = 0;
            for (SensorRecord record : records) {
                long interval = record.getTimestamp() - lastTimestamp;
                if (interval > 0) {
                    Thread.sleep(interval);
                    lastTimestamp = record.getTimestamp();
                }
                String s = record.getId() + "\t" + record.getTemperature() + "\t" + record.getTimestamp();
                printWriter.println(s);
                printWriter.flush();
            }
            client.close();
            server.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        Integer port = 9999;
        // 启动Socket服务端
        new Thread(() -> initSocketServer(port)).start();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 水位线生成需要尊重物理规律
        env.getConfig().setAutoWatermarkInterval(100);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStreamSource<String> source = env.socketTextStream(host, port);
        // 对数据流中的元素分配时间戳
        source.map(elem -> {
                    String[] props = elem.split("\t");
                    String id = props[0];
                    Double temperature = Double.valueOf(props[1]);
                    Long timestamp = Long.valueOf(props[2]);
                    return new SensorRecord(id, temperature, timestamp);
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorRecord>forBoundedOutOfOrderness(Duration.of(3, ChronoUnit.SECONDS))
                        .withTimestampAssigner(((element, recordTimestamp) -> element.getTimestamp()))
                )
                .keyBy(SensorRecord::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .allowedLateness(Time.seconds(15))
                .apply(new WindowFunction<SensorRecord, Object, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<SensorRecord> input, Collector<Object> out) throws Exception {
                        double sum = 0.0D;
                        int cnt = 0;
                        for (SensorRecord record : input) {
                            sum += record.getTemperature();
                            cnt++;
                        }
                        System.out.println();
                        out.collect(key +" window: ["+ window.getStart() + ", "+ window.getEnd()+"] temperature:" + sum / cnt);
                    }
                })
                .print();

        env.execute();
    }



}
