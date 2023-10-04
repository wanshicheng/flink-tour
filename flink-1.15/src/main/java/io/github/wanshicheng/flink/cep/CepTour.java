package io.github.wanshicheng.flink.cep;

import io.github.wanshicheng.flink.entity.SensorRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CepTour {
    private static final String HOST = "localhost";
    private static final Integer PORT = 9999;

    private static void initSocketServer() {
        List<SensorRecord> records = new LinkedList<>();
        records.add(new SensorRecord("sensor01", 20D, 0L));
        records.add(new SensorRecord("sensor01", 19D, 1000L));
        records.add(new SensorRecord("sensor01", 31D, 1010L));
        records.add(new SensorRecord("sensor01", 31D, 1020L));
        records.add(new SensorRecord("sensor01", 32D, 1030L));
        try {
            ServerSocket server = new ServerSocket(PORT);
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
        new Thread(CepTour::initSocketServer).start();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream(HOST, PORT);
        KeyedStream<SensorRecord, String> sensorRecordStream = source.map(elem -> {
                    String[] props = elem.split("\t");
                    String id = props[0];
                    Double temperature = Double.valueOf(props[1]);
                    Long timestamp = Long.valueOf(props[2]);
                    return new SensorRecord(id, temperature, timestamp);
                }).assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorRecord>forBoundedOutOfOrderness(Duration.of(3, ChronoUnit.SECONDS))
                        .withTimestampAssigner(((element, recordTimestamp) -> element.getTimestamp()))
                )
                .keyBy(SensorRecord::getId);

        // 定义匹配模式
        Pattern<SensorRecord, SensorRecord> pattern = Pattern.<SensorRecord>begin("begin").where(new IterativeCondition<SensorRecord>() {
            Double lastTemperature = Double.MAX_VALUE;
            @Override
            public boolean filter(SensorRecord value, Context<SensorRecord> ctx) throws Exception {
                boolean result = value.getTemperature() >= lastTemperature;
                lastTemperature = value.getTemperature();
                return result;
            }
        }).times(3);

        // 在事件流上应用模式，得到一个pattern stream
        PatternStream<SensorRecord> patternStream = CEP.pattern(sensorRecordStream, pattern);

        // 从pattern stream上应用select function，检出匹配事件序列
        patternStream.select(new PatternSelectFunction<SensorRecord, String>() {
            @Override
            public String select(Map<String, List<SensorRecord>> pattern) throws Exception {
                List<SensorRecord> begin = pattern.get("begin");
                return begin.toString();
            }
        }).print();

        env.execute();
    }
}
