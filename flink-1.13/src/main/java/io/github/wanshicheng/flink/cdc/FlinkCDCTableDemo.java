package io.github.wanshicheng.flink.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class FlinkCDCTableDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);

        Properties properties = new Properties();
        properties.put("decimal.handling.mode", "string");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("test")
                .tableList("test.bin_log") // set captured table
                .username("root")
                .password("123456")
                .serverId("3000")
                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .print().setParallelism(1);
        env.execute("Print MySQL Snapshot + Binlog");
    }
}
