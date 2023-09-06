package io.github.wanshicheng.flink.connector.doris;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.Properties;

/**
 * Doris有隐藏列__DORIS_DELETE_SIGN__
 */
public class DorisStreamApiTour {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        Properties properties = new Properties();
        properties.put("decimal.handling.mode", "string");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.3.15")
                .port(3306)
                .databaseList("test")
                .tableList("test.order") // set captured table
                .username("root")
                .password("123@abc")
                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();



        String[] fields = {"id", "product_name", "create_time"};
        LogicalType[] types = {new BigIntType(), new VarCharType(), new TimestampType()};
        Properties pro = new Properties();
        pro.setProperty("format", "json");
        pro.setProperty("strip_outer_array", "true");
        SinkFunction<String> sink = DorisSink.sink(fields, types, DorisReadOptions.builder().build(),
                DorisExecutionOptions.builder()
                        .setStreamLoadProp(pro)
//                        .setEnableDelete(true)
                        .setBatchSize(5)
                        .setBatchIntervalMs(0L)
                        .setMaxRetries(3)
                        .build(),
                DorisOptions.builder()
                        .setFenodes("192.168.3.11:8030")
                        .setTableIdentifier("ods.ods_order")
                        .setUsername("root")
                        .setPassword("")
                        .build());

        DataStreamSource<String> source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
//        source.print();
        source.map(x -> "{\"__DORIS_DELETE_SIGN__\": 0,\"id\":8,\"product_name\":\"product_bb\",\"create_time\":1693900124000}").addSink(sink);
        env.execute();
    }
}
