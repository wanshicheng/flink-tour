package io.github.wanshicheng.flink.connector.doris;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Doris有隐藏列__DORIS_DELETE_SIGN__
 */
public class DorisSqlTour {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(500);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        String source = "CREATE TABLE t_order (" +
                "id BIGINT," +
                "product_name STRING," +
                "create_time TIMESTAMP," +
                "PRIMARY KEY(id) NOT ENFORCED\n" +
                ") WITH(\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = '192.168.3.15',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = '123@abc',\n" +
                "     'database-name' = 'test',\n" +
                "     'table-name' = 'order'\n" +
                ")";

        String sink = "CREATE TABLE ods_order (" +
                "id BIGINT," +
                "product_name STRING," +
                "create_time TIMESTAMP," +
                "__DORIS_DELETE_SIGN__ INT" +
                ") WITH (" +
                "   'connector' = 'doris'," +
                "   'fenodes' = '192.168.3.11:8030'," +
                "   'table.identifier' = 'ods.ods_order'," +
                "   'username' = 'root'," +
                "   'password' = ''," +
                "   'sink.batch.size' = '3'," +
                "   'sink.batch.interval' = '1'," +
                "   'sink.enable-delete' = 'false'," +
                "   'sink.properties.format' = 'json'," +
                "   'sink.properties.strip_outer_array' = 'true'," +
                "   'sink.properties.columns' = 'id, product_name, create_time, __DORIS_DELETE_SIGN__'" +
                ")";
        String insert = "INSERT INTO ods_order SELECT id, product_name, create_time, 0 AS __DORIS_DELETE_SIGN__ FROM t_order";
        tenv.executeSql(source);
        tenv.executeSql(sink);
        tenv.executeSql(insert);
        env.execute();
    }
}
