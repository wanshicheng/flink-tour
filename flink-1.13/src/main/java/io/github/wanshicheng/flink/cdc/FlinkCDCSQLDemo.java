package io.github.wanshicheng.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDCSQLDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        String source = "CREATE TABLE bin_log (\n" +
                "     id INT,\n" +
                "     price DECIMAL(4, 0),\n" +
                "     PRIMARY KEY(id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'localhost',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = '123456',\n" +
                "     'database-name' = 'test',\n" +
                "     'table-name' = 'bin_log'\n" +
                ")";
        tenv.executeSql(source);

        tenv.executeSql("SELECT * FROM bin_log").print();
    }
}
