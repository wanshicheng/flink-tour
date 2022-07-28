package com.skillip.flink.table;

import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TableDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();



        env.execute();
    }
}
