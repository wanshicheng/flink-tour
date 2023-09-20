package io.github.wanshicheng.flink.operator.side;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 旁路输出
 */
public class SideOutputTour {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Integer> source = env.fromElements(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        OutputTag<Integer> oddTag = new OutputTag<>("奇数", TypeInformation.of(Integer.class));
        OutputTag<Integer> evenTag = new OutputTag<>("偶数", TypeInformation.of(Integer.class));

        SingleOutputStreamOperator<Object> split = source.process(new ProcessFunction<Integer, Object>() {
            @Override
            public void processElement(Integer value, ProcessFunction<Integer, Object>.Context ctx, Collector<Object> out) throws Exception {
                if (value % 2 == 0) {
                    ctx.output(evenTag, value);
                } else {
                    ctx.output(oddTag, value);
                }
            }
        });

        DataStream<Integer> oddStream = split.getSideOutput(oddTag);
        DataStream<Integer> evenStream = split.getSideOutput(evenTag);

        oddStream.print("奇数");
        evenStream.print("偶数");

        env.execute();
    }
}
