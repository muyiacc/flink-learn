package cc.seektao.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamSocket {
    public static void main(String[] args) throws Exception {
        // 1. 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);

        // 3. 处理数据: 切分、转换、分组、聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS
                .flatMap(
                        (String line, Collector<Tuple2<String, Integer>> collector) -> {
                            String[] words = line.split(" ");
                            for (String word : words) {
                                // 采集器采集数据发送到下游
                                collector.collect(new Tuple2<>(word, 1));
                            }
                        })
                .returns(Types.TUPLE(Types.STRING, Types.INT))  // landbda 表达式有类型擦除，需要手动指定泛型的参数类型
                .keyBy(word -> word.f0)
                .sum(1);

        // 4. 输出
        sum.print();

        // 5. 启动
        env.execute();
    }

    public String test1() {
        return "test1";
    }
}
