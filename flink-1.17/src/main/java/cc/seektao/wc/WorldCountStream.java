package cc.seektao.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WorldCountStream {
    public static void main(String[] args) throws Exception {

        // TODO 1.创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 2.读取数据，从文件读取
        DataStreamSource<String> lineDF = env.readTextFile("data/a.txt");

        // TODO 3.处理数据
        // 3.1 将单词转换为指定的格式 (word, 1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lineDF.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });

        // 3.2 对相同的单词进行分组
        KeyedStream<Tuple2<String, Integer>, Object> wordAndOneGroupby = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        // 3.3 对单词进行聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordAndOneGroupby.sum(1);

        // TODO 4.输出数据
        sum.print();


        // TODO 5.执行环境
        env.execute();


    }
}
