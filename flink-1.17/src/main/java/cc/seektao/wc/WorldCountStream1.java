package cc.seektao.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WorldCountStream1 {
    public static void main(String[] args) throws Exception {


        // TODO 1.创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 2.读取数据，从文件读取
        DataStreamSource<String> lineDF = env.readTextFile("data/a.txt");

        // TODO 3.处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = lineDF.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        })
                .keyBy(data -> data.f0)
                .sum(1);


        // TODO 4.输出数据
        sum.print();


        // TODO 5.执行环境
        env.execute();

    }

}
