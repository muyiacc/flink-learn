package cc.seektao.wc;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 实现单词出现频率统计
 */
public class WordCountBatch {
    public static void main(String[] args) throws Exception {

        // TODO 1.准备环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // TODO 2.读取文件
        DataSource<String> lineDF = env.readTextFile("data/a.txt");

        // TODO 3.转换数据
        // 3.1 切分单词
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDF.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    // 3.2 将单词转行成二元组
                    Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                    collector.collect(tuple2);
                }
            }
        });

        // TODO 4.分组 聚合
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroupby = wordAndOne.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroupby.sum(1);

        // TODO 5.输出数据
        sum.print();

    }
}
