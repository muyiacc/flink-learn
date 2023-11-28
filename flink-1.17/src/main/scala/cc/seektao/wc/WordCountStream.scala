import org.apache.flink.streaming.api.scala._

object WordCountStream {
  def main(args: Array[String]): Unit = {

    // 创建流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 读取文件： 从文件中读取
    val lineDS: DataStream[String] = env.readTextFile("data/a.txt")

    // 处理数据
    val wordAndOne: DataStream[(String, Int)] = lineDS.flatMap((line: String) => {
      line.split(" ").map((word: String) => (word, 1))
    }).keyBy(0).sum(1)

    // 输出
    wordAndOne.print()

    // 执行环境
    env.execute()
  }
}
