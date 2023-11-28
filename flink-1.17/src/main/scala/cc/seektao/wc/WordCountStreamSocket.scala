//package cc.seektao.wc
//
//import org.apache.flink.api.scala.createTypeInformation
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//
//object WordCountStreamSocket {
//
//  def main(args: Array[String]): Unit = {
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val lineDS: DataStream[String] = env.socketTextStream("hadoop102", 7777)
//
//    val sum: DataStream[(String, Int)] = lineDS
//      .flatMap((_: String).split(" ").map((word: String) => (word, 1)))
//      .keyBy(0)
//      .sum(1)
//
//    sum.print()
//
//    env.execute()
//  }
//
//}
