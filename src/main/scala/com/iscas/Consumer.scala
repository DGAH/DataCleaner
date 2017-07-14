import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Lucg on 2017/7/5.
  * 数据消费者
  * 主要任务：从上游接收数据，并对其进行分布式处理
  */
object Consumer {
  /*
   * 主函数
   */
  def work(): Unit = {
    val sparkConfig = new SparkConf().setAppName(Config.AppName)
    val streamingContext = new StreamingContext(sparkConfig, Seconds(Config.WorkInterval))
    val strtopic = createTopicString()
    val topics = Set(strtopic)
    val inputDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, Config.m_Kafka_Params, topics)
    val dStream = inputDStream.flatMap(
      line => {
        val jsval = JSONObject.fromObject(line._2)
        Some(jsval)
      }
    )
    dStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          records_iterator => {
            records_iterator.foreach(
              jsval => {
                filterData(jsval)
              }
            )
          }
        )
      }
    )
    streamingContext.start()
    streamingContext.awaitTermination()
  }
  /*
   * 生成Topic
   */
  def createTopicString(): String = {
    Config.Topic
  }
  /*
   * 数据清洗
   */
  def filterData(jsval: JSONObject): Unit = {
    if (Config.DebugMode) {
      println("================================ FilterData: Start ================================")
    }
    val record: Record = new Record()
    record.fromJSON(jsval)
    if (Config.DebugMode) {
      record.test(!Config.Debug_ShowFullRecord)
    }
    val result: (String, Int, String) = Filter.work(record)
    afterFilterData(record, result._1, result._2, result._3)
    if (Config.DebugMode) {
      println("================================ FilterData: Finish ================================")
    }
  }
  /*
   * 处理清洗结果
   */
  def afterFilterData(record: Record, ruleID: String, opinion: Int, message: String): Unit = {
    if (opinion == Global.OPINION_NOTHING) {
      Manager.commit(record)
    } else if (opinion == Global.OPINION_NEED_DISCARD) {
      Manager.discard(record, ruleID, message)
    } else if (opinion == Global.OPINION_NEED_RETRY) {
      Manager.askForRetry(record, ruleID, message)
    } else if (opinion == Global.OPINION_UNCERTAIN){
      Manager.uncertain(record, ruleID, message)
    } else {
      Manager.custom(record, ruleID, opinion, message)
    }
  }
}
