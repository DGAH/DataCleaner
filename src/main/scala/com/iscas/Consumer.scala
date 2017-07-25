package com.iscas

import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.rdd.RDD

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
    streamingContext.addStreamingListener(new StateCheckListener())

    val strtopic = createTopicString()
    val topics = Set(strtopic)
    val inputDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      streamingContext, 
      Config.m_Kafka_Params, 
      topics
    )

    val dStream = inputDStream.flatMap(
      line => {
        val jsval = JSONObject.fromObject(line._2)
        Some(jsval)
      }
    ).map(jsval => filterData(jsval))
      .foreach(rdd => afterWrapper(rdd))

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
  def filterData(jsval: JSONObject): (Record, (String, Int, String)) = {
    if (Config.DebugMode) {
      println("================================ FilterData: Start ================================")
    }
    val record: Record = new Record()
    record.fromJSON(jsval)
    if (Config.DebugMode) {
      record.test(!Config.Debug_ShowFullRecord)
    }
    val result: (String, Int, String) = Filter.work(record)
    if (Config.DebugMode) {
      println("================================ FilterData: Finish ================================")
    }
    (record, result)
  }

  def afterWrapper(rdd: RDD[(Record, (String, Int, String))]): Unit = {
    val manager: Manager = new Manager()
    if (!manager.init()) {
      if (Config.DebugMode) {
        println(s"Manager init failed! RDD=${rdd.id}")
      }
      throw RuntimeException
    }
    rdd.foreach(record => afterFilterData(manager, record._1, record._2._1, record._2._2, record._2._3))
    if (manager.isDirty) {
      manager.flushToHBase()
    }
  }
  /*
   * 处理清洗结果
   */
  def afterFilterData(manager: Manager, record: Record, ruleID: String, opinion: Int, message: String): Unit = {
    if (opinion == Global.OPINION_NOTHING) {
      manager.commit(record)
    } else if (opinion == Global.OPINION_NEED_DISCARD) {
      manager.discard(record, ruleID, message)
    } else if (opinion == Global.OPINION_NEED_RETRY) {
      manager.askForRetry(record, ruleID, message)
    } else if (opinion == Global.OPINION_UNCERTAIN){
      manager.uncertain(record, ruleID, message)
    } else {
      manager.custom(record, ruleID, opinion, message)
    }
  }
}
