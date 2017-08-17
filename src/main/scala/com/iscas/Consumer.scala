package com.iscas

import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream

/**
  * Created by Lucg on 2017/7/5.
  * 数据消费者
  * 主要任务：从上游接收数据，并对其进行分布式处理
  */
object Consumer {
  var NewTask: Boolean = false
  /*
   * 主函数
   */
  def work(): Unit = {
    if (Config.DebugMode) {
      println("ERROR: This 'Consumer.work' function may not be executed!")
      return
    }
    val streaming_context: StreamingContext = StreamingContext.getOrCreate(Config.CheckpointPath, coreLogic)
    if (streaming_context == null) {
      if (Config.DebugMode) {
        println("ERROR: Create Streaming Context Failed!")
      }
      return
    }
    if (Config.DebugMode) {
      if (NewTask) {
        println("******** Create New Task ********")
      } else {
        println("******** Continue Last Task ********")
      }
    }
    streaming_context.start()
    streaming_context.awaitTermination()
  }
  /*
   * Core Logic
   */
  def coreLogic(): StreamingContext = {
    // StreamingContext
    val streaming_context: StreamingContext = acquireStreamingContext()
    if (streaming_context == null) {
      return null
    }
    // InputDStream
    val input_dstream: InputDStream[ConsumerRecord[String, String]] = acquireInputStream(streaming_context)
    if (input_dstream == null) {
      return null
    }
    // Prepare
    streaming_context.addStreamingListener(new StateCheckListener())
    streaming_context.checkpoint(Config.CheckpointPath)
    input_dstream.checkpoint(Seconds(Config.WorkInterval * 5))
    NewTask = true
    // FilterData
    input_dstream.map(
      record => {
        val jsval: JSONObject = JSONObject.fromObject(record.value)
        jsval
      }
    ).map(
      jsval => filterData(jsval)
    ).foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => commitPartition(iter)
        )
      }
    )
    streaming_context
  }
  /*
   * 准备环境
   */
  def acquireStreamingContext(): StreamingContext = {
    val spark_config: SparkConf = new SparkConf().setAppName(Config.AppName)
    val streaming_context: StreamingContext = new StreamingContext(spark_config, Seconds(Config.WorkInterval))
    streaming_context
  }
  /*
   * 建立连接
   * 代码参考：http://www.jianshu.com/p/00b591c5f623
   */
  def acquireInputStream(streaming_context: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val topics: Set[String] = Set(Config.Topic)
    val input_stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streaming_context, 
      PreferConsistent,
      Subscribe[String, String](topics, Config.m_Kafka_Params)
    )
    input_stream
  }
  /*
   * 数据清洗
   * 针对每条数据
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
  /*
   * 数据流出
   * 针对各个rdd中的每个partition
   */
  def commitPartition(iter: Iterator[(Record, (String, Int, String))]): Unit = {
    while (iter.hasNext) {
      val result: (Record, (String, Int, String)) = iter.next()
      sendResult(result._1, result._2._1, result._2._2, result._2._3)
    }
    if (Manager.isDirty) {
      Manager.flushToHBase()
    }
  }
  /*
   * 处理清洗结果
   */
  def sendResult(record: Record, ruleID: String, opinion: Int, message: String): Unit = {
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
