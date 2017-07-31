package com.iscas

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import net.sf.json.JSONObject
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream

/**
  * Created by Lucg on 2017/7/5.
  * 数据消费者
  * 主要任务：从上游接收数据，并对其进行分布式处理
  */
object Consumer {
  var Topics: Set[String] = Set()
  var ZookeeperTopicPath: String = ""
  var ZookeeperClient: ZkClient = null
  var NewTask: Boolean = true
  /*
   * 主函数
   */
  def work(): Unit = {
    // StreamingContext
    val streaming_context: StreamingContext = acquireStreamingContext()
    if (streaming_context == null) {
      return
    }
    streaming_context.addStreamingListener(new StateCheckListener())
    // InputDStream
    val input_dstream: InputDStream[(String, String)] = acquireInputStream(streaming_context)
    if (input_dstream == null) {
      return
    }
    // FilterData
    if (NewTask) {
      if (Config.DebugMode) {
        println("******** Create New Task ********")
      }
      input_dstream.flatMap(
        line => {
          val jsval: JSONObject = JSONObject.fromObject(line._2)
          Some(jsval)
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
    }
    else {
      if (Config.DebugMode) {
        println("******** Continue Last Task ********")
      }
      var offset_ranges = Array[OffsetRange]()
      input_dstream.transform(
        rdd => {
          offset_ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
        }
      ).flatMap(
        line => {
          val jsval: JSONObject = JSONObject.fromObject(line._2)
          Some(jsval)
        }
      ).map(
        jsval => filterData(jsval)
      ).foreachRDD(
        rdd => {
          for (offset <- offset_ranges) {
            val zk_path: String = s"$ZookeeperTopicPath/${offset.partition}"
            ZkUtils.updatePersistentPath(ZookeeperClient, zk_path, offset.fromOffset.toString)
          }
          rdd.foreachPartition(
            iter => commitPartition(iter)
          )
        }
      )
    }
    // Work
    streaming_context.start()
    streaming_context.awaitTermination()
  }
  /*
   * 准备环境
   */
  def acquireStreamingContext(): StreamingContext = {
    val spark_config: SparkConf = new SparkConf().setAppName(Config.AppName)
    val streaming_context: StreamingContext = new StreamingContext(spark_config, Seconds(Config.WorkInterval))
    Topics = Set(Config.Topic)
    streaming_context
  }
  /*
   * 建立连接
   * 代码参考：http://blog.csdn.net/kk303/article/details/52767260
   */
  def acquireInputStream(streaming_context: StreamingContext): InputDStream[(String, String)] = {
    var input_stream: InputDStream[(String, String)] = null
    val group = "tempGroup" //TODO: confirm this value.
    val topic = Config.Topic
    val topic_dir = new ZKGroupTopicDirs(group, topic)
    ZookeeperTopicPath = topic_dir.consumerOffsetDir
    ZookeeperClient = new ZkClient(Config.ZookeeperHost)
    val children = ZookeeperClient.countChildren(ZookeeperTopicPath)
    if (children > 0) {
      NewTask = false
      var from_offsets: Map[TopicAndPartition, Long] = Map()
      for (i <- 0 until children) {
        val partition_offset = ZookeeperClient.readData[String](s"$ZookeeperTopicPath/$i")
        val tp = TopicAndPartition(topic, i)
        val next_offset = partition_offset.toLong
        from_offsets += (tp -> next_offset)
      }
      val message_handler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      input_stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        streaming_context, Config.m_Kafka_Params, from_offsets, message_handler
      )
    } else {
      NewTask = true
      input_stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        streaming_context, Config.m_Kafka_Params, Topics
      )
    }
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
