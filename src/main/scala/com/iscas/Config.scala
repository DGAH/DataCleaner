package com.iscas

import java.io.InputStream
import java.util
import java.util.Properties
/**
  * Created by Lucg on 2017/7/5.
  * 程序配置信息
  * 主要任务：管理和向其他部分提供程序相关的设定
  */
object Config {
  /*
   * 配置文件信息
   */
  var UserName: String = "dataclean"
  var ConfigFilePath: String = "../../config.properties" //"hdfs://bigdata-1:8082/user/dataclean/configs/config.json"
  /*
   * Debug设置
   */
  var DebugMode: Boolean = true
  var Debug_PrintConfigs: Boolean = true
  var Debug_ShowFullRecord: Boolean = true
  /*
   * SparkStreaming设置
   */
  var AppName: String = "DataCleaner" // SparkConf 的应用程序名
  var WorkInterval: Int = 1 // StreamingContext 的工作时间间隔，也就是程序抓取数据的工作间隔。单位：秒
  var Topic: String = "DataCollect" // 当前话题
  var UseKerberos: Boolean = false
  /*
   * Kafka设置
   */
  var BrokerList: String = ""
  /*
   * HBase设置
   */
  var SubmitInterval: Int = 1000 // 两次HBase提交之间的操作动作数
  var CheckInterval: Long = 5 //两次HBase提交之间的最大时间间隔。单位：Streaming Batch time
  /*
   * 存储结构
   */
  var m_Data: Properties = new Properties()
  var m_Kafka_Params: Map[String, String] = Map[String, String]()
  var m_HBase_Params: Map[String, String] = Map[String, String]()
  /*
   * 工作函数
   */
  def init(args: Array[String]): Unit = {
    if (args.length > 0) {
      if (Config.DebugMode) {
        println(s"${args.length}")
        println(s"${args(0)}")
        if (args.length > 1) {
          println(s"${args(1)}")
        }
      }
      ConfigFilePath = args(0)
    }
  }

  def loadConfigs(): Boolean = {
    try {
      val fin: InputStream = this.getClass.getResourceAsStream(ConfigFilePath)
      m_Data.load(fin)
    } catch {
      case ex: Exception => {
        if (DebugMode) {
          println("WARNING: Load Config Failed! Error Message As Follows...")
          println(ex.toString)
          println("INFORMATION: Load Config File Stopped!")
        }
        return false
      }
    }
    try {
      // Debug
      DebugMode = m_Data.getProperty("DebugMode", DebugMode.toString).toBoolean
      Debug_ShowFullRecord = m_Data.getProperty("Debug|ShowFullRecord", Debug_ShowFullRecord.toString).toBoolean
      Debug_PrintConfigs = m_Data.getProperty("Debug|PrintConfigs", Debug_PrintConfigs.toString).toBoolean
      // Spark Stream
      AppName = m_Data.getProperty("AppName", AppName)
      Topic = m_Data.getProperty("Topic", Topic)
      WorkInterval = m_Data.getProperty("WorkInterval", WorkInterval.toString).toInt
      UseKerberos = m_Data.getProperty("UseKerberos", UseKerberos.toString).toBoolean
      // Kafka & HBase
      val keys: util.Enumeration[_] = m_Data.propertyNames()
      while (keys.hasMoreElements) {
        val key: String = keys.nextElement().toString
        if (key.startsWith("Kafka|")) {
          val subkey = key.substring("Kafka|".length)
          val value: String = m_Data.getProperty(key, "")
          if (!value.isEmpty) {
            m_Kafka_Params += (subkey -> value)
          }
        } else if (key.startsWith("HBase|")) {
          val subkey = key.substring("HBase|".length)
          val value: String = m_Data.getProperty(key, "")
          if (!value.isEmpty) {
            m_HBase_Params += (subkey -> value)
          }
        }
      }
      // Kafka
      BrokerList = m_Kafka_Params.getOrElse("metadata.broker.list", BrokerList)
      if (UseKerberos) {
        m_Kafka_Params += ("security.protocol" -> "PLAINTEXTSASL")
      }
      // HBase
      SubmitInterval = m_Data.getProperty("SubmitInterval", SubmitInterval.toString).toInt
      CheckInterval = m_Data.getProperty("CheckInterval", CheckInterval.toString).toLong
    } catch {
      case ex: Exception => {
        if (DebugMode) {
          println("WARNING: Load Config Data Failed! Error Message As Follows...")
          println(ex.toString)
          println("INFORMATION: Load Config File Stopped!")
        }
      }
    }
    if (DebugMode) {
      println("INFOMATION: Load Config File Finished!")
    }
    true
  }

  def printConfigs(): Unit = {
    println("================================Configs:Begin================================")
    println(s"[ConfigFilePath] $ConfigFilePath")
    println(s"[DebugMode] $DebugMode")
    if (DebugMode) {
      println("--------------------------------Debug--------------------------------")
      println(s"[PrintConfigs] $Debug_PrintConfigs")
      println(s"[ShowFullRecord] $Debug_ShowFullRecord")
    }
    println("--------------------------------SparkStreaming--------------------------------")
    println(s"[AppName] $AppName")
    println(s"[WorkInterval] $WorkInterval second(s)")
    println(s"[Topic] $Topic")
    println("--------------------------------Kafka--------------------------------")
    m_Kafka_Params.foreach(
      kv_pair => {
        println(s"[${kv_pair._1}] ${kv_pair._2}")
      }
    )
    println("--------------------------------HBase--------------------------------")
    m_HBase_Params.foreach(
      kv_pair => {
        println(s"[${kv_pair._1}] ${kv_pair._2}")
      }
    )
    println(s"[SubmitInterval] $SubmitInterval operations")
    println("================================Configs:End================================")
  }
}
