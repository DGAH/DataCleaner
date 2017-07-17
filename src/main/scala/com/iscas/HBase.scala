package com.iscas

import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * Created by Lucg on 2017/7/5.
  * HBase操作员
  * 主要任务：接受并完成Manager交给的有关操作HBase的工作
  */
object HBase {
  var m_HBase_Config: Configuration = null
  /*
   * 初始化
   */
  def init(): Boolean = {
    try {
      m_HBase_Config = HBaseConfiguration.create()
    } catch {
      case ex: Exception => {
        if (Config.DebugMode) {
          println(ex.toString)
        }
        return false
      }
    }
    Config.m_HBase_Params.foreach(
      kv_pair => {
        m_HBase_Config.set(kv_pair._1, kv_pair._2)
      }
    )
    true
  }
  /*
   * 提交一组数据到HBase表
   */
  def commit(data: Put, tableName: String): Boolean = {
    try {
      val hConfig = HBaseConfiguration.create()
      Config.m_HBase_Params.foreach(
        kv_pair => {
          hConfig.set(kv_pair._1, kv_pair._2)
        }
      )
      val table = new HTable(hConfig, TableName.valueOf(tableName))
      //val table = new HTable(m_HBase_Config, TableName.valueOf(tableName))
      table.put(data)
      table.flushCommits()
      table.close()
    } catch {
      case ex: Exception => {
        if (Config.DebugMode) {
          println("****************ERROR:BEGIN****************")
          println(ex.toString)
          ex.printStackTrace()
          println("****************ERROR:END****************")
        }
        return false
      }
    }
    true
  }
  /*
   * 提交多组数据到HBase表
   */
  def commit(puts: List[Put], tableName: String): Boolean = {
    try {
      val hConfig = HBaseConfiguration.create()
      Config.m_HBase_Params.foreach(
        kv_pair => {
          hConfig.set(kv_pair._1, kv_pair._2)
        }
      )
      val table = new HTable(hConfig, TableName.valueOf(tableName))
      //val table = new HTable(m_HBase_Config, TableName.valueOf(tableName))
      table.put(puts.asJava)
      table.flushCommits()
      table.close()
    } catch {
      case ex: Exception => {
        if (Config.DebugMode) {
          println("****************ERROR:BEGIN****************")
          println(ex.toString)
          ex.printStackTrace()
          println("****************ERROR:END****************")
        }
        return false
      }
    }
    true
  }
  /*
   * 提交一组查询并从HBase表中获得对应的一组数据
   */
  def acquire(ask: Get, tableName: String): Result = {
    var result: Result = null
    try {
      val hConfig = HBaseConfiguration.create()
      Config.m_HBase_Params.foreach(
        kv_pair => {
          hConfig.set(kv_pair._1, kv_pair._2)
        }
      )
      val table = new HTable(hConfig, TableName.valueOf(tableName))
      //val table = new HTable(m_HBase_Config, TableName.valueOf(tableName))
      result = table.get(ask)
      table.close()
    } catch {
      case ex: Exception => {
        if (Config.DebugMode) {
          println("****************ERROR:BEGIN****************")
          println(ex.toString)
          ex.printStackTrace()
          println("****************ERROR:END****************")
        }
      }
    }
    if (result == null) {
      result = new Result()
    }
    result
  }
  /*
   * 提交多组查询并从HBase表中获得对应的多组数据
   */
  def acquire(gets: List[Get], tableName: String): Array[Result] = {
    var result: Array[Result] = Array[Result]()
    try {
      val hConfig = HBaseConfiguration.create()
      Config.m_HBase_Params.foreach(
        kv_pair => {
          hConfig.set(kv_pair._1, kv_pair._2)
        }
      )
      val table = new HTable(hConfig, TableName.valueOf(tableName))
      //val table = new HTable(m_HBase_Config, TableName.valueOf(tableName))
      result = table.get(gets.asJava)
      table.close()
    } catch {
      case ex: Exception => {
        if (Config.DebugMode) {
          println("****************ERROR:BEGIN****************")
          println(ex.toString)
          ex.printStackTrace()
          println("****************ERROR:END****************")
        }
      }
    }
    result
  }

}
