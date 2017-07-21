package com.iscas

import java.util.Date

import org.apache.hadoop.hbase.client.{Get, Put, Result}

import scala.util.control.Breaks

/**
  * Created by Lucg on 2017/7/5.
  * 数据经理
  * 主要任务：对经过Consumer处理的数据进行管理，视情况将其写入HBase，同时负责向上游反馈处理结果
  * 对应设计图中的Distributor部分
  */
object Manager {
  var EC_Records: EC_Table = new EC_Table()
  var ECS_Records: ECS_Table = new ECS_Table()
  var RecordCounts: STable = new STable()
  var ErrorRecords: ER_Table = new ER_Table()
  var nOperateTimes: Int = 0
  // var tLastOperation: Long = 0
  var waitForFlush: Int = 0
  var isDirty: Boolean = false
  /*
   * 初始化
   */
  def init(): Boolean = {
    if (!HBase.init()) {
      return false
    }
    // tLastOperation = new Date().getTime
    true
  }
  /*
   * 处理行为：准备将正常的记录保存至HBase
   * 对应 Global.OPINION_NOTHING
   */
  def commit(record: Record): Unit = {
    if (Config.DebugMode) {
      println(s"commit (record: ${record.ID})")
    }
    saveRecord(record)
    val strtoday = Global.today()
    addToStatistics(record.Type, record.ProviderID, strtoday)
    resultFeedBack(record, Global.OPINION_NOTHING, "", "Success!")
  }
  /*
   * 处理行为：直接丢弃异常的记录
   * 对应 Global.OPINION_NEED_DISCARD
   */
  def discard(record: Record, ruleID: String, message: String): Unit = {
    if (Config.DebugMode) {
      println(s"discard (record: ${record.ID}, ruleID: $ruleID, message: $message")
    }
    val strtoday = Global.today()
    addToStatistics(record.Type, record.ProviderID, strtoday, isError = true)
    val reason = s"$ruleID - $message"
    updateErrorRecord(record.ProviderID, strtoday, record.ID, reason, "Just ignore.", Global.ERROR_STATE_FINISH)
    resultFeedBack(record, Global.OPINION_NEED_DISCARD, ruleID, message)
  }
  /*
   * 处理行为：要求上游重传记录
   * 对应 Global.OPINION_NEED_RETRY
   */
  def askForRetry(record: Record, ruleID: String, message: String): Unit = {
    if (Config.DebugMode) {
      println(s"askForRetry (record: ${record.ID}, ruleID: $ruleID, message: $message)")
    }
    // TODO: 等待重传机制确定后，完成此功能
    val strtoday = Global.today()
    addToStatistics(record.Type, record.ProviderID, strtoday, isError = true)
    val reason = s"$ruleID - $message"
    updateErrorRecord(record.ProviderID, strtoday, record.ID, reason, "Ask for re-upload.", Global.ERROR_STATE_START)
    resultFeedBack(record, Global.OPINION_NEED_RETRY, ruleID, message)
  }
  /*
   * 处理行为：不确定如何处理异常的记录，暂且直接丢弃之
   * 对应 Global.OPINION_UNCERTAIN
   */
  def uncertain(record: Record, ruleID: String, message: String): Unit = {
    if (Config.DebugMode) {
      println(s"uncertain (record: ${record.ID}, ruleID: $ruleID, message: $message)")
    }
    val strtoday = Global.today()
    addToStatistics(record.Type, record.ProviderID, strtoday, isError = true)
    val reason = s"$ruleID - $message"
    updateErrorRecord(record.ProviderID, strtoday, record.ID, reason, "Just ignore.", Global.ERROR_STATE_FINISH)
    resultFeedBack(record, Global.OPINION_UNCERTAIN, ruleID, message)
  }
  /*
   * 用户自定义的针对异常记录的处理行为
   */
  def custom(record: Record, ruleID: String, opinion: Int, message: String): Unit = {
    if (Config.DebugMode) {
      println(s"custom (record: ${record.ID}, ruleID: $ruleID, opinion: $opinion, message: $message)")
    }
    val handle = Filter.getHandleFunc(opinion)
    if (handle == null) {
      uncertain(record, ruleID, message)
      return
    }
    handle(record, ruleID, message)
    resultFeedBack(record, opinion, ruleID, message)
  }
  /*
   * 将处理结果反馈给上游
   */
  def resultFeedBack(record: Record, opinion: Int, ruleID: String, message: String): Unit = {
    if (Config.DebugMode) {
      println(s"resultFeedBack (record: ${record.ID}, opinion: $opinion, ruleID: $ruleID, message: $message)")
    }
    // TODO: 等反馈机制确定后，完成此功能
  }
  /*
   * 【内部函数】操作：保存正常的记录，准备将其写入HBase
   */
  def saveRecord(record: Record): Unit = {
    if (Config.DebugMode) {
      println(s"saveRecord (record: ${record.ID})")
    }
    if (record.Type == "ExpressContract") {
      EC_Records.addRecord(record)
    } else if (record.Type == "ExpressContractState") {
      ECS_Records.addRecord(record)
    }
    afterOperate()
  }
  /*
   * 【内部函数】操作：添加统计信息
   */
  def addToStatistics(srcType: String, providerID: String, strDate: String, isError: Boolean = false): Unit = {
    if (Config.DebugMode) {
      println(s"addToStatistics (srcType: $srcType, providerID: $providerID, strDate: $strDate, isError: $isError)")
    }
    val rowKey: String = s"$strDate$providerID"
    if (srcType == "ExpressContract") {
      RecordCounts.addExpCount(rowKey, isError)
    } else if (srcType == "ExpressContractState") {
      RecordCounts.addExpStateCount(rowKey, isError)
    }
    afterOperate()
  }
  /*
   * 【内部函数】操作：添加或更新错误统计
   */
  def updateErrorRecord(providerID: String, recordDate: String, dataID: String, reason: String, handle: String, state: Int): Unit = {
    if (Config.DebugMode) {
      println(s"updateErrorRecord (providerID: $providerID, recordDate: $recordDate, dataID: $dataID, reason: $reason, handle: $handle, state: $state")
    }
    val rowKey: String = s"$recordDate$providerID$dataID"
    ErrorRecords.updateRecord(rowKey, dataID, reason, handle, state)
    afterOperate()
  }
  /*
   * 【内部函数】当操作次数达到设定值之后，真正更新HBase
   */
  def afterOperate(): Unit = {
    nOperateTimes += 1
    isDirty = true
    if (Config.DebugMode) {
      println(s"nOperateTimes=$nOperateTimes, isDirty=$isDirty")
    }
    if (isFull) { // || isTimeUp
      flushToHBase()
      nOperateTimes = 0
      // tLastOperation = new Date().getTime
    }
  }
  /*
   * 【内部函数】判断操作次数是否达到设定值
   */
  def isFull: Boolean = {
    nOperateTimes >= Config.SubmitInterval
  }
  /*
   * 【内部函数】判断操作间隔是否超过设定值
   */
  /* def isTimeUp: Boolean = { */
    // val curtime: Long = new Date().getTime
    // if (Config.DebugMode) {
      // println(s"current: $curtime")
    // }
    // curtime - tLastOperation >= Config.CheckInterval
 /*  } */
  /*
   * 【内部函数】将所有更改提交至HBase
   */
  def flushToHBase(): Unit = {
    if (Config.DebugMode) {
      println("INFORMATION: flushToHBase !")
    }
    submitECRecords()
    submitECSRecords()
    submitRecordCounts()
    submitErrorRecords()
    waitForFlush = 0
    isDirty = false
  }
  /*
   * 【内部函数】提交所有面单记录
   */
  def submitECRecords(): Unit = {
    var puts: List[Put] = List()
    EC_Records.to_save.foreach(
      kv_pair => {
        puts = puts :+ kv_pair._2.pack()
      }
    )
    HBase.commit(puts, "ExpressContract")
    EC_Records.reset()
  }
  /*
   * 【内部函数】提交所有状态单记录
   */
  def submitECSRecords(): Unit = {
    var puts: List[Put] = List()
    ECS_Records.to_save.foreach(
      kv_pair => {
        puts = puts :+ kv_pair._2.pack()
      }
    )
    HBase.commit(puts, "ExpressContractState")
    ECS_Records.reset()
  }
  /*
   * 【内部函数】提交所有统计信息
   */
  def submitRecordCounts(): Unit = {
    var gets: List[Get] = List()
    val clusterExp: Array[Byte] = "exp".getBytes("UTF-8")
    val clusterExpState: Array[Byte] = "expstate".getBytes("UTF-8")
    val columnTotal: Array[Byte] = "total".getBytes("UTF-8")
    val columnError: Array[Byte] = "error".getBytes("UTF-8")
    RecordCounts.to_save.foreach(
      kv_pair => {
        val rowKey: String = kv_pair._1
        val get: Get = new Get(rowKey.getBytes("UTF-8"))
        get.addColumn(clusterExp, columnTotal)
        get.addColumn(clusterExp, columnError)
        get.addColumn(clusterExpState, columnTotal)
        get.addColumn(clusterExpState, columnError)
        gets = gets :+ get
      }
    )
    val results: Array[Result] = HBase.acquire(gets, "ExpressStatistics")
    var puts: List[Put] = List()
    RecordCounts.to_save.foreach(
      kv_pair => {
        val rowKey: String = kv_pair._1
        val data: CountData = kv_pair._2
        var result: Result = null
        var flag: Boolean = false
        val loop: Breaks = new Breaks()
        loop.breakable {
          results.foreach(
            re => {
              if (!re.isEmpty) {
                val cur_row: String = new String(re.getRow, "UTF-8")
                if (cur_row == rowKey) {
                  flag = true
                  result = re
                  loop.break()
                }
              }
            }
          )
        }
        var originalExpTotal: Int = 0
        var originalExpError: Int = 0
        var originalExpStateTotal: Int = 0
        var originalExpStateError: Int = 0
        if (flag) {
          if (result.containsNonEmptyColumn(clusterExp, columnTotal)) {
            originalExpTotal = new String(result.getValue(clusterExp, columnTotal), "UTF-8").toInt
          }
          if (result.containsNonEmptyColumn(clusterExp, columnError)) {
            originalExpError = new String(result.getValue(clusterExp, columnError), "UTF-8").toInt
          }
          if (result.containsNonEmptyColumn(clusterExpState, columnTotal)) {
            originalExpStateTotal = new String(result.getValue(clusterExpState, columnTotal), "UTF-8").toInt
          }
          if (result.containsNonEmptyColumn(clusterExpState, columnError)) {
            originalExpStateError = new String(result.getValue(clusterExpState, columnError), "UTF-8").toInt
          }
        }
        val put: Put = new Put(rowKey.getBytes("UTF-8"))
        put.addColumn(clusterExp, columnTotal, (data.nExpTotal + originalExpTotal).toString.getBytes("UTF-8"))
        put.addColumn(clusterExp, columnError, (data.nExpError + originalExpError).toString.getBytes("UTF-8"))
        put.addColumn(clusterExpState, columnTotal, (data.nExpStateTotal + originalExpStateTotal).toString.getBytes("UTF-8"))
        put.addColumn(clusterExpState, columnError, (data.nExpStateError + originalExpStateError).toString.getBytes("UTF-8"))
        puts = puts :+ put
      }
    )
    HBase.commit(puts, "ExpressStatistics")
    RecordCounts.reset()
  }
  /*
   * 【内部函数】提交所有错误统计
   */
  def submitErrorRecords(): Unit = {
    var puts: List[Put] = List()
    val clusterInfo: Array[Byte] = "info".getBytes("UTF-8")
    val columnID: Array[Byte] = "dataid".getBytes("UTF-8")
    val columnReason: Array[Byte] = "reason".getBytes("UTF-8")
    val columnHandle: Array[Byte] = "handle".getBytes("UTF-8")
    val columnState: Array[Byte] = "state".getBytes("UTF-8")
    ErrorRecords.to_save.foreach(
      kv_pair => {
        val rowKey: String = kv_pair._1
        val data: ErrorItem = kv_pair._2
        val put: Put = new Put(rowKey.getBytes("UTF-8"))
        put.addColumn(clusterInfo, columnID, data.DataID.getBytes("UTF-8"))
        put.addColumn(clusterInfo, columnReason, data.Reason.getBytes("UTF-8"))
        put.addColumn(clusterInfo, columnHandle, data.Handle.getBytes("UTF-8"))
        put.addColumn(clusterInfo, columnState, data.State.toString.getBytes("UTF-8"))
        puts = puts :+ put
      }
    )
    HBase.commit(puts, "ExpressErrorStatistics")
    ErrorRecords.reset()
  }
}
