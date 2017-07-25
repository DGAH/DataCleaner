package com.iscas

import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted, StreamingListenerBatchStarted}
/*
 * 监听器
 */
class StateCheckListener extends StreamingListener {

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    // TODO: This function is running at driver, record Kafka offset
    // NOTE: Not worked
    // if (Config.DebugMode) {
      // println("Batch Started: waitForFlush=" + Manager.waitForFlush + ", isDirty=" + Manager.isDirty)
    // }
    // if (Manager.waitForFlush == Config.CheckInterval) {
      // Manager.flushToHBase()
    // } else {
      // if (Manager.isDirty) {
        // Manager.waitForFlush += 1
      // }
    // }
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    //val curTime: Long = batchCompleted.batchInfo.processingEndTime.getOrElse(default = 0)
    super.onBatchCompleted(batchCompleted)
  }
}

