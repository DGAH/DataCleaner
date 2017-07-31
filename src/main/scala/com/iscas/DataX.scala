package com.iscas

import com.iscas.Consumer.{NewTask, coreLogic}
import org.apache.spark.streaming.StreamingContext

/**
  * Created by Lucg on 2017/7/5.
  * 主程序
  */
object DataX {
  /*
   * 入口函数
   */
  def main(args: Array[String]): Unit = {
    Config.init(args)
    if (!Config.loadConfigs()) {
      println("Load Configs Failed! And The Program Will Use Default Config Value.")
    }
    if (Config.DebugMode && Config.Debug_PrintConfigs) {
      Config.printConfigs()
    }
    if (!Manager.init()) {
      println("Config Data Error! And The Program Has Been Stopped.")
      return
    }
    //Consumer.work()
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
}
