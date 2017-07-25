package com.iscas

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
    Config.loadConfigs()
    val managerForTest = new Manager()
    if (managerForTest.init()) {
      if (Config.DebugMode && Config.Debug_PrintConfigs) {
        Config.printConfigs()
      }
    } else {
      println("Load Configs Failed! And The Program Has Been Stopped.")
      return
    }
    Consumer.work()
  }
}
