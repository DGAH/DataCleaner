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
    Consumer.work()
  }
}
