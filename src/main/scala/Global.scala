import java.text.SimpleDateFormat
import java.util.Date

import net.sf.json.JSONObject

/**
  * Created by Lucg on 2017/7/5.
  */
object Global {
  /******************************************************************
    * 常量区
    *****************************************************************/
  /*
   * 错误处理状态
   */
  val ERROR_STATE_START: Int = 0 // 错误正在处理
  val ERROR_STATE_FINISH: Int = 1 // 错误已经处理完成
  val ERROR_STATE_WAIT: Int = 2 // 错误尚未处理
  /*
   * 数据处理意见
   */
  val OPINION_NOTHING: Int = 0 // 一切正常，没有意见
  val OPINION_NEED_DISCARD: Int = 1 // 记录相关信息并丢弃
  val OPINION_NEED_RETRY: Int = 2 // 记录相关信息并要求重发
  val OPINION_UNCERTAIN: Int = 999 // 无法处理
  /*
   * 面单数据结构
   */
  val EC_dataStruct: Map[String, List[String]] = Map(
    "info" -> List(
      "logisticproviderid",
      "mailno",
      "mailtype",
      "weight",
      "sencitycode",
      "reccitycode",
      "senareacode",
      "recareacode",
      "inserttime"
    ),
    "send" -> List(
      "senname",
      "senmobile",
      "senphone",
      "senprov",
      "sencity",
      "sencounty",
      "senaddress"
    ),
    "recv" -> List(
      "recname",
      "recmobile",
      "recphone",
      "recprov",
      "reccity",
      "reccounty",
      "recaddress"
    ),
    "pkg" -> List(
      "typeofcontents",
      "nameofcoutents",
      "mailcode",
      "recdatetime",
      "insurancevalue"
    )
  )
  /*
   * 状态单数据结构
   */
  val ECS_dataStruct: Map[String, List[String]] = Map(
    "info" -> List(
      "logisticproviderid",
      "mailno"
    ),
    "state" -> List(
      "time",
      "desc",
      "city",
      "facilitytype",
      "facilityno",
      "facilityname",
      "action"
    ),
    "contact" -> List(
      "contacter",
      "contactphone"
    )
  )
  /*
   * 允许为空的域
   */
  val Domain_canBeEmpty: Map[String, Boolean] = Map[String, Boolean](
    "senareacode" -> true,
    "recareacode" -> true
  )
  /******************************************************************
    * 工具函数区
    *****************************************************************/
  /*
   * 获取当天的日期字符串
   */
  def today(format: String = "yyyyMMdd"): String = {
    val curdate = new Date()
    val sdf = new SimpleDateFormat(format)
    val strdate = sdf.format(curdate)
    strdate
  }
  /*
   * 取得JSONObject中指定键的值
   */
  def getValue(jsdata: JSONObject, key: String, defaultValue: String, cantBeEmpty: Boolean = true): String = {
    try {
      val result: String = jsdata.getString(key)
      if (cantBeEmpty && result.isEmpty) {
        return defaultValue
      }
      return result
    } catch {
      case ex: Exception => {
        if (Config.DebugMode) {
          println(ex.toString)
        }
      }
    }
    defaultValue
  }
  def getValue(jsdata: JSONObject, key: String, defaultValue: Int): Int = {
    try {
      val result: Int = jsdata.getInt(key)
      return result
    } catch {
      case ex: Exception => {
        if (Config.DebugMode) {
          println(ex.toString)
        }
      }
    }
    defaultValue
  }
  def getValue(jsdata: JSONObject, key: String, defaultValue: Boolean): Boolean = {
    try {
      val result: Boolean = jsdata.getBoolean(key)
      return result
    } catch {
      case ex: Exception => {
        if (Config.DebugMode) {
          println(ex.toString)
        }
      }
    }
    defaultValue
  }
  def getObject(jsdata: JSONObject, key: String): JSONObject = {
    try {
      val result: JSONObject = jsdata.getJSONObject(key)
      return result
    } catch {
      case ex: Exception => {
        if (Config.DebugMode) {
          println(ex.toString)
        }
      }
    }
    new JSONObject()
  }
}
