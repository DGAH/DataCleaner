import net.sf.json.{JSONException, JSONObject}

/**
  * Created by Lucg on 2017/7/5.
  * 数据记录类
  * 是数据结构化的具体体现
  */
class Record {
  var ID: String = ""
  var Type: String = ""
  var Usage: String = ""
  var Data: JSONObject = null
  var Ready: Boolean = false
  var ProviderID: String = ""
  /*
   * 通过JSONObject进行初始化
   */
  def fromJSON(src: JSONObject): Unit = {
    Ready = false
    ID = Global.getValue(src, "id", ID)
    Type = Global.getValue(src, "type", Type)
    Usage = Global.getValue(src, "usage", Usage)
    try {
      Data = JSONObject.fromObject(Global.getValue(src, "data", ""))
      ProviderID = Data.getJSONObject("info").getString("logisticproviderid")
    } catch {
      case ex: JSONException => {}
    }
    Ready = true
  }
  /*
   * 将数据记录转化为字符串（以显示）
   */
  override def toString: String = {
    if (Ready) {
      val strData: String = if (Data.isNullObject) {
        "-Null-"
      } else if (Data.isEmpty) {
        "-Empty-"
      } else {
        Data.toString
      }
      return s"[id]$ID[type]$Type[usage]$Usage[data]$strData"
    }
    "[id]--[type]--[usage]--[data]--"
  }
  /*
   * 测试字符串是否有效
   */
  def test(only_info: Boolean = true): Boolean = {
    if (!Ready) {
      println("[id]--[type]--[usage]--[data]--")
      return false
    }
    var ok: Boolean = false
    var strData: String = ""
    if (Data.isNullObject) {
      strData = "-Null-"
    } else if (Data.isEmpty) {
      strData = "-Empty-"
    } else {
      ok = true
      strData = if (only_info) {
        "-Normal-"
      } else {
        Data.toString
      }
    }
    println(s"[id]$ID[type]$Type[usage]$Usage[data]$strData")
    ok
  }
  /*
   * 判断是否与另一条数据记录重复
   */
  def isSameWith(another: Record): Boolean = {
    if (Ready && another.Ready && (!ID.isEmpty) ) {
      if ( (ID == another.ID) && (Usage == another.Usage) && (Type == another.Type) ) {
        return true
      }
    }
    false
  }
}
