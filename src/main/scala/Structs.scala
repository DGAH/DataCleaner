import net.sf.json.{JSONException, JSONObject}
import org.apache.hadoop.hbase.client.Put

import scala.collection.mutable
/**
  * Created by Lucg on 2017/7/5.
  */
class CountData {
  var nExpTotal: Int = 0
  var nExpError: Int = 0
  var nExpStateTotal: Int = 0
  var nExpStateError: Int = 0
  def addExpData(isError: Boolean = false): Unit = {
    nExpTotal += 1
    if (isError) {
      nExpError += 1
    }
  }
  def addExpStateData(isError: Boolean = false): Unit = {
    nExpStateTotal += 1
    if (isError) {
      nExpStateError += 1
    }
  }
}

class STable {
  var to_save: mutable.Map[String, CountData] = mutable.Map[String, CountData]()

  def addExpCount(rowKey: String, isError: Boolean = false): Unit = {
    if (!to_save.contains(rowKey)) {
      to_save += (rowKey -> new CountData())
    }
    to_save(rowKey).addExpData(isError)
  }

  def addExpStateCount(rowKey: String, isError: Boolean = false): Unit = {
    if (!to_save.contains(rowKey)) {
      to_save += (rowKey -> new CountData())
    }
    to_save(rowKey).addExpStateData(isError)
  }

  def reset(): Unit = {
    to_save.clear()
  }
}

class EC_Data {
  var ID: String = ""
  var ProviderID: String = ""
  var info: Map[String, String] = Map[String, String]()
  var send: Map[String, String] = Map[String, String]()
  var recv: Map[String, String] = Map[String, String]()
  var pkg: Map[String, String] = Map[String, String]()
  var Ready: Boolean = false

  def parse(record: Record): Unit = {
    Ready = false
    ID = record.ID
    ProviderID = record.ProviderID
    try {
      var data: JSONObject = record.Data.getJSONObject("info")
      var keys: List[String] = List("logisticproviderid", "mailno", "mailtype", "weight", "sencitycode", "reccitycode", "senareacode", "recareacode", "inserttime")
      keys.foreach(
        key => {
          info += (key -> data.getString(key))
        }
      )
      data = record.Data.getJSONObject("send")
      keys = List("senname", "senmobile", "senphone", "senprov", "sencity", "sencounty", "senaddress")
      keys.foreach(
        key => {
          send += (key -> data.getString(key))
        }
      )
      data = record.Data.getJSONObject("recv")
      keys = List("recname", "recmobile", "recphone", "recprov", "reccity", "reccounty", "recaddress")
      keys.foreach(
        key => {
          recv += (key -> data.getString(key))
        }
      )
      data = record.Data.getJSONObject("pkg")
      keys = List("typeofcontents", "nameofcoutents", "mailcode", "recdatetime", "insurancevalue")
      keys.foreach(
        key => {
          pkg += (key -> data.getString(key))
        }
      )
    } catch {
      case ex: JSONException => {
        if (Config.DebugMode) {
          println(ex.toString)
        }
        info = Map[String, String]()
        send = Map[String, String]()
        recv = Map[String, String]()
        pkg = Map[String, String]()
        return
      }
    }
    Ready = true
  }

  def pack(): Put = {
    val put: Put = new Put(ID.getBytes("UTF-8"))
    var family: Array[Byte] = "info".getBytes("UTF-8")
    info.foreach(
      kv_pair => {
        put.addColumn(family, kv_pair._1.getBytes("UTF-8"), kv_pair._2.getBytes("UTF-8"))
      }
    )
    family = "send".getBytes("UTF-8")
    send.foreach(
      kv_pair => {
        put.addColumn(family, kv_pair._1.getBytes("UTF-8"), kv_pair._2.getBytes("UTF-8"))
      }
    )
    family = "recv".getBytes("UTF-8")
    recv.foreach(
      kv_pair => {
        put.addColumn(family, kv_pair._1.getBytes("UTF-8"), kv_pair._2.getBytes("UTF-8"))
      }
    )
    family = "pkg".getBytes("UTF-8")
    pkg.foreach(
      kv_pair => {
        put.addColumn(family, kv_pair._1.getBytes("UTF-8"), kv_pair._2.getBytes("UTF-8"))
      }
    )
    put
  }
}

class EC_Table {
  var to_save: Map[String, EC_Data] = Map[String, EC_Data]()
  var nRecordCount: Int = 0

  def addRecord(record: Record): Unit = {
    val data: EC_Data = new EC_Data()
    data.parse(record)
    to_save += (record.ID -> data)
    nRecordCount += 1
  }

  def reset(): Unit = {
    to_save = Map[String, EC_Data]()
    nRecordCount = 0
  }
}

class ECS_Data {
  var ID: String = ""
  var ProviderID: String = ""
  var info: Map[String, String] = Map[String, String]()
  var state: Map[String, String] = Map[String, String]()
  var contact: Map[String, String] = Map[String, String]()
  var Ready: Boolean = false

  def parse(record: Record): Unit = {
    Ready = false
    ID = record.ID
    ProviderID = record.ProviderID
    try {
      var data: JSONObject = record.Data.getJSONObject("info")
      var keys: List[String] = List("logisticproviderid", "mailno")
      keys.foreach(
        key => {
          info += (key -> data.getString(key))
        }
      )
      data = record.Data.getJSONObject("state")
      keys = List("time", "desc", "city", "facilitytype", "facilityno", "facilityname", "action")
      keys.foreach(
        key => {
          state += (key -> data.getString(key))
        }
      )
      data = record.Data.getJSONObject("contact")
      keys = List("contacter", "contactphone")
      keys.foreach(
        key => {
          contact += (key -> data.getString(key))
        }
      )
    } catch {
      case ex: JSONException => {
        if (Config.DebugMode) {
          println(ex.toString)
        }
        info = Map[String, String]()
        state = Map[String, String]()
        contact = Map[String, String]()
        return
      }
    }
    Ready = true
  }

  def pack(): Put = {
    val put: Put = new Put(ID.getBytes("UTF-8"))
    var family: Array[Byte] = "info".getBytes("UTF-8")
    info.foreach(
      kv_pair => {
        put.addColumn(family, kv_pair._1.getBytes("UTF-8"), kv_pair._2.getBytes("UTF-8"))
      }
    )
    family = "state".getBytes("UTF-8")
    state.foreach(
      kv_pair => {
        put.addColumn(family, kv_pair._1.getBytes("UTF-8"), kv_pair._2.getBytes("UTF-8"))
      }
    )
    family = "contact".getBytes("UTF-8")
    contact.foreach(
      kv_pair => {
        put.addColumn(family, kv_pair._1.getBytes("UTF-8"), kv_pair._2.getBytes("UTF-8"))
      }
    )
    put
  }
}

class ECS_Table {
  var to_save: Map[String, ECS_Data] = Map[String, ECS_Data]()
  var nRecordCount: Int = 0

  def addRecord(record: Record): Unit = {
    val data: ECS_Data = new ECS_Data()
    data.parse(record)
    to_save += (record.ID -> data)
    nRecordCount += 1
  }

  def reset(): Unit = {
    to_save = Map[String, ECS_Data]()
    nRecordCount = 0
  }
}

class ErrorItem {
  var DataID: String = ""
  var Reason: String = ""
  var Handle: String = ""
  var State: Int = 0

  def update(ID: String, reason: String, handle: String, state: Int): Unit = {
    DataID = ID
    Reason = reason
    Handle = handle
    State = state
  }
}

class ER_Table {
  var to_save: Map[String, ErrorItem] = Map[String, ErrorItem]()

  def updateRecord(rowKey: String, DataID: String, Reason: String, Handle: String, State: Int): Unit = {
    if (!to_save.contains(rowKey)) {
      to_save += (rowKey -> new ErrorItem())
    }
    to_save(rowKey).update(DataID, Reason, Handle, State)
  }

  def reset(): Unit = {
    to_save = Map[String, ErrorItem]()
  }
}