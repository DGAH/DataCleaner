import java.text.{ParseException, SimpleDateFormat}

import net.sf.json.JSONObject

/**
  * Created by Lucg on 2017/7/7.
  */

/*
 * 类型：格式错误
 * 描述：数据格式违反约定导致无法解析
 * 处理：记录相关信息并要求重发
 */
class FormatErrorRule extends FilterRule {
  override val ruleID: String = "FormatError"
  override val opinion: Int = Global.OPINION_NEED_RETRY

  override def exam(record: Record): (Int, String) = {
    if (record.ID.isEmpty) {
      return (opinion, "Domain [id] is not found.")
    } else if (record.Type.isEmpty) {
      return (opinion, "Domain [type] is not found.")
    } else if (record.Usage.isEmpty) {
      return (opinion, "Domain [usage] is not found.")
    } else if (record.Data.isNullObject || record.Data.isEmpty) {
      return (opinion, "Domain [data] is not found.")
    } else if (record.ProviderID.isEmpty) {
      return (opinion, "Domain [data.info.logisticproviderid] is not found.")
    }
    (Global.OPINION_NOTHING, "")
  }
}

/*
 * 类型：面单数据不全
 * 描述：某些数据缺失
 * 处理：记录相关信息并要求重发
 */
class ECDataMissingRule extends FilterRule {
  override val ruleID: String = "ECDataMissing"
  override val opinion: Int = Global.OPINION_NEED_RETRY

  override def isAvailable(record: Record): Boolean = {
    record.Type == "ExpressContract"
  }

  override def exam(record: Record): (Int, String) = {
    Global.EC_dataStruct.keys.foreach(
      key => {
        val part: JSONObject = Global.getObject(record.Data, key)
        if (part.isNullObject || part.isEmpty) {
          return (opinion, s"Domain [Data.$key] is missing.")
        }
        val items: List[String] = Global.EC_dataStruct.getOrElse(key, List[String]())
        items.foreach(
          item => {
            val value: String = Global.getValue(part, item, "")
            if (value.isEmpty) {
              if (!Global.Domain_canBeEmpty.getOrElse(item, false)) {
                return (opinion, s"Domain [data.$key.$item] is missing")
              }
            }
          }
        )
      }
    )
    (Global.OPINION_NOTHING, "")
  }
}

/*
 * 类型：状态单数据不全
 * 描述：某些数据缺失
 * 处理：记录相关信息并要求重发
 */
class ECSDataMissingRule extends FilterRule {
  override val ruleID: String = "ECSDataMissing"
  override val opinion: Int = Global.OPINION_NEED_RETRY

  override def isAvailable(record: Record): Boolean = {
    record.Type == "ExpressContractState"
  }

  override def exam(record: Record): (Int, String) = {
    Global.ECS_dataStruct.keys.foreach(
      key => {
        val part: JSONObject = Global.getObject(record.Data, key)
        if (part.isNullObject || part.isEmpty) {
          return (opinion, s"Domain [Data.$key] is missing.")
        }
        val items: List[String] = Global.ECS_dataStruct.getOrElse(key, List[String]())
        items.foreach(
          item => {
            val value: String = Global.getValue(part, item, "")
            if (value.isEmpty) {
              if (!Global.Domain_canBeEmpty.getOrElse(item, false)) {
                return (opinion, s"Domain [data.$key.$item] is missing")
              }
            }
          }
        )
      }
    )
    (Global.OPINION_NOTHING, "")
  }
}

/*
 * 类型：数据错误
 * 描述：某些数据错误，如数据值不在可选范围内等
 * 处理：记录相关信息并要求重发
 */
class DataErrorRule extends FilterRule {
  override val ruleID: String = "DataError"
  override val opinion: Int = Global.OPINION_NEED_RETRY

  override def exam(record: Record): (Int, String) = {
    // Usage: upload or re-upload
    if ((record.Usage!="upload") && (record.Usage!="re-upload")) {
      return (opinion, "Domain [usage] is out of range.")
    }
    // ID: yyyy mm dd tt fffff # n+
    if (record.ID.matches("[0-9]{15}#[0-9]+")) {
      val strdate: String = record.ID.substring(0, 7)
      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
      try {
        sdf.parse(strdate)
      } catch {
        case ex: ParseException => {
          return (opinion, "Domain [id] cannot be parsed.")
        }
      }
    } else {
      return (opinion, "Domain [id] has wrong format.")
    }
    // Type: ExpressContract or ExpressContractState
    if (record.Type == "ExpressContract") {
      examExpressContract(record.Data)
    }
    else if (record.Type == "ExpressContractState") {
      examExpressContractState(record.Data)
    }
    else {
      (opinion, "Domain [type] is out of range.")
    }
  }

  def examExpressContract(jsdata: JSONObject): (Int, String) = {
    //TODO: 现在还不知道有哪些数据范围方面的要求，等明确了具体的细节之后再做补充
    (Global.OPINION_NOTHING, "")
  }

  def examExpressContractState(jsdata: JSONObject): (Int, String) = {
    //TODO: 现在还不知道有哪些数据范围方面的要求，等明确了具体的细节之后再做补充
    (Global.OPINION_NOTHING, "")
  }
}