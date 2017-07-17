package com.iscas

import scala.util.control.Breaks

/**
  * Created by Lucg on 2017/7/6.
  */
object Filter {
  var rules: Array[FilterRule] = Array[FilterRule](
    new FormatErrorRule(),
    new ECDataMissingRule(),
    new ECSDataMissingRule(),
    new DataErrorRule()
  )

  var handles: Map[Int, (Record, String, String) => Unit] = Map[Int, (Record, String, String) => Unit]()

  def work(record: Record): (String, Int, String) = {
    var ruleID: String = ""
    var opinion: Int = Global.OPINION_NOTHING
    var message: String = ""
    val loop: Breaks = new Breaks()
    loop.breakable {
      for (rule <- rules) {
        if (rule.isAvailable(record)) {
          val result: (Int, String) = rule.exam(record)
          opinion = result._1
          if (opinion != Global.OPINION_NOTHING) {
            ruleID = rule.ruleID
            message = result._2
            loop.break()
          }
        }
      }
    }
    (ruleID, opinion, message)
  }

  def getHandleFunc(opinion: Int): (Record, String, String) => Unit = {
    handles.getOrElse(opinion, null)
  }
}
