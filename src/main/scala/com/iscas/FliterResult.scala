package com.iscas

class FliterResult(ruleID: String = "", opinion: Int = Global.OPINION_NOTHING, message: String = "") {
  var RuleID: String = ruleID
  var Opinion: Int = opinion
  var Message: String = message
  var Data: Record = null

  def isOK: Boolean = {
    opinion == Global.OPINION_NOTHING
  }
}
