package com.iscas

/**
  * Created by Lucg on 2017/7/5.
  * 过滤规则基类
  * 是所有将应用的过滤规则的抽象化，各过滤规则需重写其中的有关内容以实现具体的过滤效果
  */
class FilterRule {
  val ruleID: String = ""
  val opinion: Int = Global.OPINION_UNCERTAIN

  def isAvailable(record: Record): Boolean = {
    true
  }

  def exam(record: Record): (Int, String) = {
    (Global.OPINION_NOTHING, "")
  }

  def correct(record: Record): Record = {
    record
  }
}
