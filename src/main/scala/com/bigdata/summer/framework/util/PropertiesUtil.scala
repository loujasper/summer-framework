package com.bigdata.summer.framework.util


import java.util.ResourceBundle

object PropertiesUtil {
  //绑定配置文件
  val summer: ResourceBundle = ResourceBundle.getBundle("summer")

  def getValue(key: String): String = {

    //传入一个key，返回一个value
    summer.getString(key)

  }
}
