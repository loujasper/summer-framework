package com.bigdata.summer.framework.util

import java.sql.Connection
import java.util

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object JDBCUtil {

  var dataSource:DataSource = init() //初始化连接对象

//  初始化连接池 得到dataSource:DataSource
  def init() : DataSource ={
    val paraMap = new util.HashMap[String,String]()
    paraMap.put("driverClassName",PropertiesUtil.getValue("jdbc.driver.name"))
    paraMap.put("url",PropertiesUtil.getValue("jdbc.url"))
    paraMap.put("username",PropertiesUtil.getValue("jdbc.user"))
    paraMap.put("password",PropertiesUtil.getValue("jdbc.password"))
    paraMap.put("maxActive",PropertiesUtil.getValue("jdbc.datasource.size"))
//    使用Druid连接池对象
    DruidDataSourceFactory.createDataSource(paraMap)

  }

//  TODO 从连接池中获取连接对象
  def getConnection():Connection = {
    dataSource.getConnection
  }

}
