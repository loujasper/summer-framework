package com.bigdata.summer.framework.core

import java.net.{ServerSocket, Socket}

import com.bigdata.summer.framework.util.{EnvUtils, PropertiesUtil}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

trait TApplication {

  var envData:Any = null

  //第一步：初始化环境

  //def start(t: String = "jdbc")(op: => Unit)(implicit time:Duration = Seconds(5)): Unit = {
  def start(t: String = "jdbc")(op: => Unit): Unit = {

    if (t == "socket") {
      envData = new Socket(PropertiesUtil.getValue("server.host"), PropertiesUtil.getValue("server.port").toInt)
    } else if (t == "serverSocket") {
      envData = new ServerSocket(PropertiesUtil.getValue("server.port").toInt)
    } else if (t == "spark") {
      val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("sparkApplication")
      envData = EnvUtils.getEnv()

    } else if (t == "sparkStreaming") {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkStreaming").set("spark.driver.allowMultipleContexts","true")
      val ssc = new StreamingContext(sparkConf, Seconds(3))
      envData = EnvUtils.getStreamingEnv()
    }

    //TODO 2.业务逻辑
    try {
      op
    } catch {
      case ex: Exception => println("业务执行失败：" + ex.getMessage)
    }

    //环境关闭
    if (t == "serverSocket") {
      val ServerSocket: ServerSocket = envData.asInstanceOf[ServerSocket]
      if (!ServerSocket.isClosed) {
        ServerSocket.close()
      }
    } else if (t == "socket") {
      val socket: Socket = envData.asInstanceOf[Socket]
      if (!socket.isClosed) {
        socket.close()
      }
    } else if (t == "spark") {
      EnvUtils.clean()
    }else if (t == "sparkStreaming"){
      val ssc: StreamingContext = envData.asInstanceOf[StreamingContext]
      ssc.start()
      ssc.awaitTermination()
    }
  }
}
