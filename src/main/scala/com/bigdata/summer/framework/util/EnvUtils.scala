package com.bigdata.summer.framework.util

import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object EnvUtils {
  // 创建一个共享数据
  private val scLocal = new ThreadLocal[SparkContext]
  private val sscLocal = new ThreadLocal[StreamingContext]

  def getStreamingEnv(time :Duration = Seconds(5)) ={

    //从当前线程的共享空间中获取环境对象
    var ssc: StreamingContext = sscLocal.get()

    if (ssc == null) {
      //      如果获取不到环境对象
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkStreaming").set("spark.driver.allowMultipleContexts","true")
      //      则创建新的环境对象
      ssc = new StreamingContext(sparkConf,time)
      //      将创建的新的环境对象方法内存中
      sscLocal.set(ssc)
    }
    ssc
  }

  //获取环境对象
  def getEnv() = {

    //从当前线程的共享空间中获取环境对象
    var sc: SparkContext = scLocal.get()

    if (sc == null) {
//      如果获取不到环境对象
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkApplication")
//      则创建新的环境对象
      sc = new SparkContext(sparkConf)
//      将创建的新的环境对象方法内存中
      scLocal.set(sc)
    }
    sc
  }

  // 清除对象
  def clean() = {
    getEnv().stop()

    // 将共享内存中的数据清除
    scLocal.remove()
  }
}
