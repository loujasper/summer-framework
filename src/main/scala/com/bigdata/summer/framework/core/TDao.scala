package com.bigdata.summer.framework.core

import java.util.Properties
import com.bigdata.summer.framework.util.{EnvUtils, PropertiesUtil}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

trait TDao {
  def readFile(path :String) ={

    val fileRDD: RDD[String] = EnvUtils.getEnv().textFile(path)
    fileRDD

  }

  /**
    * 读取kafka数据
    */
  def readKafka(): DStream[String] = {

//    TODO 通过配置PropertiesUtil文件获取到Kafka配置信息
    val brockerList = PropertiesUtil.getValue("kafka.broker.list")
    val groupid = PropertiesUtil.getValue("kafka.group.id")
    val topic = PropertiesUtil.getValue("kafka.topic")

    //  3.定义Kafka参数 //    kafka配置信息
    val kafkaPara: Map[String, Object] = Map[String, Object](

      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brockerList,  //集群配置
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,                                                //消费者组为单位来消费数据
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",           //key反序列化
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer")         //value反序列化  为了提高数据传输效率，将数据进行序列化


    //    使用sparkStreaming读取kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](   //创建一个直连流的方法
        EnvUtils.getStreamingEnv(), //环境对象
        LocationStrategies.PreferConsistent,   //
        ConsumerStrategies.Subscribe[String, String](Set(topic), kafkaPara)    //主题名称Set("atguigu")
      )

    kafkaDStream.map(record => record.value())
  }

  /**
    * 向kafka中写数据
    */
  def writeToKafka( implicit datas :()=> Seq[String] ): Unit ={

    //    TODO 向kafka发送数据

    // 获取配置文件config.properties中的Kafka配置参数
    val broker: String = PropertiesUtil.getValue("kafka.broker.list")
    val topic = PropertiesUtil.getValue("kafka.topic")


    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // 创建Kafka消费者
    val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](prop)

    while (true) {
      // 随机产生实时数据并通过Kafka生产者发送到Kafka集群中
      for (line <- datas()) {
        kafkaProducer.send(new ProducerRecord[String, String](topic, line))
        println(line)
      }
      Thread.sleep(2000)
    }

  }
}
