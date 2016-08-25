package com.frontsurf

import com.frontsurf.util.Conf
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * Spark流式消费Kafka中消息
  * Created by Liupeng on 2016/8/9.
  */
object SparkKafkaConsumer {
  /**
    * 全局配置
    */
  private val conf = Conf.getInstance()

  def main(args: Array[String]) {
    val config = new SparkConf().setAppName("SparkKafkaConsumer").setMaster("local")
    //定义流处理，每5秒一次微批处理
    val ssc = new StreamingContext(config,Durations.seconds(conf.getInterval))
    //配置kafka broker和消费组
    val map = Map(("metadata.broker.list", conf.getKafkaBrokeList),("group.id", conf.getGroupId))
    //配置topic
    val set = Set(conf.getTopic)
    //使用spark提供的KafkaUtils，在Kafka上建立流，持续消费kafka中消息
    val lines = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,map,set)
    println("------Spark Consumer start consume message------")
    //处理并打印消息
    lines.map("Get message------"+_._2).foreachRDD(_.foreach(println))
    ssc.start()
    ssc.awaitTermination()
  }
}