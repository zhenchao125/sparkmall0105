package com.atguigu.spark.realtime

import com.atguigu.spark.realtime.app.{BlackListApp, DayAreaCityAdsApp}
import com.atguigu.spark.realtime.bean.AdsInfo
import com.atguigu.sparkmall.common.util.MyKafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-06-15 15:12
  */
object RealtimeApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setAppName("RealtimeApp")
            .setMaster("local[2]")
        
        val ssc = new StreamingContext(conf, Seconds(5))
        val dStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getDStream(ssc, "ads_log_0105")
        
        val adsInfoDStream: DStream[AdsInfo] = dStream.map(record => {
            val arr: Array[String] = record.value.split(",")
            AdsInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
        })
        
        // 需求 5: 黑名单
        val filteredDStream: DStream[AdsInfo] = BlackListApp.filterBlackList(adsInfoDStream, ssc.sparkContext)
        
        BlackListApp.checkUserToBlackList(filteredDStream) // 检测用户是否需要加入黑名单, 只对过滤后的处理
        
        // 需求 6:
        DayAreaCityAdsApp.statAreaCityAdsCountPerDay(filteredDStream, ssc.sparkContext)
        
        ssc.start()
        ssc.awaitTermination()
    }
}
