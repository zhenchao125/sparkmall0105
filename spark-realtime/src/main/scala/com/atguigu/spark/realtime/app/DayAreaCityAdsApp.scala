package com.atguigu.spark.realtime.app

import com.atguigu.spark.realtime.bean.AdsInfo
import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * Author lzc
  * Date 2019-06-17 09:09
  */
object DayAreaCityAdsApp {
    
    def statAreaCityAdsCountPerDay(adsInfoDStream: DStream[AdsInfo], spark: SparkContext) = {
        // 1. 统计数据
        val areaCityAdsToOne: DStream[(String, Int)] = adsInfoDStream.map(info => {
            (s"${info.area}:${info.city}:${info.adsId}:${info.dayString}", 1)
        })
        
        spark.setCheckpointDir("./ck1")
        val areaCityAdsToCount: DStream[(String, Int)] = areaCityAdsToOne.updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
            Some(seq.sum + opt.getOrElse(0))
        })
        
        
        // 2. 写入到redis
        areaCityAdsToCount.foreachRDD(rdd => {
            rdd.foreachPartition(it => {
                val client: Jedis = RedisUtil.getJedisClient
                it.foreach {
                    case (areaCityAdsDate, count) => {
                        val split: Array[String] = areaCityAdsDate.split(":")
                        val key = "area:clity:ads:" + split(split.length - 1)
                        val field = split.slice(0, split.length - 1).mkString(":")
                        client.hset(key, field, count.toString)
                    }
                }
                
                client.close()
                
            })
        })
    }
}

/*
每天各地区各城市各广告的点击流量实时统计

最终去 redis


k											v
"area:clity:" + $date					   map   {华北:北京:1 -> 100, }
										   hash
										   
											field    		value
											
											华北:北京:1     100
											
											
----------


k											v
"area:clity:ads"					        map   {华北:北京:1 -> 100, }
										    hash
										   
											field    		    value
											
											华北:北京:1:$date     100

DStream[AdsInfo]   map
=> DStream[("华北:北京:10", 1)]  updateStateByKey()
=> DStream[("华北:北京:10", 1)]

DStream[("华北:北京:10", 100)]
 */