package com.atguigu.spark.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.spark.realtime.bean.AdsInfo
import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
  * Author lzc
  * Date 2019-06-17 15:22
  */
object LastHouClickApp {
    
    def statLastHourAdsCount(adsInfoDStream: DStream[AdsInfo]) = {
        val dSteamWithWindow: DStream[AdsInfo] = adsInfoDStream.window(Minutes(60), Seconds(5))
        
        val formatter = new SimpleDateFormat("HH:mm")
        val adsIdHourMinuteCountDSteam: DStream[(String, (String, Int))] = dSteamWithWindow.map(info => {
            val hourMinute = formatter.format(new Date(info.ts))
            ((info.adsId, hourMinute), 1)
        }).reduceByKey(_ + _).map {
            case ((adsId, hourMinute), count) => (adsId, (hourMinute, count))
        }
    
        val adsHourMinuteCountJsonString: DStream[(String, String)] = adsIdHourMinuteCountDSteam.groupByKey().map {
            case (adsId, housrMinuteCountIt) => {
                import org.json4s.JsonDSL._
                val jsonString: String = JsonMethods.compact(JsonMethods.render(housrMinuteCountIt.toList.sortBy(_._1)))
                (adsId, jsonString)
            }
        }
        
        // 写到redis
    
        adsHourMinuteCountJsonString.foreachRDD(rdd => {
            val result: Array[(String, String)] = rdd.collect()
            val client: Jedis = RedisUtil.getJedisClient
            import scala.collection.JavaConversions._
            client.hmset("last:hour:ads:0105", result.toMap)
            client.close()
        })
        
        
    }
}
/*
统计各广告最近 1 小时内的点击量趋势：各广告最近 1 小时内各分钟的点击量

知识点:
	...withWindow
	
	val dst = dstream.window(长度, 步长)
	
	
实现:



最终的数据格式:
	redis
	
	key									value
	
	"last:hour:ads:click"				hset
										field			value
										adsId			{"15:30" : 100, "15:31" : 200}

 从原始数据
 
=> (adsId, hourMinute), 1)	 reduecBykey
=> (adsId, hourMinute), count)	     map
=> (adsId, (hourMinute, count))	  groupByKey
=> Map[adsId, Iterator[(hourMinute, count)]]
client.hmset("last:hour:ads:click", Map[adsId, jsonString])
 */