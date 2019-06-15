package com.atguigu.spark.realtime.app

import java.util

import com.atguigu.spark.realtime.bean.AdsInfo
import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * Author lzc
  * Date 2019-06-15 15:30
  */
object BlackListApp {
    val dayUserAdsCount = "day:userid:adsid"
    val blackList = "blacklist"
    
    /*
    检测每条记录, 是否需要把点击这个广告的用户加入到黑名单
     */
    def checkUserToBlackList(adsInfoDStream: DStream[AdsInfo]) = {
        // 连接redis, 让redis去统计每个广告每个用户的点击量
        // 这里创建到 redis的连接1  X
        adsInfoDStream.foreachRDD(rdd => {
            // 这里创建到 redis的连接2 X
            rdd.foreachPartition(infoIt => {
                // 这里创建到 redis的连接3  √
                val client: Jedis = RedisUtil.getJedisClient
                infoIt.foreach(info => {
                    // 1. 记录点击次数
                    val field = s"${info.dayString}:${info.userId}:${info.adsId}"
                    client.hincrBy(dayUserAdsCount, field, 1L)
                    
                    // 2. 判断用户是否要加入到黑名单. 判断刚才写入的那个值是不是到达了阈值
                    val value: String = client.hget(dayUserAdsCount, field)
                    if (value.toLong >= 100000) {
                        client.sadd(blackList, info.userId)
                    }
                })
                client.close()
            })
            
        })
    }
    
    
    def filterBlackList(adsInfoDStream: DStream[AdsInfo], sc:SparkContext): DStream[AdsInfo] = {
        /*
        1. 先拿到黑名单
        
        2. 过滤
         */
        adsInfoDStream.transform(rdd => {
            val client: Jedis = RedisUtil.getJedisClient
            // 1. 获取黑名单
            val blackListSet: util.Set[String] = client.smembers(blackList)
            client.close()
            // 2. 过滤
            val bd: Broadcast[util.Set[String]] = sc.broadcast(blackListSet)
            rdd.filter(info => {
                !bd.value.contains(info.userId)
            })
        })
    }
}

/*



}
1. 记录点击次数
    key                     value(hash  Map)
    "day:userid:adsid"       field                  value
                             "2019-06-15:101:1"      1
    

2. 判断是否添加黑名单(只存储用户的id)
    key                     value(set)
    
    "blacklist"                 101
                                102

 */
