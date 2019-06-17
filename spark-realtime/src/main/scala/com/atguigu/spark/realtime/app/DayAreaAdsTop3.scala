package com.atguigu.spark.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
  * Author lzc
  * Date 2019-06-17 11:18
  */
object DayAreaAdsTop3 {
    def statDayAearAdsTop3(dayAreaCityAdsCount: DStream[(String, Int)]) = {
        val dayAreaAdsCount: DStream[(String, Int)] = dayAreaCityAdsCount.map {
            case (dayAearCity, count) => {
                val split: Array[String] = dayAearCity.split(":")
                // ${info.area}:${info.city}:${info.adsId}:${info.dayString}"
                (s"${split(0)}:${split(2)}:${split(3)}", count)
            }
        }
        
        val dayToAreaAdsCountIt: DStream[(String, Iterable[(String, (String, Int))])] = dayAreaAdsCount.reduceByKey(_ + _).map {
            case (areaAdsDay, count) => {
                val Array(area, adsId, day) = areaAdsDay.split(":")
                (day, (area, (adsId, count)))
            }
        }.groupByKey()
        
        val dayAreaAdsCountList: DStream[(String, Map[String, List[(String, Int)]])] = dayToAreaAdsCountIt.map {
            case (day, it) => {
                val areaItMap: Map[String, Iterable[(String, (String, Int))]] = it.groupBy(_._1)
                val temp: Map[String, List[(String, Int)]] = areaItMap.map {
                    case (k, v) => (k, v.map(_._2).toList.sortBy(-_._2).take(3))
                }
                (day, temp)
            }
        }
        // 写入到redis
        //  把list集合转成json字符串
        val dayAreaJson: DStream[(String, Map[String, String])] = dayAreaAdsCountList.map {
            case (day, areaAdsCountMap) => {
                val temp: Map[String, String] = areaAdsCountMap.map {
                    case (area, adsCountList) => {
                        import org.json4s.JsonDSL._
                        (area, JsonMethods.compact(JsonMethods.render(adsCountList)))
                    }
                }
                (day, temp)
            }
        }
        
        dayAreaJson.foreachRDD(rdd => {
            val arr: Array[(String, Map[String, String])] = rdd.collect()
            val client: Jedis = RedisUtil.getJedisClient
            arr.foreach{
                case (day, map) => {
                    // 这里的map是scala的map, 但是这个函数需要java的map
                    import scala.collection.JavaConversions._   // 可以完成从scala到java集合的转换
                    client.hmset("area:ads:top3:" + day, map)
                }
            }
            client.close()
        })
        
    }
}

/*
fastjson 是给java来设计, 目前对scala的集合类型支持的不好

正推:
(area:city:ads:day, count) map
=> (day:area:ads, count)  reduceByKey
=> (day:area:ads, count)  map
=> (day,(area,(ads, count)))  groupByKey
=> (day, Iterator[(area,(ads, count)))])  对迭代器做操作 groupBy(area)
=> (day, Map[(area, Iterator[(area,(ads, count))))])  map
=> (day, Map[(area, Iterator[(ads, count))))])

倒推:
=> RDD[(day, Map[area, Iterator[(ads, count)]])]   map 的时候, 把迭代器变成List
=> RDD[(day, Map[area, List[(ads, count)]])]   排序, 取前3 list转换成json格式的字符串
=> RDD[(day, Map[area, {ads: count, ads: count}])]

client.hmset("area:ads:top3" + $day	, Map[area, {ads: count, ads: count}] )



每天每地区热门广告 Top3

上个需求: 每天每地区每城市每广告的点击量
现在的需求: 每天每地区每广告的点击量

("arer:city:ads:day", count)
=> ("arer:ads:day", count)
=> reduceByKey


-----

redis 写:

key										value
"area:ads:top3" + $day					set
										"{area+ads: count}"
										"{area+ads: count}"
										"{area+ads: count}"
										
------

使用这个:
key										value
"area:ads:top3" + $day					hash
										
										field			value
										area			"{ads: count, ads:count}"
										
										
										
 */