package com.atguigu.spark.realtime.app

import org.apache.spark.streaming.dstream.DStream

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
        
        dayAreaAdsCount.reduceByKey(_ + _).map{
            case () => {
            
            }
        }
    
    
    }
    
    
    
}
/*
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