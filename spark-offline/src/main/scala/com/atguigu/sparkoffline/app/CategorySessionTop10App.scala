package com.atguigu.sparkoffline.app

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkoffline.bean.{CategoryCountInfo, CategorySession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-06-14 14:01
  */
object CategorySessionTop10App {
    
    def statCategoryTop10Session(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction], categoryTop10: List[CategoryCountInfo], taskId: String) = {
        // 1. 得到top10的品类的id
        val categoryIdTop10: List[String] = categoryTop10.map(_.categoryId)
        // 2. 过去出来只包含 top10 品类id的那些用户行为
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(UserVisitAction => {
            categoryIdTop10.contains(UserVisitAction.click_category_id.toString)
        })
        //  => RDD[(品类id, sessionId))] map
        //    => RDD[(品类id, sessionId), 1)]
        val categorySessionOne: RDD[((Long, String), Int)] = filteredUserVisitActionRDD
            .map(userVisitAction => ((userVisitAction.click_category_id, userVisitAction.session_id), 1))
        // RDD[(品类id, sessionId), count)]
        val categorySessionCount: RDD[(Long, (String, Int))] =
            categorySessionOne.reduceByKey(_ + _).map {
                case ((cid, sid), count) => (cid, (sid, count))
            }
        // RDD[品类id, Iterator[(sessionId, count)]]
        val categorySessionCountGrouped: RDD[(Long, Iterable[(String, Int)])] = categorySessionCount.groupByKey
        
        val rdd = categorySessionCountGrouped.flatMap {
            case (cid, it) => {
                val list: List[(String, Int)] = it.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
                val result: List[CategorySession] = list.map {
                    case (sid, count) => CategorySession(taskId, cid.toString, sid, count)
                }
                result
            }
        }
        rdd.collect.foreach(println)
        //  4. 写入到mysql
        
    }
}

/*
1. 得到top10的品类的id

2. 过去出来只包含 top10 品类id的那些用户行为

3. 分组计算
    => RDD[(品类id, sessionId))] map
    => RDD[(品类id, sessionId), 1)]  reduceByKey
    => RDD[(品类id, sessionId), count)]    map
    => RDD[品类id, (sessionId, count)]     groupByKey
    RDD[品类id, Iterator[(sessionId, count)]]
    

4. 写入到mysql

 */