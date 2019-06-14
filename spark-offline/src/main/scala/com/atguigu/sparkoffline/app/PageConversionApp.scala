package com.atguigu.sparkoffline.app

import java.text.DecimalFormat

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JDBCUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-06-14 15:51
  */
object PageConversionApp {
    def calc(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction], targetPages: String, taskId: String) = {
        // 1. 先组装跳转流  "1->2", "2->3",...
        val targetFlow: Array[String] = getTargetFlow(targetPages)
        
        // 2. 计算每个目标页面的点击次数 (分母)  1 1000, 2  100
        val targetPagesClickCount: collection.Map[Long, Long] = calcTargetPageClickCount(targetPages, userVisitActionRDD)
    
        // 3. 计算目标跳转次数
        val pages: Array[String] = targetPages.split(",")
        val targetPagesRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(userVisitAction => {
            pages.contains(userVisitAction.page_id.toString)
        })
        val targetPagesGroupedRDD: RDD[(String, Iterable[UserVisitAction])] = targetPagesRDD.groupBy(_.session_id)
        // 得到所有的调转流  // 1->2  2->3  1->2 ....  1->3, 1->4
        val pagesFlow = targetPagesGroupedRDD.flatMap {
            case (sid, it) => {
                // 把用户行为记录按照时间升序排列
                val list: List[UserVisitAction] = it.toList.sortBy(_.action_time) // "1->2"
                val list1: List[UserVisitAction] = list.slice(0, list.length - 1)
                val list2: List[UserVisitAction] = list.slice(1, list.length)
                // 1->2  2->3  1->2 ....  1->3, 1->4
                val pagesFlow: List[String] = list1.zip(list2).map {
                    case (uva1, uva2) => uva1.page_id + "->" + uva2.page_id
                }
                pagesFlow
            }
        }
        val targetPagesFlows: RDD[String] = pagesFlow.filter(flow => targetFlow.contains(flow))
        // 计算出来每个调转流的次数
        val targetPagesFlowCount: RDD[(String, Int)] = targetPagesFlows.map((_, 1)).reduceByKey(_ + _)
        // 4. 跳转率   "1->2" 20%,  2->2 30%...
        val formater = new DecimalFormat(".00%")
        val flowRate: Array[(String, String)] = targetPagesFlowCount.collect.map {
            case (flow, count) => {
                // "1->2"
                val key = flow.split("->")(0).toLong
                val prePageClickCount = targetPagesClickCount(key)
                (flow, formater.format(count.toDouble / prePageClickCount))
            }
        }
        
        // 5. 写到mysql
        val resultArgs = flowRate.map {
            case (flow, rate) => Array(taskId.asInstanceOf[Any], flow, rate)
        }
        JDBCUtil.executeUpdate("use sparkmall0105", null)
        JDBCUtil.executeBatchUpdate("insert into page_conversion_rate values(?, ?, ?)", resultArgs)
    }
    
    /**
      * 计算目标页面的点击次数
      *
      * @param targetPages
      * @param userVisitActionRDD
      * @return
      */
    def calcTargetPageClickCount(targetPages: String, userVisitActionRDD: RDD[UserVisitAction]) = {
        val pages: Array[String] = targetPages.split(",")
        val pages1: Array[String] = pages.slice(0, pages.length - 1) // [0, len-2]
        // 过滤出来目标有页面, 然后统计每个页面被点击的次数
        val targetPagesRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(userVisitAction => {
            pages1.contains(userVisitAction.page_id.toString)
        })
        //        targetPagesRDD.map(x => (x.page_id, 1)).reduceByKey(_ + _)
        targetPagesRDD.map(x => (x.page_id, null)).countByKey()
    }
    
    
    /**
      * 组装需要的跳转流
      *
      * @param targetPages
      * @return
      */
    def getTargetFlow(targetPages: String) = {
        // 1,2,3,4,5,6,7
        val pages: Array[String] = targetPages.split(",")
        val pages1: Array[String] = pages.slice(0, pages.length - 1) // [0, len-2]
        val pages2: Array[String] = pages.slice(1, pages.length) // [1, len-1]
        pages1.zip(pages2).map(pp => pp._1 + "->" + pp._2)
    }
    
    def main(args: Array[String]): Unit = {
        println(getTargetFlow("1,2,3,4,5,6,7").toList)
    }
}

/*
分母: 每个页面的点击次数
分子: 对同一个session来说, 每个跳转流的总和

"1->2" / 1被点击的次数
"2->3" / 2被点击的次数
"6->7" / 6被点击的次数
 */