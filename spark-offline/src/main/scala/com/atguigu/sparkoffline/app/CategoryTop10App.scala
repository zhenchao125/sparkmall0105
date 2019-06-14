package com.atguigu.sparkoffline.app

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkoffline.acc.MapAcc
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategoryTop10App {
    
    // 计算 calculate  统计 statistics
    
    def statCategoryTop10(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction]) ={
        
        // 1. 统计出来要的指标.
        // 1.1 遍历 userVisitActionRDD, 一次统计出来所有的指标
        // 需要累加器, 来记录统计的数据
        var acc = new MapAcc
        spark.sparkContext.register(acc)
        userVisitActionRDD.foreach(userVisitAction => {
            if(userVisitAction.click_category_id != -1){
                acc.add((userVisitAction.click_category_id + "", "click"))
            }else if(userVisitAction.order_category_ids != null){
                // 1,2,3,4
                userVisitAction.order_category_ids.split(",").foreach(cid => {
                    acc.add((cid, "order"))
                })
            }else{
                userVisitAction.pay_category_ids.split(",").foreach(cid => {
                    acc.add((cid, "pay"))
                })
            }
        })
        // 1.2 排序, 取前10
        val categoryActionCountMap: Map[(String, String), Long] = acc.value
        // (1, "click")-> 1000  按照categoryId 进行分组
        val categoryActionGroupedMap: Map[String, Map[(String, String), Long]] = categoryActionCountMap.groupBy(kv => kv._1._1)
        // 把需要向外写的数据,封装到一个对象中.
        
        // 2. 把指标写入到 mysql
    }
}

/*
累加器:
    (cid, "click") -> 10000
    (cid, "oder") -> 500
    (cid, "pay") -> 100
 */