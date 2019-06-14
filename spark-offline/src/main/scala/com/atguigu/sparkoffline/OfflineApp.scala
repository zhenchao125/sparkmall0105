package com.atguigu.sparkoffline

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import com.atguigu.sparkoffline.app.CategoryTop10App
import com.atguigu.sparkoffline.bean.Condition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-06-14 08:49
  */
object OfflineApp {
    def main(args: Array[String]): Unit = {
//        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("OfflineApp")
            .enableHiveSupport()
            .getOrCreate()
//        spark.sparkContext.setCheckpointDir("hdfs://hadoop201:9000/ck")
        // 1. 从hive中读取数据
        val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(spark)
        // 对rdd做缓存
        userVisitActionRDD.cache()
        // checkpoint
//        userVisitActionRDD.checkpoint()
        
        // 需求1:
        CategoryTop10App.statCategoryTop10(spark, userVisitActionRDD)
        
        // 需求2:
        
        
    }
    
    def readUserVisitActionRDD(spark: SparkSession): RDD[UserVisitAction] = {
        val condition = readCondition()
        var sql =
            s"""
               |select v.*
               |from user_visit_action v join user_info u on v.user_id=u.user_id
               |where 1=1
             """.stripMargin
        if (isNotEmpty(condition.startDate)) {
            sql += s" and v.date>='${condition.startDate}'"
        }
        if (isNotEmpty(condition.endDate)) {
            sql += s" and v.date<='${condition.endDate}'"
        }
        if (condition.startAge > 0) {
            sql += s" and u.age>=${condition.startAge}"
        }
        if (condition.endAge > 0) {
            sql += s" and u.age<=${condition.endAge}"
        }
        println(sql)
        import spark.implicits._
        spark.sql("use sparkmall0105")
        spark.sql(sql).as[UserVisitAction].rdd
    }
    
    def readCondition() = {
        val conditionString: String = ConfigurationUtil("conditions.properties").getString("condition.params.json")
        JSON.parseObject(conditionString, classOf[Condition])
    }
}

/*
JSON: 解析 序列化

 */
