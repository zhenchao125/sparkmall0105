package com.atguigu.sparkoffline

import java.util.UUID

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import com.atguigu.sparkoffline.app.AreaProductClick
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
        val taskId: String = UUID.randomUUID().toString
        
        // 需求1:
        //        println("需求1 开始")
        //        val categoryTop10: List[CategoryCountInfo] = CategoryTop10App.statCategoryTop10(spark, userVisitActionRDD, taskId)
        //        println("需求1 结束")
        
        // 需求2: Top10热门品类中每个品类的 Top10 活跃 Session 统计
        //        CategorySessionTop10App.statCategoryTop10Session(spark, userVisitActionRDD, categoryTop10, taskId)
        
        // 需求3:
        //        println("需求3 开始")
        //        PageConversionApp.calc(spark, userVisitActionRDD, readCondition.targetPageFlow, taskId)
        //        println("需求3 结束")
        
        // 需求4:
        AreaProductClick.statAreaClickTop3Product(spark, taskId)
        
        
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
