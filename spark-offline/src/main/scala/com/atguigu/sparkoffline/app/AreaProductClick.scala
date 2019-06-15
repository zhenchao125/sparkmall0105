package com.atguigu.sparkoffline.app

import java.util.Properties

import com.atguigu.sparkmall.common.util.ConfigurationUtil
import com.atguigu.sparkoffline.udf.AreaClickUDAF
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Author lzc
  * Date 2019-06-15 09:21
  */
object AreaProductClick {
    
    
    def statAreaClickTop3Product(spark: SparkSession, taskId: String) = {
        // 0 注册自定义聚合函数
        spark.udf.register("city_remark", new AreaClickUDAF)
        // 1. 查询出所有的点击记录,并和城市表产品表做内连接
        spark.sql(
            """
              |select
              |    c.*,
              |    v.click_product_id,
              |	   p.product_name
              |from user_visit_action v join city_info c join product_info p on v.city_id=c.city_id and v.click_product_id=p.product_id
              |where click_product_id>-1
            """.stripMargin).createOrReplaceTempView("t1")
        
        // 2. 计算每个区域, 每个产品的点击量
        spark.sql(
            """
              |select
              |    t1.area,
              |    t1.product_name,
              |    count(*) click_count,
              |    city_remark(t1.city_name)
              |from t1
              |group by t1.area, t1.product_name
            """.stripMargin).createOrReplaceTempView("t2")
        
        // 3. 对每个区域内产品的点击量进行倒序排列
        spark.sql(
            """
              |select
              |    *,
              |    rank() over(partition by t2.area order by t2.click_count desc) rank
              |from t2
            """.stripMargin).createOrReplaceTempView("t3")
        
        // 4. 每个区域取top3
        val conf = ConfigurationUtil("config.properties")
        val props: Properties = new Properties()
        props.setProperty("user", conf.getString("jdbc.user"))
        props.setProperty("password", conf.getString("jdbc.password"))
        spark.sql(
            """
              |select
              |    *
              |from t3
              |where rank<=3
            """.stripMargin)
            .write
            .mode(SaveMode.Overwrite)
            .jdbc(conf.getString("jdbc.url"), "arer_product_click", props)
    }
}

/*
各区域热门商品 Top3

spark sql

涉及到那些表: user_visit_action, city_info

1. 查询出所有的点击记录,并和城市表产品表做内连接
    t1:
    select
        c.*,
        v.click_product_id,
        p.product_name
    from user_visit_action v jon city_info c join product_info p on v.city_id=c.city_id and v.click_product_id=p.product_id
    where click_product_id>-1;

2. 计算每个区域, 每个产品的点击量
    t2:
    select
        t1.area,
        t1.product_name,
        count(*) click_count
    from t1
    group t1.by area, t1.product_name;

3. 对每个区域内产品的点击量进行倒序排列
    t3:
    select
        *,
        rank() over(partition by t2.area oder by t2.click_count desc) rank
    from t2

4. 每个区域去top3
    select
        *
    from t3
    where rank<=3
 */