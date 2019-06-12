package com.atguigu.sparkmall.common.util

import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters

import scala.collection.mutable

/**
  * Author lzc
  * Date 2019-06-12 15:29
  */
object ConfigurationUtil {
    // 这个 map 用来存储配置文件名和在这个文件内定义的配置
    private val configs = mutable.Map[String, FileBasedConfiguration]()
    
    // 参数是配置文件名
    def apply(propertiesFileName: String) = {
        // 根据配置文件名来获取来获取对应的配置.
        // 如果 map 中存在这一的配置文件, 则读取配置文件的内容并更新到 map 中
        configs.getOrElseUpdate(
            propertiesFileName,
            new FileBasedConfigurationBuilder[FileBasedConfiguration](
                classOf[PropertiesConfiguration]).configure(new Parameters().properties().setFileName(propertiesFileName)).getConfiguration)
    }
    
    
    def main(args: Array[String]): Unit = {
        val config = ConfigurationUtil("config.properties")
        val value: String = config.getString("hive.database")
        println(value)
        
    }
}
