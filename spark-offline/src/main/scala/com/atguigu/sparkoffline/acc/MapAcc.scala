package com.atguigu.sparkoffline.acc

import org.apache.spark.util.AccumulatorV2

/*
  *  In:  (cid, "click")
  *  out: Map[(String, String), Long]
  */
class MapAcc extends AccumulatorV2[(String, String), Map[(String, String), Long]] {
    var map = Map[(String, String), Long]()
    
    // 是否为空
    override def isZero: Boolean = map.isEmpty
    
    // copy 数据
    override def copy(): AccumulatorV2[(String, String), Map[(String, String), Long]] = {
        val acc = new MapAcc
        acc.map ++= map
        acc
    }
    
    // 重置数据
    override def reset(): Unit = {
        map = Map[(String, String), Long]()
    }
    
    // 真正的累加
    /*
        (cid, "click")
        (cid, "order")
     */
    override def add(v: (String, String)): Unit = {
        map += v -> (map.getOrElse(v, 0L) + 1L)
    }
    
    // 合并, 合并map中的 数据  this.map + other.map
    override def merge(other: AccumulatorV2[(String, String), Map[(String, String), Long]]): Unit = {
        other.value.foreach {
            case (k, count) => map += k -> (map.getOrElse(k, 0L) + count)
        }
    }
    
    // 返回 map
    override def value: Map[(String, String), Long] = map
}
