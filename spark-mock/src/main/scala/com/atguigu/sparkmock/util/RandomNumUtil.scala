package com.atguigu.sparkmock.util

import java.util.Random

import scala.collection.mutable

/**
  * Author lzc
  * Date 2019-06-12 15:40
  */
object RandomNumUtil {
    val random = new Random()
    
    /**
      *
      * @param from
      * @param to
      * @return [from, to]
      */
    def randomInt(from: Int, to: Int) = {
        if (from > to) throw new IllegalArgumentException("参数异常:from小于等于to")
        
        random.nextInt(to - from + 1) + from
        
    }
    
    def randomLong(from: Long, to: Long) = {
        if (from > to) throw new IllegalArgumentException("参数异常:from小于等于to")
        
        random.nextLong().abs % (to - from + 1) + from
    }
    
    /**
      * 产生多个随机的整数
      * @param from
      * @param to
      * @param count
      * @param canRepeat
      * @return
      */
    def randomMultiInt(from: Int, to: Int, count: Int, canRepeat: Boolean = true): List[Int] = {
        if (canRepeat) { // 允许重复的
            (1 to count).toList.map(_ => randomInt(from, to))
        } else {  // 生成不重复的随机数
            val set = mutable.Set[Int]()
            while (set.size < count) {  // 3
                set += randomInt(from, to)
            }
            set.toList
        }
    }
    
    
    def main(args: Array[String]): Unit = {
        println(randomMultiInt(1, 7, 6))
        println(randomMultiInt(1, 7, 6, false))
    }
}
