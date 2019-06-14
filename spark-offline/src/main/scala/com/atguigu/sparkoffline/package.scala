package com.atguigu
// 包对象
package object sparkoffline {
    def isNotEmpty(s: String) = s != null && s.length > 0
    
    def isEmpty(s: String) = !isNotEmpty(s)
}
