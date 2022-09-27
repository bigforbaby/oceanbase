package com.atguigu;

import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;

/**
 * @Author lzc
 * @Date 2022/9/27 14:27
 */
public class Test6 {
    public static void main(String[] args) {
        System.out.println(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, "abcAaaCcDd"));
    }
}
