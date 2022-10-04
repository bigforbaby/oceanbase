package com.atguigu.gmall.gmallsugar.service;

import com.atguigu.gmall.gmallsugar.bean.Kw;
import com.atguigu.gmall.gmallsugar.bean.Traffic;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/10/4 11:40
 */
public interface TrafficService {
    List<Traffic> statsTraffic(int date);
    List<Kw> kw(int date);
}
