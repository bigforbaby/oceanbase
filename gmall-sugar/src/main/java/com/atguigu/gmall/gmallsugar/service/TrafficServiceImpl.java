package com.atguigu.gmall.gmallsugar.service;

import com.atguigu.gmall.gmallsugar.bean.Kw;
import com.atguigu.gmall.gmallsugar.bean.Traffic;
import com.atguigu.gmall.gmallsugar.mapper.TrafficMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/10/4 11:40
 */
@Service
public class TrafficServiceImpl implements TrafficService{
    @Autowired
    TrafficMapper trafficMapper;
    @Override
    public List<Traffic> statsTraffic(int date) {
        return trafficMapper.statsTraffic(date);
    }
    
    @Override
    public List<Kw> kw(int date) {
        return trafficMapper.kw(date);
    }
    
}
