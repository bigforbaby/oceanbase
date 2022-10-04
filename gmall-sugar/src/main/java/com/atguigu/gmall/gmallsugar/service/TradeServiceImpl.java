package com.atguigu.gmall.gmallsugar.service;

import com.atguigu.gmall.gmallsugar.mapper.TradeMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

// 添加 service 注解, springboot 才认为这是一个 service
@Service
public class TradeServiceImpl implements TradeService {
    
    @Autowired
    TradeMapper tradeMapper;
    @Override
    public BigDecimal gmv(int date) {
        return tradeMapper.gmv(date);
    }
}
