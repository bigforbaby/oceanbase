package com.atguigu.gmall.gmallsugar.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.gmallsugar.service.TradeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author lzc
 * @Date 2022/10/4 10:02
 */
@RestController
public class SugarController {
    
    @Autowired
    TradeService tradeService;
    
    @RequestMapping("/sugar/gmv")
    public String gmv(Integer date){
        // 如果前端没有传递日期, 则表示要查询的是今天
        if (date == null) {
            date = Integer.valueOf(new SimpleDateFormat("yyyMMdd").format(new Date()));
        }
    
        BigDecimal gmv = tradeService.gmv(date);
    
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
    
        result.put("data", gmv);
    
        return result.toJSONString();
    }
    
}
