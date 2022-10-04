package com.atguigu.gmall.gmallsugar.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.gmallsugar.bean.Spu;
import com.atguigu.gmall.gmallsugar.bean.Tm;
import com.atguigu.gmall.gmallsugar.bean.Traffic;
import com.atguigu.gmall.gmallsugar.service.TradeService;
import com.atguigu.gmall.gmallsugar.service.TrafficService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/10/4 10:02
 */
@RestController
public class SugarController {
    
    @Autowired
    TradeService tradeService;
    
    @RequestMapping("/sugar/gmv")
    public String gmv(Integer date) {
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
    
    
    @RequestMapping("/sugar/gmv/spu")
    public String gmvBySpu(Integer date) {
        // 如果前端没有传递日期, 则表示要查询的是今天
        if (date == null) {
            date = Integer.valueOf(new SimpleDateFormat("yyyMMdd").format(new Date()));
        }
        
        List<Spu> list = tradeService.gmvBySpu(date);
        
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        
        JSONObject data = new JSONObject();
        
        JSONArray categories = new JSONArray();
        for (Spu spu : list) {
            categories.add(spu.getSpu_name());
        }
        data.put("categories", categories);
        
        JSONArray series = new JSONArray();
        JSONObject obj = new JSONObject();
        obj.put("name", "产品名字");
        JSONArray data1 = new JSONArray();
        for (Spu spu : list) {
            data1.add(spu.getOrder_amount());
        }
        obj.put("data", data1);
        
        series.add(obj);
        
        data.put("series", series);
        result.put("data", data);
        
        
        return result.toJSONString();
    }
    
    @RequestMapping("/sugar/gmv/tm")
    public String gmvByTm(Integer date) {
        // 如果前端没有传递日期, 则表示要查询的是今天
        if (date == null) {
            date = Integer.valueOf(new SimpleDateFormat("yyyMMdd").format(new Date()));
        }
        
        List<Tm> list = tradeService.gmvByTm(date);
        
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        
        JSONArray data = new JSONArray();
        for (Tm tm : list) {
            JSONObject obj = new JSONObject();
            obj.put("name", tm.getTm_name());
            obj.put("value", tm.getOrder_amount());
            data.add(obj);
        }
        
        result.put("data", data);
        return result.toJSONString();
    }
    
    
    @Autowired
    TrafficService trafficService;
    
    @RequestMapping("/sugar/traffic")
    public String traffic(Integer date) {
        // 如果前端没有传递日期, 则表示要查询的是今天
        if (date == null) {
            date = Integer.valueOf(new SimpleDateFormat("yyyMMdd").format(new Date()));
        }
        
        List<Traffic> list = trafficService.statsTraffic(date);
        // list中的Traffic转存到 map 中, 方便后面来获取
        HashMap<Integer, Traffic> hourToTraffic = new HashMap<>();
        for (Traffic traffic : list) {
            hourToTraffic.put(traffic.getHour(), traffic);
        }
        
        
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        
        JSONObject data = new JSONObject();
        
        JSONArray categories = new JSONArray();
        for (int hour = 0; hour < 24; hour++) {
            categories.add(hour);
        }
        data.put("categories", categories);
        
        JSONArray series = new JSONArray();
        
        JSONObject pv = new JSONObject();
        pv.put("name", "pv");
        JSONArray pvData = new JSONArray();
        pv.put("data", pvData);
        series.add(pv);
        
        JSONObject uv = new JSONObject();
        uv.put("name", "uv");
        JSONArray uvData = new JSONArray();
        uv.put("data", uvData);
        series.add(uv);
        
        JSONObject sv = new JSONObject();
        sv.put("name", "sv");
        JSONArray svData = new JSONArray();
        sv.put("data", svData);
        series.add(sv);
        
        
        for (int hour = 0; hour < 24; hour++) {
            Traffic traffic = hourToTraffic.getOrDefault(hour, new Traffic(hour, 0L, 0L, 0L));
            pvData.add(traffic.getPv());
            uvData.add(traffic.getUv());
            svData.add(traffic.getSv());
        }
        
        data.put("series", series);
        
        result.put("data", data);
        return result.toJSONString();
    }
}
