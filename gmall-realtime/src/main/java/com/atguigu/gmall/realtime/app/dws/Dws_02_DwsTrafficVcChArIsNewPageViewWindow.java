package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.AtguiguUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Author lzc
 * @Date 2022/9/27 11:06
 */
public class Dws_02_DwsTrafficVcChArIsNewPageViewWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_02_DwsTrafficVcChArIsNewPageViewWindow().init(
            4002,
            2,
            "Dws_02_DwsTrafficVcChArIsNewPageViewWindow",
            Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        // 1. 先封装数据:TrafficPageViewBean
        parseToBean(stream);
        
        // 2. 开窗聚会
        
        // 3. 写出到 clickhouse 中
    }
    
    private void parseToBean(DataStreamSource<String> stream) {
        stream
            .keyBy(str -> JSON.parseObject(str).getJSONObject("common").getString("uid"))
            .map(new RichMapFunction<String, TrafficPageViewBean>() {
                
                private ValueState<String> dateState;
                private ValueState<String> uidState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 存储用户 id
                    uidState = getRuntimeContext().getState(new ValueStateDescriptor<String>("uidState", String.class));
                    
                    dateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("dateState", String.class));
                }
                
                @Override
                public TrafficPageViewBean map(String value) throws Exception {
                    JSONObject obj = JSON.parseObject(value);
                    
                    JSONObject common = obj.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");
                    
                    JSONObject page = obj.getJSONObject("page");
                    
                    Long pvCt = 1L;
                    Long durSum = page.getLong("during_time");
                    
                    Long svCt = 0L; // 会话数
                    if (page.getString("last_page_id") == null) {
                        svCt = 1L;
                    }
                    
                    // 判断是否到了第二天
                    String today = AtguiguUtil.toDate(obj.getLong("ts"));
                    if (!today.equals(dateState.value())) {
                        // 如果换天,.则需要清除 uid
                        uidState.clear();
                        // 把日期状态中的值更新
                        dateState.update(today);
                    }
                    
                    Long uvCt = 0L;
                    // 判断这个 uid 是否来过, 如果来过则 uvCt=0 否则就是设置为 1. 一天为限
                    if (uidState.value() == null) {
                        // 这个用户第一次来是 null
                        uidState.update(common.getString("uid"));
                        uvCt = 1L;
                    }
                    return new TrafficPageViewBean(
                        "", "",
                        vc, ch, ar, isNew,
                        uvCt, svCt, pvCt, durSum,
                        obj.getLong("ts")
                    );
                }
            })
            .print();
    }
}
