package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.common.Constant;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/9/29 11:18
 */
public class Dws_09_DwsTradeSkuOrderWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_09_DwsTradeSkuOrderWindow().init(
            4009,
            2,
            "Dws_09_DwsTradeSkuOrderWindow",
            Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 先按照详情id 进行去重
        distinctByOrderDetailId(stream).print();
        
        // 2. 把数据封装到 pojo 中
        
        // 3. 开窗聚合
        // keyBy: 按照 sku_id
        
        // 4. 补充维度信息
        // 2个优化
        
        // 5. 写出到 clickhouse 中
    }
    
    private SingleOutputStreamOperator<JSONObject> distinctByOrderDetailId(DataStreamSource<String> stream) {
        
        /*
        去重的逻辑:
            row_op_ts 保存这个时间最大的那条数据
            需要把同一个详情 id 的数据放在一起进行比较
        
        1.开窗
            同一个详情 id 的数据放在同一个窗口中
            什么窗口? 基于个数的不行, 应该同一个详情将来有多少条数据不确定
                    用基于时间
                        用 session 窗口
                            可以.  gap 多少? 5s
                            
            
            最后一条数据来了之后, 5s 之后才会出最终结果
            
        2. 定时器
            第一条数据来了之后, 注册定时器: 5s 后触发的定时器
                定时器触发之前, 每来一条数据都进行比较,保留下时间打的那个. 定时器触发的时候, 状态只存储的就是最大的那个
                
            实效性要比开窗要高
            
         3. 如果我需要统计的字段都在左表, 右边根本就没有用到.
            直接保留第一条
         */
        return stream
            .map(JSON::parseObject)
            .keyBy(obj -> obj.getString("id"))
            .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                
                private ValueState<JSONObject> rowOpTsMaxDataState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    rowOpTsMaxDataState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("rowOpTsMaxDataState", JSONObject.class));
                }
                
                @Override
                public void processElement(JSONObject obj,
                                           Context ctx,
                                           Collector<JSONObject> out) throws Exception {
                    // 如果不是第一条, 新数据和状态中的数据进行比较, 保留时间大的那个
                    JSONObject rowOpTsMaxData = rowOpTsMaxDataState.value();
                    
                    if (rowOpTsMaxData == null) {
                        long ts = obj.getLong("ts") * 1000;
                        // 当第一条来的时候注册定时器
                        ctx.timerService().registerEventTimeTimer(ts + 5000);
                        
                        // 数据存入到状态中
                        rowOpTsMaxDataState.update(obj);
                    }else{
                        String preTime = rowOpTsMaxData.getString("row_op_ts");
                        String currentTime = obj.getString("row_op_ts");
                        // 2022-09-29 03:38:59.055Z
                        
                        // 07z=70z
                        // 071z=71z
                    }
                    
                }
            });
    }
}
