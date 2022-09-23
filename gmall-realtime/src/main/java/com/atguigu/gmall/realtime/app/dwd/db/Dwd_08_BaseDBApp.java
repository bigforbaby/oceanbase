package com.atguigu.gmall.realtime.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Author lzc
 * @Date 2022/9/23 11:04
 */
public class Dwd_08_BaseDBApp extends BaseAppV1 {
    public static void main(String[] args) {
        new Dwd_08_BaseDBApp().init(
            3008,
            2,
            "Dwd_08_BaseDBApp",
            Constant.TOPIC_ODS_DB
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        // 1. 对业务数据进行 etl
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);
        
        // 2. 读取配置表数据(cdc)
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(env);
        // 3. 清洗后的数据与配置表进行 connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream = connect(etledStream, tpStream);
        // 5. 过滤掉不需要的字段
        connectedStream = filterNoNeedColumns(connectedStream);
        // 6. 写出到 kafka 中
        writeToKafka(connectedStream);
        
    }
    
    private void writeToKafka(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        // 重新实现一个新的 kafka sink, 这个时候的 topic 是由数据本身来决定
        stream.addSink(FlinkSinkUtil.getKafkaSink());
    }
    
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterNoNeedColumns(
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        return stream.map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
            @Override
            public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> value) throws Exception {
                JSONObject data = value.f0;
                TableProcess tp = value.f1;
    
                List<String> needColumns = Arrays.asList(tp.getSinkColumns().split(","));
                data.keySet().removeIf(key -> !needColumns.contains(key));
                return value;
            }
        });
    }
    
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(SingleOutputStreamOperator<JSONObject> dataStream,
                                                                                 SingleOutputStreamOperator<TableProcess> tpStream) {
        // 1. 把配置流做成广播流
        // key:表名    value:TableProcess
        // 读或写的时候都能得到这个表名
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        BroadcastStream<TableProcess> tpBcStream = tpStream.broadcast(tpStateDesc);
        // 2. 数据流和广播流进行 connect
        return dataStream
            .connect(tpBcStream)
            .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                
                // 4. 根据广播状态中的配置信息, 来处理数据流中的数据
                @Override
                public void processElement(JSONObject obj,
                                           ReadOnlyContext ctx,
                                           Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    // mysql 表名:操作类型
                    String key = obj.getString("table") + ":" + obj.getString("type") + ":";
                    ReadOnlyBroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                    TableProcess tp = state.get(key);
                    if (tp != null) {
                        String sinkType = tp.getSinkType();
                        // 首先是事实表的操作
                        if ("dwd".equals(sinkType)) {
                            // 数据  配置信息
                            out.collect(Tuple2.of(obj.getJSONObject("data"), tp));
                        }
                    } else {
                        //tp取数据是不可能的, 因为 tp=null
                        // 通过这个 key 没有找到对应的配置信息. 需要对 coupon_use 的下单和支付
                        //TODO  dwd_tool_coupon_order 和 dwd_tool_coupon_pay
                        String table = obj.getString("table");
                        String type = obj.getString("type");
                        if ("coupon_use".equals(table) && "update".equals(type)) {
                            // 特殊处理
                            JSONObject data = obj.getJSONObject("data");
                            JSONObject old = obj.getJSONObject("old");
                            
                            if (data.getString("used_time") != null) {
                                // 这次使用优惠券支付完毕
                                tp = state.get(key + "{\"data\": {\"used_time\": \"not null\"}}");
                                out.collect(Tuple2.of(data, tp));
                            } else if (old != null && "1402".equals(data.getString("coupon_status")) && "1401".equals(old.getString("coupon_status"))) {
                                tp = state.get(key + "{\"data\": {\"coupon_status\": \"1402\"}, \"old\": {\"coupon_status\": \"1401\"}}");
                                out.collect(Tuple2.of(data, tp));
                            }
                        }
                    }
                    
                    
                }
                
                // 3. 广播流中的数据: 放入到广播状态中
                @Override
                public void processBroadcastElement(TableProcess tp,
                                                    Context ctx,
                                                    Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    // 先把配置信息写入到广播状态中
                    // key: source_table:source_type:sink_extend
                    // value: Tp
                    BroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                    // 拼接tp.getSinkExtend()目的是为了防止覆盖
                    String key = tp.getSourceTable() + ":" + tp.getSourceType() + ":" + (tp.getSinkExtend() == null ? "" : tp.getSinkExtend());
                    if ("d".equals(tp.getOp())) { // 如果有配置信息被删除, 则应该把这个信息从广播状态中移除
                        state.remove(key);
                    } else {
                        state.put(key, tp);
                    }
                }
            });
        
        
    }
    
    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        // 实时的读取配置表的数据
        // 用到cdc方案
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        // 一启动默认先读取表中所有数据(快照), 然后再监控binlog读取变化的数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("hadoop162")
            .port(3306)
            .databaseList("gmall_config") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
            .tableList("gmall_config.table_process") // set captured table
            .username("root")
            .password("aaaaaa")  // jdbc:mysql:/....?useSSL=false
            .jdbcProperties(props) // 解决ssl报错的问题
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .build();
        
        return env
            .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql cdc source")
            .flatMap(new FlatMapFunction<String, TableProcess>() {
                /*
                关于对配置表进行修改的时候, 数据特点:
                1. 新增数据:  before = null after=新增的值  op=c
                2. 删除数据   before =删除之前的值 after=null op=d
                3. 更新数据
                        更新主键: 有两条数据
                            先删除原数据
                                before = 有值 after=null op=d
                            再新增
                                before = null after=有值  op=c
                        更新非主键: 只有一条数据
                                before=更新前的值 after是更新后的值  op=u
                                
                 4. 读取快照的时候:
                      before =null  after=值  op=r
                 */
                @Override
                public void flatMap(String value,
                                    Collector<TableProcess> out) throws Exception {
                    JSONObject obj = JSON.parseObject(value);
                    
                    String op = obj.getString("op");
                    if ("d".equals(op)) {
                        //op=d
                        // 如果是删除的数据, 则把before拿出来, 后面通过这里的信息知道谁被删除了
                        TableProcess tp = obj.getObject("before", TableProcess.class);
                        // 把op封装到对象中的目的是为了后期能够判断出这次是什么操作: 删除or新增
                        tp.setOp(op);  // d
                        out.collect(tp);
                    } else {   //r u c
                        TableProcess tp = obj.getObject("after", TableProcess.class);
                        tp.setOp(op);
                        out.collect(tp);
                    }
                }
            });
        
        // 流中的数据有两种: 删除 新增 (更新是分解成了删除和更新)
        
    }
    
    /*private void readTableProcess(StreamExecutionEnvironment env) {
        // 用 sql 的方式读写
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // 启动读取快照, 会用多并行度的方式读取
        // 读取 binlog 的时候, 只会用一个并行度去读取
        tEnv.executeSql("CREATE TABLE tp (" +
                            " `source_table` string," +
                            " `source_type` string," +
                            " `sink_table` string ," +
                            " `sink_type` string ," +
                            " `sink_columns` string," +
                            " `sink_pk` string," +
                            " `sink_extend` string," +
                            "  PRIMARY KEY (`sink_table`) NOT ENFORCED" +
                            "     ) WITH (" +
                            "     'connector' = 'mysql-cdc'," +
                            "     'hostname' = 'hadoop162'," +
                            "     'port' = '3306'," +
                            "     'username' = 'root'," +
                            "     'password' = 'aaaaaa'," +
                            "     'database-name' = 'gmall_config'," +
                            "     'table-name' = 'table_process')");
    
        Table tp = tEnv.from("tp");
    
        // 配置表有可能新增, 也可能删除
        DataStream<Tuple2<Boolean, JSONObject>> result = tEnv.toRetractStream(tp, JSONObject.class);
        result.print();
    
    
    }*/
    
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
            .filter(jsonStr -> {
                try {
                    JSONObject obj = JSON.parseObject(jsonStr);
                    String type = obj.getString("type");
                    if (obj.getString("table").equals("coupon_use")) {
                        System.out.println(obj);
                    }
                    return "gmall2022".equals(obj.getString("database"))
                        && ("insert".equals(type) || "update".equals(type));
                } catch (Exception e) {
                    System.out.println("数据是: " + jsonStr + "   格式不对, 请核对...");
                    return false;
                }
            })
            .map(JSON::parseObject);
        
    }
}
/*
消费 topic , 使用流的方式
 */