package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Author lzc
 * @Date 2022/9/16 11:28
 */
public class DimApp extends BaseAppV1 {
    public static void main(String[] args) {
        new DimApp().init(
            2001,
            2,
            "DimApp",
            Constant.TOPIC_ODS_DB
        );
        
        
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          DataStreamSource<String> stream) {
        // 对流进行业务处理
        
        // 1. 对维度数据(ods_db)做etl: 数据清洗
        SingleOutputStreamOperator<JSONObject> dataStream = elt(stream);
        // 2. 读取维度的配置信息(使用flink cdc)
        SingleOutputStreamOperator<TableProcess> tpStream = readTableProcess(env);
        tpStream.print();
    
        // 3. 让数据流和配置进行connect
        
        
        // 4. 根据connect之后的流的数据, 把相应的维度数据写入到Phoenix中
        
        
    }
    
    private SingleOutputStreamOperator<TableProcess> readTableProcess(StreamExecutionEnvironment env) {
        // 实时的读取配置表的数据
        // 用到cdc方案
        Properties props =  new Properties();
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
    
      return  env
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
                        tp.setOp(op);
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
    
    private SingleOutputStreamOperator<JSONObject> elt(DataStreamSource<String> stream) {
        // 过滤掉脏数据
        return stream
            .filter(jsonString -> {
                // 数据格式要对: 必须json格式的字符串
                try {
                    // 把bootstrap的时候数据的type中的bootstrap-去掉
                    JSONObject obj = JSON.parseObject(jsonString.replaceAll("bootstrap-", ""));
                    String type = obj.getString("type");
                    String data = obj.getString("data");
                    return "gmall2022".equals(obj.getString("database"))
                        && obj.getString("table") != null
                        && ("insert".equals(type) || "update".equals(type))
                        && data != null
                        && data.length() > 2;
                    
                    
                } catch (Exception e) {
                    System.out.println("你的数据是: " + jsonString + " 格式有误, 必须是json格式...");
                    return false;
                }
            })
            // 把type中的bootstrap-insert换成insert
            .map(jsonString -> JSON.parseObject(jsonString.replaceAll("bootstrap-", "")));
            
        
    }
}
/*
维度数据的特点:
    变化比较慢 缓慢变化维度
    
    很多时候, 维度数据一般都是提前准备好的
    
    我们模拟数据的数据的时候, 维度数据只有user_info在变化, 其他维度数据没有任何的变化
    
    怎么维度数据中历史数据?




-----
完成维度从 ods_db 到维度底层


维度数据:
 ods层: ods_db

 目标: flink消费ods_db, 分流, 一个维度表写入到phoenix中的一个表中

 分流使用动态分流: 根据配置动态的把维度分开
 
 ---------
 如何识别出来流中的维度数据?
     
     1. 最简单,也是最不灵活
        在代码中写死, 通过if语句来进行判断
         不灵活, 如果维度表有新增, 需要更新代码
         
         6if,可以判断6张表, 后来如果有新增表或者有表不需要, 这个时候都需要更新代码
         
     2. 灵活配置.
            把需要什么表放在配置信息中, 如果需要的维度信息有变化, 直接更新配置, flink程序应该可以自动感知到这个变化
                当新增一个维度表, flink'程序在不停止的情况下, 自动感知到表的变化
                
          配置信息放在什么位置可以方便的让我们的程序感知?
          
          sku_info  user_info  spu_info
          
          sku_info  user_info  spu_info   base_province
          
          1. 放入到zookeeper的节点, 观察节点
          
          2. 把配置放入到mysql的一个配置表中.
            启动的时候, 可以一次性把所有配置信息读进来.
            整个程序运行期间, 配置可能发生变化(新增配置), 还需要感知到这个变化
            
            周期的去读取mysql中的数据
          
            效率低: 需要频繁的读取mysql中的数据
          
          3. cdc方案
               自动监控到mysql中配置信息的变化
           

维度数据以及读取到了,配置信息也以及读取到了

如何用配置信息去控制维度数据的处理方式?
    把配置信息做成广播流利用广播状态去控制维度数据的处理
    
    把配置信息存入到广播状态, 数据流从广播状态读取自己的配置信息
    
    
    
把维度数据写得出到phoenix中
     自定义phoenix sink
 
 */