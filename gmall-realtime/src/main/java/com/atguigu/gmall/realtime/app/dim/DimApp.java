package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.BaseAppV1;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * &#064;Author  lzc
 * &#064;Date  2022/9/16 11:28
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
        // 3. 在Phoenix中建表
        tpStream = createDimTable(tpStream);
        // 4. 让数据流和配置进行connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream = connect(dataStream, tpStream);
        // 5. 把数据中不需要的列过滤掉    resultStream 已经存储了所有维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultStream = filterNoNeedColumns(connectedStream);
        // 6. 根据connect之后的流的数据, 把相应的维度数据写入到Phoenix中
        writeToPhoenix(resultStream);
    }
    
    private void writeToPhoenix(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> stream) {
        /*
        把流中的数据写出到Phoenix中
        1. 找一个Phoenix连接器. 很不幸, 目前没有Phoenix专用连接器
        2. 能否使用jdbc sink?
            JdbcSink.sink( sql语句,  给sql中的占位符赋值,  执行参数, 连接参数)
            sql语句是固定, 那么就意味着只能把流中的数据写入到一个表中
            
            
            实际上, 我们这个流中有多个维度表的数据, 所以不能使用jdbc sink
        3. 自定义sink
         */
        stream.addSink(FlinkSinkUtil.getPhoenixSink());
    }
    
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterNoNeedColumns(
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream) {
        return connectedStream
            .map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
                @Override
                public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> t) throws Exception {
                    JSONObject data = t.f0;
                    // 把需要的所有的列放入到集合中, 放进行判断
                    List<String> columns = Arrays.asList(t.f1.getSinkColumns().split(","));
                    
                    // 遍历map集合, 删除不需要的列, op要保留
                    data.keySet().removeIf(key -> !columns.contains(key) && !"op".equals(key));
                    
                    return t;
                }
            });
    }
    
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(
        SingleOutputStreamOperator<JSONObject> dataStream,
        SingleOutputStreamOperator<TableProcess> tpStream) {
        // 1. 先把配置里做成广播流
        // key: mysql中表名 sourceTable
        // value: TableProcess
        
        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        BroadcastStream<TableProcess> tpBcStream = tpStream.broadcast(tpStateDesc);
        // 2. 让数据流去connect 广播流
        return dataStream
            .connect(tpBcStream)
            // 输出就是每个维度数据配一个配置信息
            .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                @Override
                public void processElement(JSONObject value,
                                           ReadOnlyContext ctx,
                                           Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    // 4. 处理数据流中的数据: 从广播状态读取配置信息, 和数据流组合在一起, 交个后序的流进行写出
                    // 来了一条数据: sku_info的数据, 要找到对应的配置信息
                    
                    ReadOnlyBroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                    // 1. 先根据mysql的表名获取对应你的配置信息
                    String key = value.getString("table");
                    
                    TableProcess tp = state.get(key);// tp后坑是null: 当数据是事实表数据的时候
                    // 2. 把数据和配置信息组成有元组返回
                    if (tp != null) {
                        // 向外输出的时候, 只需要data数据即可, 其他的一些元数据, 不需要了
                        JSONObject data = value.getJSONObject("data");
                        // 给data中新增一个字段: op表示这条数据是更新还是新增. 后期有用
                        data.put("op", value.getString("type"));
                        out.collect(Tuple2.of(data, tp));
                    }
                }
                
                @Override
                public void processBroadcastElement(TableProcess tp,
                                                    Context ctx,
                                                    Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                    // 3. 把配置信息放入到广播状态
                    BroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                    String key = tp.getSourceTable();// mysql中的表名. 前面别写成sinkTable
                    state.put(key, tp);
                }
            });
        
        
    }
    
    private SingleOutputStreamOperator<TableProcess> createDimTable(SingleOutputStreamOperator<TableProcess> tpStream) {
        /*
        根据传来的数据进行建表
        每来一条数据建一张表 ?
            r 读取快照 (建表)
            u 更新 (是否需要对表做操作?  先删表, 再建表. 需要把维度重新同步)
            d 删除 (删除表)
            c 新增 (建表)
        
         */
        
        return tpStream.map(new RichMapFunction<TableProcess, TableProcess>() {
            
            private Connection conn;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                // 建立连接
                conn = JdbcUtil.getPhoenixConnection();
            }
            
            @Override
            public void close() throws Exception {
                // 关闭连接
                JdbcUtil.closeConnection(conn);
            }
            
            @Override
            public TableProcess map(TableProcess tp) throws Exception {
                // 删表和建表
                String sinkType = tp.getSinkType();
                String op = tp.getOp();
                if ("dim".equals(sinkType)) {
                    if ("c".equals(op) || "r".equals(op)) {
                        createTable(tp);
                    } else if ("d".equals(op)) {
                        dropTable(tp);
                    } else { // u
                        dropTable(tp);
                        createTable(tp);
                    }
                }
                return tp;
            }
            
            // 根据配置在Phoenix中建表
            // TODO
            private void createTable(TableProcess tp) throws SQLException {
                
                if (conn.isClosed()) {
                    conn = JdbcUtil.getPhoenixConnection();
                }
            /*
            TableProcess(
                sourceTable=activity_rule
                sourceType=ALL
                sinkTable=dim_activity_rule
                sinkType=dim
                sinkColumns=id,activity_id,activity_type
                sinkPk=id
                sinkExtend=null
                op=r
            )
             */
                // 本质就是执行一个建议语句  表一定要添加主键! 因为主键会成为hbase中的rowkey
                // create table if not exists t(id varchar, name varchar , constraint pk primary key(id)) SALT_BUCKETS = 4;
                StringBuilder sql = new StringBuilder();
                sql
                    .append("create table if not exists ")
                    .append(tp.getSinkTable())
                    .append("(")
                    .append(tp.getSinkColumns().replaceAll("[^,]+", "$0 varchar"))
                    .append(", constraint pk primary key(")
                    // 每张维度表都有一个id, 如果没有提供主键,则用这个id作为主键
                    .append(tp.getSinkPk() == null ? "id" : tp.getSinkPk())
                    .append("))")
                    // 创建盐表(预分区表)
                    .append(tp.getSinkExtend() == null ? "" : tp.getSinkExtend());
                System.out.println("维度表建表语句: " + sql);
                // 1. 使用conn 根据sql语句得到一个预处理语句
                PreparedStatement ps = conn.prepareStatement(sql.toString());
                // 2. 给sql中的占位符进行赋值(建表语句一般没有占位符)
                // 略
                // 3. 执行
                ps.execute();
                // 4. 提交
                conn.commit();
                // 5. 关闭预处理语句
                ps.close();
                
            }
            
            // 根据配置信息在Phoenix中删除表
            //TODO
            private void dropTable(TableProcess tp) throws SQLException {
                if (conn.isClosed()) {
                    conn = JdbcUtil.getPhoenixConnection();
                }
                
                // drop table t;
                String sql = "drop table " + tp.getSinkTable();
                
                System.out.println("维度删表语句: " + sql);
                // 1. 使用conn 根据sql语句得到一个预处理语句
                PreparedStatement ps = conn.prepareStatement(sql);
                // 3. 执行
                ps.execute();
                // 4. 提交
                conn.commit();
                // 5. 关闭预处理语句
                ps.close();
                
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
------
 SALT_BUCKETS = 4
 建盐表 / 给表加盐
 hbase:
    RegionServer 存储 Region
    
    建表的时候, 默认只有一个Region
    
    随着数据规模的增长, Region也会增长
    当Region增长到一定程度之后, 会自动分裂
    
    一分为二
        分裂算法:
            固定大小  10G
            
            新的算法:
                region^3*256 分裂
    
    hadoop162
        1  -> 2
        
    同一张表的Region分裂之后, 会自动迁移. 一般在集群不太忙的时候, 后天迁移
    
    hbase为了避免region的分裂和迁移, 提供了一个预分区功能. 以后就不会自动分裂
    
    每个分区都有一个rowkey的范围
    
      a  b  c
      
---------------

Phoenix建表, 怎么建预分区表?

盐表
 



---------
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
 
 
 错误:
Inconsistent namespace mapping properties. Cannot initiate connection as SYSTEM:CATALOG is found but client does not have phoenix.schema.isNamespaceMappingEnabled enabled

数据库中有 database的概念, 所以Phoenix也有database的概念
hbase中没有database概念, 但是它你有个概念叫 命名空间 namespace, 就等价于数据库的 database

所以说, 如果想要在Phoenix中使用database, 则需要把database和namespace做映射.

 */