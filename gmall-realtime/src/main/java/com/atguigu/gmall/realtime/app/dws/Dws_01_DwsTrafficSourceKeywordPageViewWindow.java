package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.BaseSQLApp;
import com.atguigu.gmall.realtime.bean.KeywordBean;
import com.atguigu.gmall.realtime.common.Constant;
import com.atguigu.gmall.realtime.function.KwSplit;
import com.atguigu.gmall.realtime.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/9/26 13:48
 */
public class Dws_01_DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new Dws_01_DwsTrafficSourceKeywordPageViewWindow().init(
            4001,
            2,
            "Dws_01_DwsTrafficSourceKeywordPageViewWindow"
        );
    }
    
    @Override
    protected void handle(StreamExecutionEnvironment env,
                          StreamTableEnvironment tEnv) {
        // 1. 建动态, 读取 页面日志
        tEnv.executeSql("create table dwd_page(" +
                            "  page map<string, string>, " +
                            "  ts bigint, " +
                            "  et as to_timestamp_ltz(ts, 3), " +
                            "  watermark for et as et - interval '3' second" +
                            ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRAFFIC_PAGE, "Dws_01_DwsTrafficSourceKeywordPageViewWindow"));
        
        
        // 2. 过滤搜索行为, 取出关键词
        Table t1 = tEnv.sqlQuery("select  " +
                                     " page['item'] keyword, " +
                                     " et " +
                                     "from " +
                                     "dwd_page " +
                                     "where page['last_page_id']='search' " +
                                     "and page['item_type']='keyword' " +
                                     "and page['item'] is not null ");
        tEnv.createTemporaryView("t1", t1);
        // 3. 分词
        // 3.1 注册自定义函数
        tEnv.createTemporaryFunction("kw_split", KwSplit.class);
        // 3.2 在 sql 中使用
        Table t2 = tEnv.sqlQuery("select " +
                                     " kw, " +
                                     " et " +
                                     "from t1 " +
                                     "join lateral table(kw_split(keyword)) on true");
        tEnv.createTemporaryView("t2", t2);
        
        // 4. 开窗聚合  tvf
        Table result = tEnv.sqlQuery("select " +
                                         "date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                                         "date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                                         "'search' source, " +
                                         "kw keyword, " +
                                         "count(*) keyword_count, " +
                                         "unix_timestamp() * 1000 ts " +  // count(*) count(1) count(kw) sum(1)
                                         "from table( tumble( table t2, descriptor(et), interval '5' second ) ) " +
                                         "group by window_start, window_end, kw ");
        
        // 5. 写出到 clickhouse 中
        // 把 result 转成流, 自定义 sink 向外写入
        tEnv
            .toRetractStream(result, KeywordBean.class)
            .filter(t -> t.f0)
            .map(t ->t.f1)
            .addSink(FlinkSinkUtil.getClickHouseSink("dws_traffic_source_keyword_page_view_window", KeywordBean.class));
    
    
        try {
            env.execute("Dws_01_DwsTrafficSourceKeywordPageViewWindow");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    
    }
}
/*
sql 方式:
1. 创建一个动态表与 topic: 页面日志关联

2. 过滤搜索搜索行为日志, 取出关键词

3. 分词
	用 ik 分词器

	"手机 苹果手机 512g"
		手机
	    苹果
	    手机
	    512g

	自定义函数:
		TableFunction

3. 开窗聚会
	
	tvf
		滚动


4. 写出到 clickhouse 中
	用流的方式
		自定义 sink
		
统计热词

数据源:
	搜索行为.

	页面  启动 曝光 活动 错误

	从页面日志中找出搜索页面, 从这个页面中,找到搜索的关键词

 "item": is not null
"item_type": "keyword",
"last_page_id": "search",


取出 item 的值

"手机 苹果手机 512g"  ->1
"手机 华为 512g 5g 黑色" -> 1

搜索的关键分词:
    手机
    苹果
    手机
    512g

统计: 开窗 统计个数
*/