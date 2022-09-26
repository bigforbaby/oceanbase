package com.atguigu.gmall.realtime.function;

import com.atguigu.gmall.realtime.util.AtguiguUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/9/26 15:11
 */
// 泛型: Row表示制表之后的每行的数据的封装
@FunctionHint(output = @DataTypeHint("row<kw string>"))
public class KwSplit extends TableFunction<Row> {
    public void eval(String s){
        if (s == null) {
            return;
        }
        // 对 s 分词
       List<String> kws =  AtguiguUtil.ikSplit(s);
        for (String kw : kws) {
            collect(Row.of(kw));
        }
    }
}
