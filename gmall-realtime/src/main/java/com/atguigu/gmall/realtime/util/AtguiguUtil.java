package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.annotation.NoNeedSink;
import com.atguigu.gmall.realtime.bean.TradeSkuOrderBean;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Author lzc
 * @Date 2022/9/19 10:16
 */
public class AtguiguUtil {
    
    public static String toDate(Long ts) {
        return new SimpleDateFormat("yyyy-MM-dd").format(ts);
    }
    
    // 使用 ik 实现专业的粉刺
    public static List<String> ikSplit(String s) {
        List<String> result = new ArrayList<>();
        // ik 两种分词模式: smart: 智能, max_word: 最多单词
        // 我是中国人
        // s  -> Reader
        // 内存流:
        StringReader reader = new StringReader(s);
        IKSegmenter seg = new IKSegmenter(reader, true);
        
        try {
            Lexeme next = seg.next();
            while (next != null) {
                result.add(next.getLexemeText());
                next = seg.next();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return distinct(result);
    }
    
    // 返回一个去重后的 List 集合
    public static List<String> distinct(List<String> list) {
        return new ArrayList<>(new HashSet<>(list));
    }
    
    public static <T> List<String> getClassFieldsName(Class<T> tClass) {
        /*Field[] fields = tClass.getDeclaredFields();
        ArrayList<String> list = new ArrayList<>();
        for (int i = 0; i < fields.length; i++) {
            list.add(fields[i].getName());
        }
        return list;*/
        
        // 流式编程
        return Stream
            .of(tClass.getDeclaredFields())
            // 把有标记的过滤掉
            .filter(field -> {
                // 有注解就向外写, 没有注解才写
                NoNeedSink noNeedSink = field.getAnnotation(NoNeedSink.class);
                return noNeedSink == null;
            })
            
            // 为了和表的字段名保持兼容, 需要把属性名改成下划线命名
            .map(f -> {
                String name = f.getName();
                // aaaBbbCcc
                // 把驼峰改成下划线
                return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name);
            })
            .collect(Collectors.toList());
    }
    
    
    public static void main(String[] args) {
        System.out.println(getClassFieldsName(TradeSkuOrderBean.class));
    }
    
    public static String toDateTime(long ts) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);
    }
    
    
    public static Long dateToTs(String date) throws ParseException {
        // 把年月日转成毫秒值
        return new SimpleDateFormat("yyyy-MM-dd").parse(date).getTime();
    }
    
    public static long dateTimeToTs(String dateTime) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateTime).getTime();
    }
    
    // 测试 one 是大于 tow, 如果是返回 true, 否则返回 false
    public static boolean isGreater(String one, String two) {
        one = one.replaceAll("Z", "");
        two = two.replaceAll("Z", "");
        return one.compareTo(two) > 0;
    }
    
    
}
