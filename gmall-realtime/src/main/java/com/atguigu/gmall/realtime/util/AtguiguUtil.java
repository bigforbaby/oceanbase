package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.bean.KeywordBean;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Field;
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
            .map(Field::getName)
            .collect(Collectors.toList());
    }
    
    
    public static void main(String[] args) {
        List<String> list = getClassFieldsName(KeywordBean.class);
    
        String r = String.join("?", list);
        System.out.println(r);
    }
}
