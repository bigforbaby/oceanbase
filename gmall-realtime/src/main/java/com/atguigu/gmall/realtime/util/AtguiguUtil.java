package com.atguigu.gmall.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

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
    public static List<String> distinct(List<String> list){
        return new ArrayList<>(new HashSet<>(list));
    }
    
    public static void main(String[] args) {
        System.out.println(ikSplit("手机华为手机256g手机"));
    }
}
