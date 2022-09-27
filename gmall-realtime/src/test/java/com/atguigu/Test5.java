package com.atguigu;

import com.atguigu.gmall.realtime.bean.KeywordBean;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @Author lzc
 * @Date 2022/9/27 09:02
 */
public class Test5 {
    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        // 返回类中的属性
        // Class.forName("...")  类名.class  对象.getClass()
        Class<KeywordBean> tClass = KeywordBean.class;
    
//        Field[] fields = tClass.getFields(); // 只能获取 public 的属性
//     /   Field[] fields = tClass.getDeclaredFields(); // 获取所有属性, 包括私有的
//        for (Field field : fields) {
//            System.out.println(field.getName());
//        }
    
        KeywordBean bean = new KeywordBean();
        bean.setStt("abc");
        
        // 通过反射获取 stt 属性的值
        // 先拿到 stt 属性
        Field stt = tClass.getDeclaredField("stt");
        stt.setAccessible(true);
        //Object o = stt.get(bean);
        //System.out.println(o);
        
        stt.set(bean,"zs");
        System.out.println(bean);
    
    
        Method fM = tClass.getMethod("f");
        fM.invoke(bean);
       
    
    }
}
