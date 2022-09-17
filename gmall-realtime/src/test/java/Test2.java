import net.minidev.json.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * @Author lzc
 * @Date 2022/9/17 14:00
 */
public class Test2 {
    public static void main(String[] args) {
        JSONObject data = new JSONObject();
        data.put("a", 1);
        data.put("b", 1);
        data.put("c", 1);
    
    
        List<String> list = Arrays.asList("a,c".split(","));
    
        Set<String> set = data.keySet();
        // 删除集合的时候, 永远不要使用for循环
        /*for (String key : set) {
            if (!list.contains(key)) {
                set.remove(key);
            }
        }*/
    
        /*Iterator<String> it = set.iterator();
        while (it.hasNext()) {
            if (!list.contains(it.next())) {
                it.remove();
            }
        }
    */
        set.removeIf(s -> !list.contains(s));
        System.out.println(data);
    }
}
