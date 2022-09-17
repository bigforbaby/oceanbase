/**
 * @Author lzc
 * @Date 2022/9/17 10:04
 */
public class Test1 {
    public static void main(String[] args) {
//        System.out.println("id,activity_id,activity_type".replaceAll(",", " varchar,") + " varchar");
        System.out.println("id,activity_id,activity_type".replaceAll("[^,]+", "$0 varchar"));
    }
}
