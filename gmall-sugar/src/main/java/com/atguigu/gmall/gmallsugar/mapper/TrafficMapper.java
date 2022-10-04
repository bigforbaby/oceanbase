package com.atguigu.gmall.gmallsugar.mapper;

import com.atguigu.gmall.gmallsugar.bean.Traffic;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/10/4 11:37
 */
public interface TrafficMapper {
    @Select("SELECT\n" +
        "    toHour(stt) AS hour,\n" +
        "    sum(pv_ct) AS pv,\n" +
        "    sum(uv_ct) AS uv,\n" +
        "    sum(sv_ct) AS sv\n" +
        "FROM dws_traffic_vc_ch_ar_is_new_page_view_window\n" +
        "WHERE toYYYYMMDD(stt) = #{date}\n" +
        "GROUP BY toHour(stt)")
   List<Traffic> statsTraffic(int date);
}
