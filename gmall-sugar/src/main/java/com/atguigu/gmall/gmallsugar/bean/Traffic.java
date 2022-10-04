package com.atguigu.gmall.gmallsugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author lzc
 * @Date 2022/10/4 11:38
 */
@Data
@AllArgsConstructor
public class Traffic {
    private Integer hour;
    private Long pv;
    private Long uv;
    private Long sv;
}
