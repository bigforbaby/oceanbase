package com.atguigu.gmall.gmallsugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lzc
 * @Date 2022/10/4 14:29
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Kw {
    private String keyword;
    private Long score;
}
