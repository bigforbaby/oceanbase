package com.atguigu.gmall.gmallsugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @Author lzc
 * @Date 2022/10/4 11:05
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Tm {
    private String tm_name;
    private BigDecimal order_amount;
}
