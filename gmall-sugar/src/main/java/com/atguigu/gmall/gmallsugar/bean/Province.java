package com.atguigu.gmall.gmallsugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @Author lzc
 * @Date 2022/10/4 14:06
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Province {
    private String province_name;
    private BigDecimal order_amount;
    private Long order_count;
}
