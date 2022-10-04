package com.atguigu.gmall.gmallsugar;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// 让 springboot 只能扫描到 mapper 接口
@MapperScan(basePackages = "com.atguigu.gmall.gmallsugar.mapper")
@SpringBootApplication
public class GmallSugarApplication {

	public static void main(String[] args) {
		SpringApplication.run(GmallSugarApplication.class, args);
	}

}
