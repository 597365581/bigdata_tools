package com.yongqing.spring.mybatis.sqloutput.autoconfigure;

import com.yongqing.spring.mybatis.sqloutput.aspect.SqlOutputAdvice;
import com.yongqing.spring.mybatis.sqloutput.plugin.MybatisSqlOutputPlugin;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 *
 */
@Configuration
//@ComponentScan("com.yongqing.spring.mybatis")
@EnableConfigurationProperties({SqlOutputAdvice.class, MybatisSqlOutputPlugin.class})
public class SelfAutoConfig {

}
