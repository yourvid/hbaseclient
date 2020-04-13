package com.orieange.hbase.config.phoenix;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

/**
 *@描述: hbase-phoenix 数据源模版
 */
@Configuration
@PropertySource(value="classpath:application.properties")
@MapperScan(basePackages = HbasePhoenixDataSourceConfig.PACKAGE,sqlSessionFactoryRef = HbasePhoenixDataSourceConfig.HBASEPHOENIX_SQL_SESSION_FACTORY)
public class HbasePhoenixDataSourceConfig {
    static final String HBASEPHOENIX_SQL_SESSION_FACTORY = "hbasePhoenixSqlSessionFactory";
    static final String PACKAGE = "com.orieange.hbase.dao";
    static final String MAPPER_LOCATION = "classpath:mapper/hbase/*.xml";

    @Value("${hbase.phoenix.datasource.url}")
    private String url;


    @Value("${hbase.phoenix.datasource.driverClassName}")
    private String driverClass;

    @Bean(name = "hbasePhoenixDataSource")
    public DataSource hbasePhoenixDataSource() {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(driverClass);
        dataSource.setUrl(url);
        return dataSource;
    }

    @Bean(name = "hbasePhoenixTransactionManager")
    public DataSourceTransactionManager hbasePhoenixTransactionManager(@Qualifier("hbasePhoenixDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean(name = HBASEPHOENIX_SQL_SESSION_FACTORY)
    public SqlSessionFactory hbasePhoenixSqlSessionFactory(@Qualifier("hbasePhoenixDataSource") DataSource hbasePhoenixDataSource)
            throws Exception {
        final SqlSessionFactoryBean sessionFactory = new SqlSessionFactoryBean();
        sessionFactory.setDataSource(hbasePhoenixDataSource);
        sessionFactory.setMapperLocations(new PathMatchingResourcePatternResolver()
                .getResources(HbasePhoenixDataSourceConfig.MAPPER_LOCATION));
        return sessionFactory.getObject();
    }
}