package com.orieange.hbase.config.phoenix;

import com.alibaba.druid.pool.DruidDataSource;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.MybatisXMLLanguageDriver;
import com.baomidou.mybatisplus.extension.plugins.PaginationInterceptor;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.type.JdbcType;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

/**
 *@描述: hbase-phoenix 数据源模版
 */
@Configuration
@MapperScan(basePackages = HbasePhoenixDataSourceConfig.PACKAGE,sqlSessionFactoryRef = HbasePhoenixDataSourceConfig.HBASEPHOENIX_SQL_SESSION_FACTORY)
public class HbasePhoenixDataSourceConfig {
    static final String HBASEPHOENIX_SQL_SESSION_FACTORY = "hbasePhoenixSqlSessionFactory";
    static final String PACKAGE = "com.orieange.hbase.dao.phoenix";
    static final String MAPPER_LOCATION = "classpath*:/mapper/hbase/*.xml";
    static final String SESSION_FACTORY = "hbasePhoenixSqlSessionFactory";
    static final String TRANSACTION_MANAGER = "hbasePhoenixTransactionManager";
    static final String DATA_SOURCE = "hbasePhoenixDataSource";
    static final String SQL_SESSION_TEMPLATE = "hbasePhoenixSqlSessionTemplate";

    @Value("${hbase.phoenix.datasource.url}")
    private String url;


    @Value("${hbase.phoenix.datasource.driverClassName}")
    private String driverClass;

    @Bean(name = DATA_SOURCE)
    public DataSource hbasePhoenixDataSource() {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(driverClass);
        dataSource.setUrl(url);
        return dataSource;
    }

    @Bean(name = TRANSACTION_MANAGER)
    public DataSourceTransactionManager hbasePhoenixTransactionManager(@Qualifier(DATA_SOURCE) DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean(name = SQL_SESSION_TEMPLATE)
    public SqlSessionTemplate sqlSessionTemplate(@Qualifier(SESSION_FACTORY) SqlSessionFactory sqlSessionFactory)
            throws Exception {
        return new SqlSessionTemplate(sqlSessionFactory);
    }

    @Bean(name = HBASEPHOENIX_SQL_SESSION_FACTORY)
    public SqlSessionFactory sqlSessionFactory(@Qualifier(DATA_SOURCE) DataSource dataSource) throws Exception {
        MybatisSqlSessionFactoryBean sqlSessionFactory = new MybatisSqlSessionFactoryBean();
        sqlSessionFactory.setDataSource(dataSource);
        MybatisConfiguration configuration = new MybatisConfiguration();
        configuration.setDefaultScriptingLanguage(MybatisXMLLanguageDriver.class);
        configuration.setJdbcTypeForNull(JdbcType.NULL);
        sqlSessionFactory.setConfiguration(configuration);
        sqlSessionFactory.setMapperLocations(new PathMatchingResourcePatternResolver()
                .getResources(HbasePhoenixDataSourceConfig.MAPPER_LOCATION));
        sqlSessionFactory.setPlugins(new Interceptor[]{
                new PaginationInterceptor()
        });
        return sqlSessionFactory.getObject();
    }

//    @Bean(name = HBASEPHOENIX_SQL_SESSION_FACTORY)
//    public SqlSessionFactory hbasePhoenixSqlSessionFactory(@Qualifier("hbasePhoenixDataSource") DataSource hbasePhoenixDataSource)
//            throws Exception {
//        final SqlSessionFactoryBean sessionFactory = new SqlSessionFactoryBean();
//        sessionFactory.setDataSource(hbasePhoenixDataSource);
//        sessionFactory.setMapperLocations(new PathMatchingResourcePatternResolver()
//                .getResources(HbasePhoenixDataSourceConfig.MAPPER_LOCATION));
//        return sessionFactory.getObject();
//    }
}