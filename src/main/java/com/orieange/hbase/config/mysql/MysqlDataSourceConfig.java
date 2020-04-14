package com.orieange.hbase.config.mysql;

import com.alibaba.druid.pool.DruidDataSource;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.MybatisXMLLanguageDriver;
import com.baomidou.mybatisplus.extension.plugins.PaginationInterceptor;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.orieange.hbase.config.phoenix.HbasePhoenixDataSourceConfig;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.type.JdbcType;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

@Configuration
@MapperScan(basePackages = MysqlDataSourceConfig.PACKAGE, sqlSessionTemplateRef = MysqlDataSourceConfig.SQL_SESSION_TEMPLATE)
public class MysqlDataSourceConfig {
    static final String SESSION_FACTORY = "mysqlSqlSessionFactory";
    static final String TRANSACTION_MANAGER = "mysqlTransactionManager";
    static final String DATA_SOURCE = "mysqlDataSource";
    static final String SQL_SESSION_TEMPLATE = "mysqlSqlSessionTemplate";
    static final String PACKAGE = "com.orieange.hbase.dao.mysql";
    static final String MAPPER_LOCATION = "classpath*:/mapper/mysql/*.xml";

    @Bean(name = DATA_SOURCE)
    @Primary   //配置默认数据源
    @ConfigurationProperties(prefix = "spring.datasource.druid.mysql")
    public DataSource dataSource() {
        return new DruidDataSource();
    }

//    @Bean(name = SESSION_FACTORY)
//    public SqlSessionFactory sqlSessionFactory(@Qualifier(DATA_SOURCE) DataSource dataSource) throws Exception {
//        SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
//        bean.setDataSource(dataSource);
//        bean.setMapperLocations(new PathMatchingResourcePatternResolver().getResources(MAPPER_LOCATION));
//        return bean.getObject();
//    }

    @Bean(name = SESSION_FACTORY)
    public SqlSessionFactory sqlSessionFactory(@Qualifier(DATA_SOURCE) DataSource dataSource) throws Exception {
        MybatisSqlSessionFactoryBean sqlSessionFactory = new MybatisSqlSessionFactoryBean();
        sqlSessionFactory.setDataSource(dataSource);
        MybatisConfiguration configuration = new MybatisConfiguration();
        configuration.setDefaultScriptingLanguage(MybatisXMLLanguageDriver.class);
        configuration.setJdbcTypeForNull(JdbcType.NULL);
        sqlSessionFactory.setConfiguration(configuration);
        sqlSessionFactory.setMapperLocations(new PathMatchingResourcePatternResolver()
                .getResources(MAPPER_LOCATION));
        sqlSessionFactory.setPlugins(new Interceptor[]{
                new PaginationInterceptor()
        });
        return sqlSessionFactory.getObject();
    }


    @Bean(name = TRANSACTION_MANAGER)
    public DataSourceTransactionManager transactionManager(@Qualifier(DATA_SOURCE) DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean(name = SQL_SESSION_TEMPLATE)
    public SqlSessionTemplate sqlSessionTemplate(@Qualifier(SESSION_FACTORY) SqlSessionFactory sqlSessionFactory)
            throws Exception {
        return new SqlSessionTemplate(sqlSessionFactory);
    }

}
