package com.orieange.hbase.config;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class HbaseConnectionFactory {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private HbaseConfig config;
    // 创建一个定长线程池，可控制线程最大并发数，超出的线程会在队列中等待。
    private static ExecutorService executor = null;
    private static Connection connection = null;

    @PostConstruct
    private void init() {
        if (connection != null) {
            return;
        }
        try {
            executor = Executors.newFixedThreadPool(3);
            connection = ConnectionFactory.createConnection(config.configuration(),executor);
        } catch (IOException e) {
            logger.error("HBase create connection failed: {}", e);
        }
    }

    public Connection getConnection(){
        return connection;
    }

}
