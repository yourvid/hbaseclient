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

@Component
public class HbaseConnectionFactory {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private HbaseConfig config;
    private static Connection connection;
    private static Admin admin;

    @PostConstruct
    private void init() {
        if (connection != null) {
            return;
        }
        try {
            connection = ConnectionFactory.createConnection(config.configuration());
            admin = connection.getAdmin();
        } catch (IOException e) {
            logger.error("HBase create connection failed: {}", e);
        }
    }

    public Connection getConnection(){
        return connection;
    }

    public Admin getAdmin(){
        return admin;
    }
}
