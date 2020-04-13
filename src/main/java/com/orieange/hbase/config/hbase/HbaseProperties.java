package com.orieange.hbase.config.hbase;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@ConfigurationProperties(prefix = "hbase")
public class HbaseProperties {
    private Map<String, String> config;
    private Map<String, String> phoenix;

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public Map<String, String> getPhoenix() {
        return phoenix;
    }

    public void setPhoenix(Map<String, String> phoenix) {
        this.phoenix = phoenix;
    }
}
