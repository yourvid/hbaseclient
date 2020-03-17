package com.orieange.hbase.coprocessor.observer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * observer协处理器
 *
 * 这一类协处理器与触发器(trigger)类似：回调函数（也被称作钩子函数，hook）在一些特定事件发生时被执行。这些事件包括一些用户产生的事件，也包括服务器端内部自动产生的事件。
 *
 * 协处理器框架提供的接口如下
 *
 * RegionObserver：用户可以用这种的处理器处理数据修改事件，它们与表的region联系紧密。
 *
 * MasterObserver：可以被用作管理或DDL类型的操作，这些是集群级事件。
 *
 * WALObserver：提供控制WAL的钩子函数
 *
 * Observer提供了一些设计好的回调函数，每个操作在集群服务器端都可以被调用。
 */
public class ReverseInfoObserver implements RegionObserver, RegionCoprocessor {

    private static final Logger logger = LoggerFactory.getLogger(ReverseInfoObserver.class);

    private static Configuration conf = null;
    private static Connection connection = null;
    private RegionCoprocessorEnvironment env = null;

    static{
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "centos1,centos2,centos3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        this.env = (RegionCoprocessorEnvironment) e;
    }


    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {

    }

    /**
     * 加入该方法，否则无法生效
     */
    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }


    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c,Put put, WALEdit edit, Durability durability) throws IOException {
        logger.info("run ReverseInfoObserver............prePut...........................");
        try {
            System.out.println("---------------------------------------------------------------------");
            byte[] user = put.getRow();
            Cell cell = put.get(Bytes.toBytes("info"), Bytes.toBytes("order")).get(0);
            Put o_put = new Put(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
            o_put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("user"), user);
            Table order = connection.getTable(TableName.valueOf("order"));
            order.put(o_put);
            order.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw e;
        }
    }


}

