package com.orieange.hbase.coprocessor;


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

