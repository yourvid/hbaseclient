package com.orieange.hbase.coprocessor.observer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
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
public class MyRegionObserver implements RegionObserver, RegionCoprocessor {
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        outInfo("MyRegionObserver.start()");
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        outInfo("MyRegionObserver.stop()");
    }

    /**
     * 加入该方法，否则无法生效
     */
    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        outInfo("MyRegionObserver.preOpen()");
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        outInfo("MyRegionObserver.postOpen()");
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        String rowkey = Bytes.toString(get.getRow());
        outInfo("MyRegionObserver.preGetOp() : rowkey = " + rowkey);
    }

    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        String rowkey = Bytes.toString(get.getRow());
        outInfo("MyRegionObserver.postGetOp() : rowkey = " + rowkey);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        String rowkey = Bytes.toString(put.getRow());
        outInfo("MyRegionObserver.prePut() : rowkey = " + rowkey);
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        String rowkey = Bytes.toString(put.getRow());
        outInfo("MyRegionObserver.postPut() : rowkey = " + rowkey);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        String rowkey = Bytes.toString(delete.getRow());
        outInfo("MyRegionObserver.preDelete() : rowkey = " + rowkey);
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        String rowkey = Bytes.toString(delete.getRow());
        outInfo("MyRegionObserver.postDelete() : rowkey = " + rowkey);
    }


    private void outInfo(String str) {
        try {
            FileWriter fw = new FileWriter("/opt/coprocessor.txt", true);
            fw.write(str + "\r\n");
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
