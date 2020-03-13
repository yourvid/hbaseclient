package com.orieange.hbase.config;

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
 * 自定义Observer
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
