package com.orieange.hbase.coprocessor.endpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * endpoint服务端
 */
@SuppressWarnings("all")
public class GetSumEndpoint extends SumRows.SumRowService implements RegionCoprocessor{

    private RegionCoprocessorEnvironment rce = null;

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {

    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        rce = (RegionCoprocessorEnvironment) env;
    }

    @Override
    public Iterable<Service> getServices() {
        return Collections.singleton(this);
    }

    @Override
    public void getSum(RpcController controller, SumRows.SumRequest request, RpcCallback<SumRows.SumResponse> done) {
        //获取列族
        byte[] family = Bytes.toBytes(request.getFamily());
        byte[] column = Bytes.toBytes(request.getColumn());
        int count = this.getCount(family,column);
        //获取response
        SumRows.SumResponse response = SumRows.SumResponse.newBuilder().setCount(count).build();
        done.run(response);
    }

    private int getCount(byte[] family, byte[] column){
        try {
            if (rce == null) {
                return 0;
            }
            int count = 0;
            byte[] currentRow = null;
            Scan scan = new Scan();
            scan.addColumn(family, column);
            RegionScanner scanner = rce.getRegion().getScanner(scan);
            List<Cell> cells = new ArrayList<Cell>();
            boolean hasMore;
            String value = null;
            do {
                hasMore = scanner.nextRaw(cells);
                for (Cell cell : cells) {
                    if (currentRow == null || !CellUtil.matchingRows(cell, currentRow)) {
                        currentRow = CellUtil.cloneRow(cell);
                        value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
                        count += Integer.parseInt(value);
                        break;
                    }
                }
                cells.clear();
            } while (hasMore);

            return count;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -99999;
    }

}

