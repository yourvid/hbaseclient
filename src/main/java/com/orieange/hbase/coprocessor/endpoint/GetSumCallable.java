package com.orieange.hbase.coprocessor.endpoint;

import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;

import java.io.IOException;

/**
 * endpoint客户端（回调方法）
 */
public class GetSumCallable implements Batch.Call<SumRows.SumRowService, Integer>{

    private SumRows.SumRequest request;

    public GetSumCallable(SumRows.SumRequest request) {
        super();
        this.request = request;
    }

    @Override
    public Integer call(SumRows.SumRowService service) throws IOException {
        CoprocessorRpcUtils.BlockingRpcCallback<SumRows.SumResponse> rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<SumRows.SumResponse>();
        service.getSum(null, request, rpcCallback);
        final SumRows.SumResponse response = rpcCallback.get();
        return (int) (response.hasCount() ? response.getCount() : 0);
    }
}
