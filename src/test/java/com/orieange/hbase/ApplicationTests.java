package com.orieange.hbase;

import com.orieange.hbase.config.HBaseClient;
import com.orieange.hbase.config.HbaseConnectionFactory;
import com.orieange.hbase.coprocessor.endpoint.GetSumCallable;
import com.orieange.hbase.coprocessor.endpoint.SumRows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Set;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {Application.class})
public class ApplicationTests {
    @Autowired
    private HBaseClient hBaseClient;
    @Autowired
    private HbaseConnectionFactory hbaseConnectionFactory;

    /**
     * 测试删除、创建表
     */
//    @Test
    public void testGetValue() {
        String value = hBaseClient.getValue("tbl_abc", "mengday", "info", "age");
        System.out.println(value);
    }

//    @Test
    public void testCreateTable() throws IOException {
        String tableName = "tbl_abc";
        hBaseClient.deleteTable(tableName);
        hBaseClient.createTable(tableName, new String[]{"cf1", "cf2"});
    }

//    @Test
    public void dropTable() throws IOException {
        hBaseClient.deleteTable("tbl_abc");
    }


//    @Test
    public void testInsertOrUpdate() throws IOException {
        hBaseClient.insertOrUpdate("tbl_abc", "rowKey1", "cf1", new String[]{"c1", "c2"}, new String[]{"v1", "v22"});
    }

//    @Test
    public void testSelectOneRow() throws IOException {
        hBaseClient.selectOneRow("tbl_abc", "rowKey1");
    }

//    @Test
    public void testScanTable() throws IOException {
        hBaseClient.scanTable("tbl_abc", "rowKey1");
    }

//    @Test
    public void tableExists() throws IOException {
        Boolean result = hBaseClient.tableExists("test1");
        System.out.println(result);
    }

    /**
     * 协处理器测试
     *
     * @throws Throwable
     */
//    @Test
    public void testCoprocessor() throws Throwable {
        Connection connection = hbaseConnectionFactory.getConnection();
        try (final Table table = connection.getTable(TableName.valueOf("account"))) {
            SumRows.SumRequest request = SumRows.SumRequest.newBuilder().setFamily("info").setColumn("order").build();
            final Map<byte[], Integer> longMap = table.coprocessorService(SumRows.SumRowService.class, null, null, new GetSumCallable(request));
            long totalRows = 0;
            final Set<Map.Entry<byte[], Integer>> entries = longMap.entrySet();
            for (Map.Entry<byte[], Integer> entry : entries) {
                totalRows += entry.getValue();
            }
            System.out.println("总和:" + totalRows);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    /**
     * 命令：create 'yl_data','data',SPLITS_FILE=>'/usr/splitkeys.txt'
     * @throws Exception
     */
    @Test
    public void createBaseTable() throws Exception {
        int regionCount = 100;
        String tableName = "yl_data";
        String[] families = {"data"};
        DecimalFormat df = new DecimalFormat("00");
        String[] splits = new String[regionCount];
        for (int i = 0; i < regionCount ; i++) {
            String split = df.format(i);
            splits[i] = split;
        }
        hBaseClient.createTable(tableName, families, splits);
    }


}
