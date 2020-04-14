package com.orieange.hbase;

import com.orieange.hbase.config.hbase.HBaseClient;
import com.orieange.hbase.config.hbase.HbaseConnectionFactory;
import com.orieange.hbase.coprocessor.endpoint.GetSumCallable;
import com.orieange.hbase.coprocessor.endpoint.SumRows;
import com.orieange.hbase.config.hbase.KeyGenerate;
import com.orieange.hbase.utils.TimeUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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
//    @Test
    public void createYlTable() throws Exception {
        int regionCount = 100;
        String tableName = "yl_data";
        String[] families = {"data"};
        DecimalFormat df = new DecimalFormat("00");
        String[] splits = new String[regionCount-1];
        for (int i = 1; i < regionCount ; i++) {
            String split = df.format(i);
            splits[i-1] = split;
        }
        hBaseClient.createTable(tableName, families, splits);
    }

    @Test
    public void createYlData() throws Exception {
        String tableName = "yl_data";
        String families = "data";
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        String yhdNo = "513432030019";
        String sNo1 = yhdNo+"YL01";
        String sNo2 = yhdNo+"YL02";
        String sNo3 = yhdNo+"YL03";
        String eNo1 = sNo1 + "01";
        String eNo2 = sNo1 + "02";
        String eNo3 = sNo1 + "03";
        try {
            long start = System.currentTimeMillis();
            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
            Date endDate = sdf1.parse("2020-04-05 00:00:00");
            List<Put> putList = new ArrayList<>();
            String[] columns = {"hno","sno","eno","time","v"};
            String[] vs = new String[5];
            for (int i = 0; i < 1000000 ; i++) {
                Date thisDate = TimeUtil.getAfterDay(endDate,-5*60*1000L*i);
                String rowKey = KeyGenerate.getDataKey(yhdNo,sNo1,thisDate);
                Put put = new Put(Bytes.toBytes(rowKey));
                vs[0] = yhdNo;
                vs[1] = sNo1;
                vs[2] = eNo1;
                vs[3] = thisDate.getTime()+"";
                vs[4] = "0.1";
                put.addColumn(Bytes.toBytes(families), Bytes.toBytes(columns[0]), Bytes.toBytes(vs[0]));
                put.addColumn(Bytes.toBytes(families), Bytes.toBytes(columns[1]), Bytes.toBytes(vs[1]));
                put.addColumn(Bytes.toBytes(families), Bytes.toBytes(columns[2]), Bytes.toBytes(vs[2]));
                put.addColumn(Bytes.toBytes(families), Bytes.toBytes(columns[3]), Bytes.toBytes(vs[3]));
                put.addColumn(Bytes.toBytes(families), Bytes.toBytes(columns[4]), Bytes.toBytes(vs[4]));
                putList.add(put);
            }
            table.put(putList);
            System.out.println("耗时："+(System.currentTimeMillis()-start)/1000+"s");

        } catch (Exception e) {
            e.printStackTrace();
        }  finally {
            table.close();
            connection.close();
        }

    }


    @Test
    public void rowkeyfilter() throws Exception {
        String tableName = "yl_data";
        String families = "data";
        String yhdNo = "513432030019";
        String sNo1 = yhdNo+"YL01";
        String sNo2 = yhdNo+"YL02";
        String sNo3 = yhdNo+"YL03";
        String eNo1 = sNo1 + "01";
        String eNo2 = sNo1 + "02";
        String eNo3 = sNo1 + "03";
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd");
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        Date date = sdf2.parse("2020-04-04");
        String rowKey = getDataKey1(yhdNo,sNo1,date);
        //str$ 末尾匹配，相当于sql中的 %str  ^str开头匹配，相当于sql中的str%
        RowFilter filter = new RowFilter(CompareOperator.EQUAL,new RegexStringComparator("^"+rowKey));
        scan.setFilter(filter);
        ResultScanner scanner  = table.getScanner(scan);
        for(Result rs:scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :"+rowkey);
            Cell[] cells  = rs.rawCells();
            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+"::"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
        }
    }

    private String getDataKey1(String hiddenNo,String equipNo, Date collectionTime){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        DecimalFormat df = new DecimalFormat("00");
        int hash = (equipNo + sdf.format(collectionTime)).hashCode();
        hash =(hash & Integer.MAX_VALUE) % 100;
        String regNo = df.format(hash);
        String key = regNo+collectionTime.getTime();
        return key;
    }


}
