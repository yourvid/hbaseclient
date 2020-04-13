package com.orieange.hbase.utils;

import com.orieange.hbase.config.hbase.HbaseConnectionFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

@DependsOn("hbaseConfig")
@Component
public class HBaseClient {
    @Autowired
    private HbaseConnectionFactory hbaseConnectionFactory;

    public void createTable(String tableName, String[] columnFamilies) throws IOException {
        Admin admin = hbaseConnectionFactory.getConnection().getAdmin();
        TableName name = TableName.valueOf(tableName);

        boolean isExists = this.tableExists(tableName);
        if (isExists) {
            throw new TableExistsException(tableName + "is exists!");
        }

        TableDescriptorBuilder descriptorBuilder = TableDescriptorBuilder.newBuilder(name);
        List<ColumnFamilyDescriptor> columnFamilyList = new ArrayList<>();
        for (String columnFamily : columnFamilies) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes()).build();
            columnFamilyList.add(columnFamilyDescriptor);
        }
        descriptorBuilder.setColumnFamilies(columnFamilyList);
        TableDescriptor tableDescriptor = descriptorBuilder.build();
        admin.createTable(tableDescriptor);
    }

    public void createTable(String tableName, String[] columnFamilies ,String[] splitKeys) throws IOException {
        Admin admin = hbaseConnectionFactory.getConnection().getAdmin();
        TableName name = TableName.valueOf(tableName);

        boolean isExists = this.tableExists(tableName);
        if (isExists) {
            throw new TableExistsException(tableName + "is exists!");
        }

        TableDescriptorBuilder descriptorBuilder = TableDescriptorBuilder.newBuilder(name);
        List<ColumnFamilyDescriptor> columnFamilyList = new ArrayList<>();
        for (String columnFamily : columnFamilies) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes()).build();
            columnFamilyList.add(columnFamilyDescriptor);
        }
        descriptorBuilder.setColumnFamilies(columnFamilyList);
        TableDescriptor tableDescriptor = descriptorBuilder.build();

        byte[][] splitKeysBytes = new byte[splitKeys.length][];
        int i = 0;
        for(String split : splitKeys){
            if (!StringUtils.isBlank(split)) {
                byte[] bytes = Bytes.toBytes(split);
                splitKeysBytes[i ++] = bytes;
            }
        }
        admin.createTable(tableDescriptor,splitKeysBytes);
    }

    public void insertOrUpdate(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        this.insertOrUpdate(tableName, rowKey, columnFamily, new String[]{column}, new String[]{value});
    }

    public void insertOrUpdate(String tableName, String rowKey, String columnFamily, String[] columns, String[] values) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        for (int i = 0; i < columns.length; i++) {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
            table.put(put);
        }
    }

    public void deleteRow(String tableName, String rowKey) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        table.delete(delete);
    }

    public void deleteColumnFamily(String tableName, String rowKey, String columnFamily) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        delete.addFamily(Bytes.toBytes(columnFamily));
        table.delete(delete);
    }

    public void deleteColumn(String tableName, String rowKey, String columnFamily, String column) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        table.delete(delete);
    }

    public void deleteTable(String tableName) throws IOException {
        Admin admin = hbaseConnectionFactory.getConnection().getAdmin();
        boolean isExists = this.tableExists(tableName);
        if (!isExists) {
            return;
        }

        TableName name = TableName.valueOf(tableName);
        admin.disableTable(name);
        admin.deleteTable(name);
    }

    public String getValue(String tableName, String rowkey, String family, String column) {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = null;

        String value = "";
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(family)
                || StringUtils.isBlank(rowkey) || StringUtils.isBlank(column)) {
            return null;
        }
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get g = new Get(rowkey.getBytes());
            g.addColumn(family.getBytes(), column.getBytes());
            Result result = table.get(g);
            List<Cell> ceList = result.listCells();
            if (ceList != null && ceList.size() > 0) {
                for (Cell cell : ceList) {
                    value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                table.close();
                connection.close();
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
        return value;
    }

    public Result selectOneRow(String tableName, String rowKey) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Result result = table.get(get);
//        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
//        for (Cell cell : result.rawCells()) {
//            String row = Bytes.toString(cell.getRowArray());
//            String columnFamily = Bytes.toString(cell.getFamilyArray());
//            String column = Bytes.toString(cell.getQualifierArray());
//            String value = Bytes.toString(cell.getValueArray());
//
//            // 可以通过反射封装成对象(列名和Java属性保持一致)
//            System.out.println(row);
//            System.out.println(columnFamily);
//            System.out.println(column);
//            System.out.println(value);
//        }
        return result;
    }

    public ResultScanner scanTable(String tableName, String rowKeyFilter) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        if (!StringUtils.isEmpty(rowKeyFilter)) {
            RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(rowKeyFilter));
            scan.setFilter(rowFilter);
        }
        ResultScanner rsacn = table.getScanner(scan);
//        for(Result rs:rsacn) {
//            String rowkey = Bytes.toString(rs.getRow());
//            System.out.println("row key :"+rowkey);
//            Cell[] cells  = rs.rawCells();
//            for(Cell cell : cells) {
//                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+"::"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
//                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
//            }
//        }
        return rsacn;
    }


    /**
     * 判断表是否已经存在，这里使用间接的方式来实现
     * @param tableName
     * @return
     * @throws IOException
     */
    public boolean tableExists(String tableName) throws IOException {
        Admin admin = hbaseConnectionFactory.getConnection().getAdmin();
        TableName[] tableNames = admin.listTableNames();
        if (tableNames != null && tableNames.length > 0) {
            for (int i = 0; i < tableNames.length; i++) {
                if (tableName.equals(tableNames[i].getNameAsString())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 全表扫描
     * @param tableName
     * @throws IOException
     */
    public ResultScanner scanTable(TableName tableName) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner rsacn = table.getScanner(scan);
//        for(Result rs:rsacn) {
//            String rowkey = Bytes.toString(rs.getRow());
//            System.out.println("row key :"+rowkey);
//            Cell[] cells  = rs.rawCells();
//            for(Cell cell : cells) {
//                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+"::"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
//                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
//            }
//        }
        return rsacn;
    }

}
