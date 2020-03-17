package com.orieange.hbase.config;

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
        Admin admin = hbaseConnectionFactory.getAdmin();
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
        Admin admin = hbaseConnectionFactory.getAdmin();
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

    public String selectOneRow(String tableName, String rowKey) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Result result = table.get(get);
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        for (Cell cell : result.rawCells()) {
            String row = Bytes.toString(cell.getRowArray());
            String columnFamily = Bytes.toString(cell.getFamilyArray());
            String column = Bytes.toString(cell.getQualifierArray());
            String value = Bytes.toString(cell.getValueArray());

            // 可以通过反射封装成对象(列名和Java属性保持一致)
            System.out.println(row);
            System.out.println(columnFamily);
            System.out.println(column);
            System.out.println(value);
        }

        return null;
    }

    public String scanTable(String tableName, String rowKeyFilter) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        if (!StringUtils.isEmpty(rowKeyFilter)) {
            RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(rowKeyFilter));
            scan.setFilter(rowFilter);
        }
        ResultScanner scanner = table.getScanner(scan);

        try {
            for (Result result : scanner) {
                System.out.println(Bytes.toString(result.getRow()));
                for (Cell cell : result.rawCells()) {
                    System.out.println(cell);
                }
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }

        return null;
    }


    /**
     * 判断表是否已经存在，这里使用间接的方式来实现
     *
     * admin.tableExists() 会报NoSuchColumnFamilyException， 有人说是hbase-client版本问题
     * @param tableName
     * @return
     * @throws IOException
     */
    public boolean tableExists(String tableName) throws IOException {
        Admin admin = hbaseConnectionFactory.getAdmin();
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
    public void scanTable(TableName tableName) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner rsacn = table.getScanner(scan);
        for(Result rs:rsacn) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :"+rowkey);
            Cell[] cells  = rs.rawCells();
            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+"::"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }


    /**
     * rowKey过滤器
     * LESS <  LESS_OR_EQUAL <=  EQUAL =   NOT_EQUAL <>  GREATER_OR_EQUAL >=   GREATER >   NO_OP 排除所有
     * @param tableName
     * @throws IOException
     */
    public void rowkeyFilter(TableName tableName) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        //str$ 末尾匹配，相当于sql中的 %str  ^str开头匹配，相当于sql中的str%
        RowFilter filter = new RowFilter(CompareOperator.EQUAL,new RegexStringComparator("Key1$"));
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
            System.out.println("-----------------------------------------");
        }
    }

    /**
     * 列值过滤器
     * @throws IOException
     */
    public void singColumnFilter(TableName tableName) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        //下列参数分别为，列族，列名，比较符号，值
        SingleColumnValueFilter filter =  new SingleColumnValueFilter( Bytes.toBytes("author"),  Bytes.toBytes("name"),
                CompareOperator.EQUAL,  Bytes.toBytes("spark")) ;
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for(Result rs:scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :"+rowkey);
            Cell[] cells  = rs.rawCells();
            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+"::"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    /**
     * 列名前缀过滤器
     * @throws IOException
     */
    public void columnPrefixFilter(TableName tableName) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("name"));
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
            System.out.println("-----------------------------------------");
        }
    }

    /**
     * 过滤器集合
     * @throws IOException
     */
    public void filterSet(TableName tableName) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter1 =  new SingleColumnValueFilter( Bytes.toBytes("author"),  Bytes.toBytes("name"),
                CompareOperator.EQUAL,  Bytes.toBytes("spark")) ;
        ColumnPrefixFilter filter2 = new ColumnPrefixFilter(Bytes.toBytes("name"));
        list.addFilter(filter1);
        list.addFilter(filter2);

        scan.setFilter(list);
        ResultScanner scanner  = table.getScanner(scan);
        for(Result rs:scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :"+rowkey);
            Cell[] cells  = rs.rawCells();
            for(Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+"::"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }

    }



}
