package com.orieange.hbase.config.hbase;

import com.orieange.hbase.config.hbase.HbaseConnectionFactory;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Collection;

@DependsOn("hbaseConfig")
@Component
public class HbaseFilterClient {

    @Autowired
    private HbaseConnectionFactory hbaseConnectionFactory;
    /**
     * 获得相等过滤器。相当于SQL的 [字段] = [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter eqFilter(String cf, String col, byte[] val) {
        SingleColumnValueFilter f = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareOperator.EQUAL, val);
        f.setLatestVersionOnly(true);
        f.setFilterIfMissing(true);
        return f;
    }

    /**
     * 获得大于过滤器。相当于SQL的 [字段] > [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter gtFilter(String cf, String col, byte[] val) {
        SingleColumnValueFilter f = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareOperator.GREATER, val);
        f.setLatestVersionOnly(true);
        f.setFilterIfMissing(true);
        return f;
    }

    /**
     * 获得大于等于过滤器。相当于SQL的 [字段] >= [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter gteqFilter(String cf, String col, byte[] val) {
        SingleColumnValueFilter f = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareOperator.GREATER_OR_EQUAL, val);
        f.setLatestVersionOnly(true);
        f.setFilterIfMissing(true);
        return f;
    }

    /**
     * 获得小于过滤器。相当于SQL的 [字段] < [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter ltFilter(String cf, String col, byte[] val) {
        SingleColumnValueFilter f = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareOperator.LESS, val);
        f.setLatestVersionOnly(true);
        f.setFilterIfMissing(true);
        return f;
    }

    /**
     * 获得小于等于过滤器。相当于SQL的 [字段] <= [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter lteqFilter(String cf, String col, byte[] val) {
        SingleColumnValueFilter f = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareOperator.LESS_OR_EQUAL, val);
        f.setLatestVersionOnly(true);
        f.setFilterIfMissing(true);
        return f;
    }

    /**
     * 获得不等于过滤器。相当于SQL的 [字段] != [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter neqFilter(String cf, String col, byte[] val) {
        SingleColumnValueFilter f = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareOperator.NOT_EQUAL, val);
        f.setLatestVersionOnly(true);
        f.setFilterIfMissing(true);
        return f;
    }

    /**
     * 和过滤器 相当于SQL的 的 and
     *
     * @param filters 多个过滤器
     * @return 过滤器
     */
    public static Filter andFilter(Filter... filters) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        if (filters != null && filters.length > 0) {
            if (filters.length > 1) {
                for (Filter f : filters) {
                    filterList.addFilter(f);
                }
            }
            if (filters.length == 1) {
                return filters[0];
            }
        }
        return filterList;
    }

    /**
     * 和过滤器 相当于SQL的 的 and
     *
     * @param filters 多个过滤器
     * @return 过滤器
     */
    public static Filter andFilter(Collection<Filter> filters) {
        return andFilter(filters.toArray(new Filter[0]));
    }


    /**
     * 或过滤器 相当于SQL的 or
     *
     * @param filters 多个过滤器
     * @return 过滤器
     */
    public static Filter orFilter(Filter... filters) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        if (filters != null && filters.length > 0) {
            for (Filter f : filters) {
                filterList.addFilter(f);
            }
        }
        return filterList;
    }

    /**
     * 或过滤器 相当于SQL的 or
     *
     * @param filters 多个过滤器
     * @return 过滤器
     */
    public static Filter orFilter(Collection<Filter> filters) {
        return orFilter(filters.toArray(new Filter[0]));
    }

    /**
     * 非空过滤器 相当于SQL的 is not null
     *
     * @param cf  列族
     * @param col 列
     * @return 过滤器
     */
    public static Filter notNullFilter(String cf, String col) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareOperator.NOT_EQUAL, new NullComparator());
        filter.setFilterIfMissing(true);
        filter.setLatestVersionOnly(true);
        return filter;
    }

    /**
     * 空过滤器 相当于SQL的 is null
     *
     * @param cf  列族
     * @param col 列
     * @return 过滤器
     */
    public static Filter nullFilter(String cf, String col) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareOperator.EQUAL, new NullComparator());
        filter.setFilterIfMissing(false);
        filter.setLatestVersionOnly(true);
        return filter;
    }

    /**
     * 子字符串过滤器 相当于SQL的 like '%[val]%'
     *
     * @param cf  列族
     * @param col 列
     * @param sub 子字符串
     * @return 过滤器
     */
    public static Filter subStringFilter(String cf, String col, String sub) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareOperator.EQUAL, new SubstringComparator(sub));
        filter.setFilterIfMissing(true);
        filter.setLatestVersionOnly(true);
        return filter;
    }

    /**
     * 正则过滤器 相当于SQL的 rlike '[regex]'
     *
     * @param cf    列族
     * @param col   列
     * @param regex 正则表达式
     * @return 过滤器
     */
    public static Filter regexFilter(String cf, String col, String regex) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareOperator.EQUAL, new RegexStringComparator(regex));
        filter.setFilterIfMissing(true);
        filter.setLatestVersionOnly(true);
        return filter;
    }




    /**
     * RowFilter过滤器
     * public RowFilter(CompareOperator op, ByteArrayComparable rowComparator)
     * @param tableName
     * @param filter
     * @throws IOException
     */
    public ResultScanner rowkeyFilter(TableName tableName,RowFilter filter) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        //str$ 末尾匹配，相当于sql中的 %str  ^str开头匹配，相当于sql中的str%
//        RowFilter filter = new RowFilter(CompareOperator.EQUAL,new RegexStringComparator("Key1$"));
        scan.setFilter(filter);
        ResultScanner scanner  = table.getScanner(scan);
//        for(Result rs:scanner) {
//            String rowkey = Bytes.toString(rs.getRow());
//            System.out.println("row key :"+rowkey);
//            Cell[] cells  = rs.rawCells();
//            for(Cell cell : cells) {
//                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+"::"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
//                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
//            }
//        }
        return scanner;
    }

    /**
     * 列值过滤器
     * @param tableName
     * @param filter
     * @return
     * @throws IOException
     */
    public ResultScanner singColumnFilter(TableName tableName,SingleColumnValueFilter filter) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        //下列参数分别为，列族，列名，比较符号，值
//        SingleColumnValueFilter filter =  new SingleColumnValueFilter( Bytes.toBytes("author"),  Bytes.toBytes("name"),
//                CompareOperator.EQUAL,  Bytes.toBytes("spark")) ;
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
//        for(Result rs:scanner) {
//            String rowkey = Bytes.toString(rs.getRow());
//            System.out.println("row key :"+rowkey);
//            Cell[] cells  = rs.rawCells();
//            for(Cell cell : cells) {
//                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+"::"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
//                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
//            }
//        }
        return scanner;
    }

    /**
     * 列名前缀过滤器
     * @param tableName
     * @param filter
     * @return
     * @throws IOException
     */
    public ResultScanner columnPrefixFilter(TableName tableName,ColumnPrefixFilter filter) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
//        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("name"));
        scan.setFilter(filter);
        ResultScanner scanner  = table.getScanner(scan);
//        for(Result rs:scanner) {
//            String rowkey = Bytes.toString(rs.getRow());
//            System.out.println("row key :"+rowkey);
//            Cell[] cells  = rs.rawCells();
//            for(Cell cell : cells) {
//                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+"::"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
//                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
//            }
//        }
        return scanner;
    }

    /**
     * 过滤器集合
     * @param tableName
     * @param list
     * @return
     * @throws IOException
     */
    public ResultScanner filterSet(TableName tableName, FilterList list) throws IOException {
        Connection connection = hbaseConnectionFactory.getConnection();
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
//        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//        SingleColumnValueFilter filter1 =  new SingleColumnValueFilter( Bytes.toBytes("author"),  Bytes.toBytes("name"),
//                CompareOperator.EQUAL,  Bytes.toBytes("spark")) ;
//        ColumnPrefixFilter filter2 = new ColumnPrefixFilter(Bytes.toBytes("name"));
//        list.addFilter(filter1);
//        list.addFilter(filter2);
        scan.setFilter(list);
        ResultScanner scanner  = table.getScanner(scan);
//        for(Result rs:scanner) {
//            String rowkey = Bytes.toString(rs.getRow());
//            System.out.println("row key :"+rowkey);
//            Cell[] cells  = rs.rawCells();
//            for(Cell cell : cells) {
//                System.out.println(Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength())+"::"+Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
//                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
//            }
//        }
        return scanner;

    }

}
