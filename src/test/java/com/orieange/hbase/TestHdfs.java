package com.orieange.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;

/**
 *测试hdfs
 */
public class TestHdfs {

    @Test
    public void testRead() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://centos1/test/hello.txt") ;
        FSDataInputStream fis = fs.open(path);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copyBytes(fis,baos,1024);
        fis.close();
        System.out.println(new String(baos.toByteArray()));

        testWrite();
    }

    /**
     * 写入
     */
    @Test
    public void testWrite() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://s201/user/centos/hello.txt") ;
        FSDataOutputStream fout = fs.create(new Path("/user/centos/a.txt"));
        fout.write("how are you?".getBytes());
        fout.close();
    }

    /**
     * 定制副本数和blocksize
     */
    @Test
    public void testWrite2() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream fout = fs.create(new Path("/user/centos/a.txt"),
                true, 1024, (short) 2,
                1024);

        FileInputStream fis = new FileInputStream("d:/a.txt");
        IOUtils.copyBytes(fis,fout,1024);
        fout.close();
        fis.close();
    }

    @Test
    public void generateSplitFile() throws IOException {
        int regionCount = 100;
        DecimalFormat df = new DecimalFormat("00");
        FileWriter fw = new FileWriter("d:\\splitkeys.txt",false);
        for (int i = 0; i < regionCount-1; i++) {
            String split = df.format(i + 1);
            fw.write(split+"\r\n");
        }
        fw.flush();
        fw.close();
    }
}
