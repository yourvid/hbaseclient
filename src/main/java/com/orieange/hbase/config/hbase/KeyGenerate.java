package com.orieange.hbase.config.hbase;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 生成rowkey hashcode
 */
public class KeyGenerate {

    public static String getDataKey(String hiddenNo,String equipNo, Date collectionTime){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        DecimalFormat df = new DecimalFormat("00");
        int hash = (equipNo + sdf.format(collectionTime)).hashCode();
        hash =(hash & Integer.MAX_VALUE) % 100;
        String regNo = df.format(hash);
        String key = regNo+collectionTime.getTime()+hiddenNo+equipNo;
        return key;
    }
}
