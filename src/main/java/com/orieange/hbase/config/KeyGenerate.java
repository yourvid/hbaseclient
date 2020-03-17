package com.orieange.hbase.config;

import java.text.DecimalFormat;

/**
 * 生成rowkey hashcode
 */
public class KeyGenerate {

    public static String getRegNo(String callerId , String callTime){
        //区域00-99
        int hash = (callerId + callTime.substring(0, 6)).hashCode();
        hash =(hash & Integer.MAX_VALUE) % 100;

        //hash区域号
        DecimalFormat df = new DecimalFormat();
        df.applyPattern("00");
        String regNo = df.format(hash);
        return regNo ;
    }
}
