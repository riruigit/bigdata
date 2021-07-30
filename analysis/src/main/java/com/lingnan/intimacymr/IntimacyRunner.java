package com.lingnan.intimacymr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class IntimacyRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(IntimacyRunner.class);

        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob("call:calllog",scan,IntimacyMapper.class, Text.class,Text.class,job);

        job.setReducerClass(IntimacyReduce.class);

        job.setOutputFormatClass(IntimacySqlOutPutFormat.class);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
