package com.lingnan.mr;

import com.lingnan.kv.CommDimension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class CountDurationDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//      获取配置和Job对象
        Configuration configuration = HBaseConfiguration.create();
        Job job = Job.getInstance(configuration);
//       设置jar
        job.setJarByClass(CountDurationDriver.class);

        Scan scan = new Scan();
//      设置Mapper
        TableMapReduceUtil.initTableMapperJob("call:calllog",scan,CountDurationMapper.class,
                CommDimension.class, Text.class,job);

//        设置Reduce
        job.setReducerClass(CountDurationReduce.class);

//        设置输出类型和自定义的OutputFormat
        job.setOutputFormatClass(MySQLOutPutFormat.class);

//        提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
