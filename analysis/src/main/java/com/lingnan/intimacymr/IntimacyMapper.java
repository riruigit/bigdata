package com.lingnan.intimacymr;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class IntimacyMapper extends TableMapper<Text,Text> {

    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 05_19826588765_2020-06-16 23:01:20_17560232813_1_0534

        String rowKey = Bytes.toString(value.getRow());
        String[] rokSplits = rowKey.split("_");
        if("0".equals(rokSplits[4])){
            return;
        }
        String phone1 = rokSplits[1];
        String phone2 = rokSplits[3];
        String towContact = phone1+"_"+phone2;
        String duration = rokSplits[5];

        k.set(towContact);
        v.set(duration);

        //输出类似于19826588765_17560232813  0534
        context.write(k,v);



    }
}
