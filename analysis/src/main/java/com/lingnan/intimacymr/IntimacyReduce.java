package com.lingnan.intimacymr;

import com.lingnan.kv.CountDurationValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntimacyReduce extends Reducer<Text,Text,Text, CountDurationValue> {

    private CountDurationValue sumAndDuration = new CountDurationValue();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int duration=0;
        for (Text value : values) {
            sum++;
            duration += Integer.valueOf(value.toString());
        }

        sumAndDuration.setCountSum(String.valueOf(sum));
        sumAndDuration.setDurationSum(String.valueOf(duration));
        context.write(key,sumAndDuration);
    }
}
