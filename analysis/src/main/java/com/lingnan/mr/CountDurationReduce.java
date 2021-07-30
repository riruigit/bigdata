package com.lingnan.mr;

import com.lingnan.kv.CommDimension;
import com.lingnan.kv.CountDurationValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountDurationReduce extends Reducer<CommDimension, Text,CommDimension, CountDurationValue> {
    private CountDurationValue countDurationValue = new CountDurationValue();
    @Override
    protected void reduce(CommDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int countSum = 0;
        int durationSum = 0;
        for (Text value : values) {
            countSum++;
            durationSum+=Integer.valueOf(value.toString());
        }


        countDurationValue.setDurationSum(String.valueOf(durationSum));
        countDurationValue.setCountSum(String.valueOf(countSum));

        context.write(key,countDurationValue);
    }
}
