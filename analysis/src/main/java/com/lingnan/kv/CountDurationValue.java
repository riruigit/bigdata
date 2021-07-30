package com.lingnan.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountDurationValue extends BaseValue{

    private String countSum;
    private String durationSum;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(countSum);
        dataOutput.writeUTF(durationSum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.countSum = dataInput.readUTF();
        this.durationSum = dataInput.readUTF();
    }

    public CountDurationValue() {
        this.countSum="";
        this.durationSum="";
    }

    public String getCountSum() {
        return countSum;
    }

    public void setCountSum(String countSum) {
        this.countSum = countSum;
    }

    public String getDurationSum() {
        return durationSum;
    }

    public void setDurationSum(String durationSum) {
        this.durationSum = durationSum;
    }

    @Override
    public String toString() {
        return countSum + '\t' + durationSum;
    }
}
