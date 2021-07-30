package com.lingnan.kv;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TowContactNum  implements WritableComparable<TowContactNum> {

    private String contactNum1;
    private String contactNum2;


    @Override
    public int compareTo(TowContactNum o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
