package com.lingnan.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//时间
public class DateDimension extends BaseDimension{

    private String year;
    private String month;
    private String day;

    public DateDimension(String year, String month, String day) {
        this.year = year;
        this.month = month;
        this.day = day;
    }

    @Override
    public int compareTo(BaseDimension o) {
        DateDimension other = (DateDimension) o;

        int result = this.year.compareTo(other.year);
        if(result == 0){
            result = this.month.compareTo(other.month);
        }
        if(result == 0){
            result = this.day.compareTo(other.day);
        }

        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(month);
        dataOutput.writeUTF(day);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readUTF();
        this.month = dataInput.readUTF();
        this.day = dataInput.readUTF();
    }

    public DateDimension() {
        this.year="";
        this.month="";
        this.day="";
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    @Override
    public String toString() {
        return year+"\t"+month+"\t"+day;
    }
}
