package com.lingnan.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//联系人
public class ContactDimension extends BaseDimension{

    private String name;
    private String phoneNum;

    public ContactDimension(String name, String phoneNum) {
        this.name = name;
        this.phoneNum = phoneNum;
    }

    @Override
    public int compareTo(BaseDimension o) {
        ContactDimension other = (ContactDimension) o;

        return phoneNum.compareTo(other.phoneNum);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(phoneNum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.phoneNum = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return name + '\t' + phoneNum;
    }

    public ContactDimension() {
        this.name="";
        this.phoneNum="";
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }
}
