package com.lingnan.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommDimension extends BaseDimension{
//  所以为毛要new???因为要向下传递 所以要实例化？？
    private ContactDimension contactDimension = new ContactDimension();
    private DateDimension dateDimension = new DateDimension();



    @Override
    public int compareTo(BaseDimension o) {
        CommDimension other = (CommDimension) o;
        int result = this.contactDimension.compareTo(other.contactDimension);
        if(result == 0 ){
           result= this.dateDimension.compareTo(other.dateDimension);
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.contactDimension.write(dataOutput);
        this.dateDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
         this.contactDimension.readFields(dataInput);
         this.dateDimension.readFields(dataInput);
    }

    public CommDimension() {
    }

    public ContactDimension getContactDimension() {
        return contactDimension;
    }

    public void setContactDimension(ContactDimension contactDimension) {
        this.contactDimension = contactDimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

}
