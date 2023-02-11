package com.wyt.test.demo5;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 降序
 */
public class DescLongWritable implements WritableComparable<DescLongWritable> {

    private long value;

    public DescLongWritable() {
    }

    public long get() {
        return value;
    }

    public void set(long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value+"";
    }

    public DescLongWritable(long value) {
        this.value = value;
    }

    @Override
    public int compareTo(DescLongWritable o) {
        long thisValue = this.value;
        long thatValue = o.value;
        if(thisValue>thatValue){
            return -1;
        }else if (thisValue == thatValue){
            return 0;
        }else{
            return 1;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.value = in.readLong();
    }
}
