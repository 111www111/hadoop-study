package com.wyt.homework.day02;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class AnchorData implements WritableComparable<AnchorDataJob> {
    private Integer id;
    private Long heat;
    private Long peopleCount;
    private Long money;
    private Long peopleTimeCount;


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeLong(heat);
        out.writeLong(peopleCount);
        out.writeLong(money);
        out.writeLong(peopleTimeCount);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.heat = in.readLong();
        this.peopleCount = in.readLong();
        this.money = in.readLong();
        this.peopleTimeCount = in.readLong();
    }

    @Override
    public int compareTo(AnchorDataJob o) {
        return 0;
    }
}
