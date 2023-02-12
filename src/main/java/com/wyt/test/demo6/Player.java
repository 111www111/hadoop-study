package com.wyt.test.demo6;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 观看人数+热度
 */
@Data
public class Player implements WritableComparable<Player> {
    //人数
    private int peopleNum;
    //时长
    private int videoTime;

    @Override
    public int compareTo(Player o) {
        //1.先按照观众人数降序排序 2.如果观众人数相同，则按照直播时长排序
        if(Objects.equals(o.videoTime,this.videoTime)){
            return this.videoTime-o.videoTime;
        }
        return o.peopleNum-this.peopleNum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(peopleNum);
        out.writeInt(videoTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.peopleNum = in.readInt();
        this.videoTime = in.readInt();
    }
}
