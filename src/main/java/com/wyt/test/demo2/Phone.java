package com.wyt.test.demo2;


import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1363157985066	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	24	27	2481	24681	200
 * 1363157995052	13826544101	5C-0E-8B-C7-F1-E0:CMCC	120.197.40.4	4	0	264	0	200
 * 1363157991076	13926435656	20-10-7A-28-CC-0A:CMCC	120.196.100.99	2	4	132	1512	200
 * 1363154400022	13926251106	5C-0E-8B-8B-B1-50:CMCC	120.197.40.4	4	0	240	0	200
 * 1363157985066	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	24	27	2481	24681	200
 * 1363157995052	13826544101	5C-0E-8B-C7-F1-E0:CMCC	120.197.40.4	4	0	264	0	200
 * 1363157991076	13926435656	20-10-7A-28-CC-0A:CMCC	120.196.100.99	2	4	132	1512	200
 * 1363154400022	13926251106	5C-0E-8B-8B-B1-50:CMCC	120.197.40.4	4	0	240	0	200
 */
@Data
public class Phone implements Writable {
    //上传流量
    private Integer up;
    //下载流量
    private Integer down;

    /**
     * 构造
     */
    public Phone() {
    }

    public Phone(Integer up, Integer down) {
        this.up = up;
        this.down = down;
    }

    /**
     * 重写序列化接口
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(up);
        out.writeInt(down);
    }

    /**
     * 重写反序列化接口
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        up = in.readInt();
        down = in.readInt();
    }
}
