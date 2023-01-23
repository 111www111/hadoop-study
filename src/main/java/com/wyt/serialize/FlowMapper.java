package com.wyt.serialize;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 处理数据
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text outKey = new Text();
    private FlowBean outValue = new FlowBean();

    /**
     * 业务逻辑
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        //1.获取一行信息,这样的数据： 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        String line = value.toString();
        //2.切割数据
        String[] datas = line.split("\t");
        //3.抓取数据，我们只要手机号，上行，下行流量
        String phone = datas[1];
        String up = datas[datas.length-3];
        String down = datas[datas.length-2];
        //4.封装
        outKey.set(phone);
        outValue.setUpFlow(Long.parseLong(up));
        outValue.setDownFlow(Long.parseLong(down));
        outValue.setSumFlow();
        //5.写出
        context.write(outKey,outValue);
    }
}
