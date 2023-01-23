package com.wyt.serialize;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 处理逻辑
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    /**
     * 输出
     */
    private FlowBean outValue = new FlowBean();

    /**
     * 处理逻辑
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        //1.遍历集合累加
        long totalUp = 0L;
        long totalDown = 0L;
        for (FlowBean value : values) {
            totalUp += value.getUpFlow();
            totalDown += value.getDownFlow();
        }
        //2.封装
        outValue.setUpFlow(totalUp);
        outValue.setDownFlow(totalDown);
        outValue.setSumFlow();
        //3.写出
        context.write(key,outValue);
    }
}
