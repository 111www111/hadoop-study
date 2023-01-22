package com.wyt.hdfs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 统计数据
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable intWritable = new IntWritable();

    /**
     * 统计阶段
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //统计变量
        int sum = 0;
        //入参形式 ： wyt，（1，1）
        for (IntWritable value : values) {
            sum += value.get();
        }
        //写出
        intWritable.set(sum);
        context.write(key, intWritable);
    }
}
