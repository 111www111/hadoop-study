package com.wyt.hdfs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
// hadoop2.x以上引用这个mapreduce的Mapper
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

/**
 * 处理数据
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //创建一个属性转换类String -> text
    private Text outKey = new Text();
    //创建一个属性转换类int-> IntWritable;
    private IntWritable one = new IntWritable(1);

    /**
     * 重写map方法
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //获取文件中的一行数据
        String line = value.toString();
        //切割这一行数据，文件中为空格,这部分就是单词
        String[] words = line.split(" ");
        //循环写
        for (String word : words) {
            outKey.set(word);
            //存放处理后的数据
            context.write(outKey, one);
        }

    }
}
