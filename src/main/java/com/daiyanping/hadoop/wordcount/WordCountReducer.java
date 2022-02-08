package com.daiyanping.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN, Reducer 阶段输入的key的类型：Text
 * VALUEIN, Reducer 阶段输入value类型：IntWritable
 * KEYOUT, Reducer 阶段输出key类型：Text
 * VALUEOUT, Reducer 阶段输出value类型：IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    int sum;
    IntWritable v = new IntWritable();

    /**
     *     （1）用户自定义的Reducer要继承自己的父类
     *     （2）Reducer的输入数据类型对应Mapper的输出数据类型，也是KV
     *     （3）Reducer的业务逻辑写在reduce()方法中
     *     （4）ReduceTask进程对每一组相同k的<k,v>组调用一次reduce()方法
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,Context
            context) throws IOException, InterruptedException {

        // 1 累加求和
        sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }
        // 2 输出
        v.set(sum);
        context.write(key,v);
    }
}
