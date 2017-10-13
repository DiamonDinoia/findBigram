package it.cnr.isti.pad.mp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class FindBigrams {


    public static class Mymapper extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class Myreducer extends Reducer<Text,IntWritable, Text, IntWritable>{

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "findbigrams");

        job.setMapperClass(Mymapper.class);
        job.setReducerClass(Myreducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));



        System.exit(job.waitForCompletion(true) ? 0:1);
}
