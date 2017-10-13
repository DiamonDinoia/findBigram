package it.cnr.isti.pad.mp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static sun.swing.MenuItemLayoutHelper.max;

/**
 * Hello world!
 *
 */
public class FindBigrams {

    public static class Mymapper extends Mapper<Object, Text, Text, IntWritable> {

        private static IntWritable ONE = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (int i = 0; i < words.length-1; i++) {
                value.set(words[i]+ " " +words[i+1]);
                context.write(value,ONE);
            }

        }
    }

    public static class Myreducer extends Reducer<Text, IntWritable, LongWritable, Text> {


        private static Text string;
        private static LongWritable max = new LongWritable(-1);

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long tmp = 0;
            for (IntWritable value : values) {
                tmp+=1;
            }
            if (tmp>max.get()){
                max.set(tmp);
                string = key;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(max,string);
            super.cleanup(context);
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "findbigrams");

        job.setMapperClass(Mymapper.class);
        job.setReducerClass(Myreducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
