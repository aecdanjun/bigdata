package br.edu.ifrn.tads.bigdata.hadooptest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinMaxTemp {
    public static final Text TEMP = new Text("temp");

    public static class MinMaxWritable implements Writable {

        private int min;
        private int max;

        public MinMaxWritable() {
            this.min = 0;
            this.max = 0;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.min = in.readInt();
            this.max = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.min);
            out.writeInt(this.max);
        }

        public int getMin() {
            return this.min;
        }

        public void setMin(int min) {
            this.min = min;
        }

        public int getMax() {
            return this.max;
        }

        public void setMax(int max) {
            this.max = max;
        }

        @Override
        public String toString() {
            return String.format("min:%d, max: %d", this.min, this.max);
        }

    }

    public static class MinMaxTempMapper extends Mapper<LongWritable, Text, Text, MinMaxWritable> {


        public final int MAX_COLUMN = 1;
        public final int MIN_COLUMN = 3;

        private static int count = 0;

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            
            if (count > 0) { // ignore first line
                MinMaxWritable rec = new MinMaxWritable();

                rec.setMax(Integer.parseInt(columns[MAX_COLUMN]));
                rec.setMin(Integer.parseInt(columns[MIN_COLUMN]));


                context.write(MinMaxTemp.TEMP, rec);
            }

            count++;
        }

    }

    public static class MinMaxTempReducer extends Reducer<Text, MinMaxWritable, Text, MinMaxWritable> {

        protected void reduce(Text key, Iterable<MinMaxWritable> values, Context context) throws IOException, InterruptedException {
            int min = -9999;
            int max = 0;
            MinMaxWritable result = new MinMaxWritable();

            for(MinMaxWritable v: values) {
                if (v.getMax() > max) {
                    max = v.getMax();
                }

                if (min == -9999) {
                    min = v.getMin();
                } else if (v.getMin() < min) {
                    min = v.getMin();
                }
            }

            result.setMin(min);
            result.setMax(max);

            context.write(key, result);
        }

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Min Max Temperature");
        job.setJarByClass(MinMaxTemp.class);
        job.setMapperClass(MinMaxTempMapper.class);
        job.setReducerClass(MinMaxTempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
