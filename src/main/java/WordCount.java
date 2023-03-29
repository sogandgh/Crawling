import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
    {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text docID = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] value_split = value.toString().split("\t", 2);
            docID.set(value_split[0]);
            String text = value_split[1];

            text = text.toLowerCase();
            text = text.replaceAll("[^a-zA-Z]+", " ");

            StringTokenizer itr = new StringTokenizer(text);
            while (itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                context.write(word, docID);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text>
    { private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> wordCountMap = new HashMap<>();

            for (Text val : values) {
                if(wordCountMap.containsKey(val.toString())){
                    wordCountMap.put(val.toString(), wordCountMap.get(val.toString()) + 1);
                }else{
                    wordCountMap.put(val.toString(), 1);
                }
            }


            StringBuilder resultString = new StringBuilder();
            for (String docId : wordCountMap.keySet()) {
                if(resultString.length() != 0){
                    resultString.append("\t");
                }
                resultString.append(docId).append(":").append(wordCountMap.get(docId));

            }

            result.set(resultString.toString());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}