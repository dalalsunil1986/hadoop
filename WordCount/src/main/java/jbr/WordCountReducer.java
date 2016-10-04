package jbr.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  private static Log _log = LogFactory.getLog(WordCountReducer.class);

  public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException {
    int sum = 0;

    while (values.hasNext()) {
      sum += values.next().get();
    }
    _log.info("Sum: " + sum);
    context.write(key, new IntWritable(sum));
  }

}