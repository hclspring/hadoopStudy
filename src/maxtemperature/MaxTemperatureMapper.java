package maxtemperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.*;

//Reference: Page 32 of "Hadoop: The Definitive Guide"
public class MaxTemperatureMapper// extends MapReduceBase
	extends Mapper<LongWritable, Text, Text, IntWritable>{
	private static final int MISSING = 9999;
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.substring(15, 19);
		int airTemperature;
		if (line.charAt(87) == '+') {
			airTemperature = Integer.parseInt(line.substring(88, 92));
		} else {
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		String quality = line.substring(92, 93);
		if (airTemperature != MISSING && quality.matches("[01459]")) {
			System.out.println("Year = " + year
					+ ", airTemperature = " + airTemperature);
			context.write(new Text(year), new IntWritable(airTemperature));
		}
	}
}
