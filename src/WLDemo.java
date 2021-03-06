import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WLDemo extends Configured implements Tool {
	static int Top_k_Items = 10;

	public static class WLMapper1 extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		// HashMap<String, Integer> map = new HashMap<String, Integer>();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws java.io.IOException, InterruptedException {
			String IPorServerName = "";

			if (value.toString().contains(" ")) {
				String[] arr = value.toString().split(" ");
				if (arr.length > 0) {
					IPorServerName = arr[0].toLowerCase().trim();
				}
			}

			context.write(new Text(IPorServerName), new IntWritable(1));
		};
	}

	public static class WLReducer1 extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws java.io.IOException,
				InterruptedException {
			int count = 0;
			if (values.iterator().hasNext()) {

				for (IntWritable x : values) {
					int val = new Integer(x.toString());
					count += 1;
				}

				context.write(key, new IntWritable(count));
			}
		};

	}

	public static class WLMapper2 extends
			Mapper<LongWritable, Text, IntWritable, Text> {
		private TreeMap<Integer, List<String>> records = new TreeMap<Integer, List<String>>();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Parse the input string into a nice map

			if (value.toString().contains("\t")) {

				String[] arr = value.toString().split("\t");
				if (arr.length > 1) {
					System.out.println(arr[1]);
					Integer k = Integer.parseInt(arr[1]);
					List<String> keyVal = new ArrayList<String>();
					if (records.containsKey(k)) {
						keyVal = records.get(k);

					}
					keyVal.add(arr[0]);

					records.put(Integer.parseInt(arr[1]), keyVal);

					if (records.size() > Top_k_Items) {
						records.remove(records.firstKey());

					}

				}
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			for (Map.Entry<Integer, List<String>> entry : records.entrySet()) {

				Integer key = entry.getKey();
				List<String> value = entry.getValue();

				for (String val : value) {
					context.write(new IntWritable(key), new Text(val));
				}
			}

		}

	}

	public static class WLReducer2 extends
			Reducer<IntWritable, Text, Text, IntWritable> {
		int count = 0;

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			for (Text x : values) {
				if (count < Top_k_Items) {
					context.write(new Text(x), key);
					count = count + 1;
				}
			}

		};

	}

	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(IntWritable.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			// TODO Auto-generated method stub

			IntWritable ip1 = (IntWritable) w1;
			IntWritable ip2 = (IntWritable) w2;
			int cmp = -1 * ip1.compareTo(ip2);

			return cmp;
		}
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new Configuration(), new WLDemo(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();

		Job job = new Job(conf, "WL Demo");

		job.setJarByClass(WLDemo.class);

		job.setMapperClass(WLMapper1.class);

		job.setReducerClass(WLReducer1.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(IntWritable.class);

		Path in = new Path(args[0]);

		Path out = new Path(args[1]);

		Path out2 = new Path(args[2]);

		FileInputFormat.setInputPaths(job, in);

		FileOutputFormat.setOutputPath(job, out);

		boolean succ = job.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job1 failed, exiting");
			return -1;
		}
		Job job2 = new Job(conf, "top-k-pass-2");
		FileInputFormat.setInputPaths(job2, out);
		FileOutputFormat.setOutputPath(job2, out2);
		job2.setJarByClass(WLDemo.class);
		job2.setMapperClass(WLMapper2.class);
		job2.setReducerClass(WLReducer2.class);
		// job2.setReducerClass(Reducer1.class);
		job2.setInputFormatClass(TextInputFormat.class);

		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setSortComparatorClass(KeyComparator.class);
		job2.setNumReduceTasks(1);
		succ = job2.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job2 failed, exiting");
			return -1;
		}
		return 0;
	}
}
