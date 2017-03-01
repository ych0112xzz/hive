package hive.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class SumPer extends UDAF {

	public static class AvgEvaluate implements UDAFEvaluator {
		public static class PartialResult {
			public int count;
			public long total;
			public ArrayList<Long> percent;

			public PartialResult() {
				count = 0;
				total = 0;
				percent=null;
			}
		}

		private PartialResult partialResult;

		public void init() {
			partialResult = new PartialResult();
		}

		public boolean iterate(IntWritable value) {
			// 此处一定要判断partialResult是否为空，否则会报错
			// 原因就是init函数只会被调用一遍，不会为每个部分聚集操作去做初始化
			// 此处如果不加判断就会出错
			if (partialResult == null) {
				partialResult = new PartialResult();
			}

			if (value != null) {
				partialResult.total = partialResult.total + value.get();
				partialResult.percent.add(partialResult.total);
				// partialResult.count=partialResult.count + 1;
			}

			return true;
		}

		public PartialResult terminatePartial() {
			return partialResult;
		}

		public boolean merge(PartialResult other) {
			partialResult.total = partialResult.total + other.total;
			// partialResult.count=partialResult.count + other.count;
			partialResult.percent.addAll(other.percent);
			return true;
		}

		public LongWritable terminate() {
			return new LongWritable(partialResult.percent.size());
//			Long[] s = new Long[partialResult.percent.size()];
//			s= (Long[]) partialResult.percent.toArray();
//			return s;

		}
	}
}