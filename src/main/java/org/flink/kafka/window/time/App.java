package org.flink.kafka.window.time;

import java.io.Serializable;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

class App {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(),
				properties);

		DataStream<String> stream = env.addSource(myConsumer);

		stream.map(new MapFunction<String, Count>() {
			@Override
			public Count map(String value) throws Exception {
				return new Count(value, 1);
			}
		}).keyBy("symbol").timeWindow(Time.milliseconds(1000)).sum("count").print();

		env.execute();

	}

	public static class Count implements Serializable {
		private static final long serialVersionUID = 1L;
		public String symbol;
		public Integer count;

		public Count() {
		}

		public Count(String symbol, Integer count) {
			this.symbol = symbol;
			this.count = count;
		}

		@Override
		public String toString() {
			return "Count{" + "symbol='" + symbol + '\'' + ", count=" + count + '}';
		}
	}
}
