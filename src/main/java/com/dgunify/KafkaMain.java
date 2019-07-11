package com.dgunify;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;

import net.sf.json.JSONObject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import scala.Tuple2;

/**
 * dgunify
 */
public class KafkaMain {
	public static void main(String[] args) throws InterruptedException {
		try {

			Properties p = new Properties();
			p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkCons.ip);// kafka地址，多个地址用逗号分割
			p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			//获取需要消费的topics
			Collection<String> topics = KafkaUtil.getAllGroupsForTopic(p, "logs");

			Map<String, Object> kafkaParams = new HashMap<String, Object>();
			kafkaParams.put("bootstrap.servers", KafkCons.ip);
			kafkaParams.put("key.deserializer", StringDeserializer.class);
			kafkaParams.put("value.deserializer", StringDeserializer.class);
			kafkaParams.put("group.id", "logs_group_id");
			kafkaParams.put("auto.offset.reset", "latest");
			kafkaParams.put("enable.auto.commit", false);
			//
			SparkConf conf = new SparkConf().setMaster("spark://hadoop0:7077")
					.setAppName("SparkStreamingOnKafkaDirected");
			//创建流数据上下文
			JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));
			//创建流数据
			JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext,
					LocationStrategies.PreferConsistent(),
					ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
			//获取流数据消费
			JavaDStream<String> jds = stream.mapPartitions(new FlatMapFunction<Iterator<ConsumerRecord<String, String>>, String>() {
						public Iterator<String> call(Iterator<ConsumerRecord<String, String>> t) throws Exception {
							List<String> ls = new ArrayList<String>();
							MacAddress m = new MacAddress();
							String hdfsurl = "hdfs://hadoop0:9000";
							if (t != null && t.hasNext()) {
								FileSystem fs = getFileSystem(hdfsurl);
								while (t.hasNext()) {
									ConsumerRecord<String, String> record = t.next();
									String value = record.value();
									if(value == null || "".equals(value)) {
										continue;
									}
									JSONObject jso = null;
									try {
										jso = JSONObject.fromObject(value);
										if(jso == null) {
											continue;
										}
									} catch (Exception e) {
										//e.printStackTrace();
										continue;
									}
									String username = jso.get("username").toString();
									OutputStream os = fs.create(new Path(hdfsurl + "/datas/" + System.currentTimeMillis() + ".txt"));
									ls.add(username);
									InputStream is = new ByteArrayInputStream(username.getBytes());
									//把数据写入hdfs
									IOUtils.copyBytes(is, os, 4096, true);
									is.close();
									os.close();
								}
								fs.close();
							}
							return ls.iterator();
						}
					});

			jds.print();//打印
			streamingContext.start();
			streamingContext.awaitTermination();
			streamingContext.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String getStrByMap(Map<String, String> map, String keys) {
		String[] keysArr = keys.split("、");
		String str = "";
		for (String key : keysArr) {
			String tmpStr = "";
			if (map.get(key) != null) {
				tmpStr = map.get(key);
			}
			if ("".equals(str)) {
				str = tmpStr;
			} else {
				str += " " + tmpStr;
			}
		}
		return str;
	}

	/**
	 * 获取文件系统
	 *
	 * @return FileSystem 文件系统
	 */
	public static FileSystem getFileSystem(String HDFSUri) {
		// 读取配置文件
		Configuration conf = new Configuration();
		// 文件系统
		FileSystem fs = null;
		String hdfsUri = HDFSUri;
		if (StringUtils.isBlank(hdfsUri)) {
			// 返回默认文件系统 如果在 Hadoop集群下运行，使用此种方法可直接获取默认文件系统
			try {
				fs = FileSystem.get(conf);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			// 返回指定的文件系统,如果在本地测试，需要使用此种方法获取文件系统
			try {
				URI uri = new URI(hdfsUri.trim());
				fs = FileSystem.get(uri, conf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return fs;
	}

	private static Map<String, String> getMapByObj(JSONObject jso, String keys) {
		String[] keysArr = keys.split("、");
		Map<String, String> reMap = new HashMap<String, String>();
		for (String key : keysArr) {
			Object obj = jso.get(key);
			if (obj != null) {
				reMap.put(key, obj.toString());
			}
		}
		return reMap;
	}
}
