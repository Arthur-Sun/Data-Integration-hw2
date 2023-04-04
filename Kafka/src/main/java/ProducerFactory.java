import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

public class ProducerFactory {
	public static Producer<String, String>  getProducer(String ip,String port){
		Properties props = new Properties();
		//kafka 集群，broker-list
		props.put("bootstrap.servers", ip+":"+port);
		props.put("acks", "all");
		//重试次数
		props.put("retries", 1);
		//批次大小
		props.put("batch.size", 1000000);
		//配置幂等性
		props.put("enable.idempotence",true);
		//等待时间
		props.put("linger.ms", 1);
		//RecordAccumulator 缓冲区大小
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return new KafkaProducer<String, String>(props);
	}
}
