import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.header.Header;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

public class Consumer {
    private static Logger logger = Logger.getLogger(Consumer.class);

    public static void main(String[] args) {
        LogConfig.initLog();

        int row = 0;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.29.4.17:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // GROUP_ID请使用学号，不同组应该使用不同的GROUP。
        // 原因参考：https://blog.csdn.net/daiyutage/article/details/70599433
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "201850100");
        // 原因参考：https://blog.csdn.net/matrix_google/article/details/88658234
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"student\" password=\"nju2023\";");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Collections.singletonList("transaction"));

        File writeSpecialName = new File("D:\\2大学\\大三下\\数据集成\\作业\\hw2\\data\\special\\special.txt"); // 相对路径，如果没有则要建立一个新的output。txt文件
        try {

            if (!writeSpecialName.exists()) writeSpecialName.createNewFile(); // 创建新文件
            BufferedWriter specialOut = new BufferedWriter(new FileWriter(writeSpecialName));
            File writeCommonName = new File("D:\\2大学\\大三下\\数据集成\\作业\\hw2\\data\\common\\0.txt");
            if (!writeCommonName.exists()) writeSpecialName.createNewFile(); // 创建新文件
            BufferedWriter commonOut = new BufferedWriter(new FileWriter(writeCommonName));
            // 会从最新数据开始消费
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    // 获取消息数据
//                    System.out.println(record.value());
                    row++;
                    if ((row % 100000) == 0) {
                        logger.info(row/100000);
                        commonOut.flush();
                        commonOut.close();
                        writeCommonName = new File("D:\\2大学\\大三下\\数据集成\\作业\\hw2\\data\\common\\" + row/100000 + ".txt"); // 相对路径，如果没有则要建立一个新的output。txt文件
                        if (!writeCommonName.exists()) writeCommonName.createNewFile(); // 创建新文件
                        commonOut = new BufferedWriter(new FileWriter(writeCommonName));
                    }
                    commonOut.write(record.value());
                    commonOut.newLine();
                    // 获取消息头
                    Header groupIdHeader = record.headers().lastHeader("groupId");
                    if (Objects.nonNull(groupIdHeader)) {
                        byte[] groupId = groupIdHeader.value();
                        // 此处yourGroupId替换成你们组的组号
                        if (Arrays.equals("11".getBytes(), groupId)) {
                            // 额外记录这条数据
                            logger.info("special:"+row);
                            specialOut.write(record.value());
                            specialOut.newLine();
                        }
                    }
                }
                specialOut.flush();
                commonOut.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }




}
