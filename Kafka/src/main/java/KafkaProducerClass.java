import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

// kafka生产者代码
public class KafkaProducerClass {
    private static Logger logger = Logger.getLogger(KafkaProducerClass.class);

    public static void main(String[] args) throws IOException, SQLException {
        int index = 0;
        int count = 0;
        SimpleDateFormat formatter= new SimpleDateFormat("yyyyMMddHHmmssSS");
        Date date = new Date(System.currentTimeMillis());
        String temp = formatter.format(date);
        if(temp.length()==16) temp=temp+"0";
        long starTime=Long.parseLong(temp);
        LogConfig.initLog();
        Producer<String, String> producer1 = ProducerFactory.getProducer("172.31.74.50","9092");
        Producer<String, String> producer2 = ProducerFactory.getProducer("172.31.74.50","9093");
        Producer<String, String> producer3 = ProducerFactory.getProducer("172.31.74.50","9094");

        for(int i=0;i<200;i+=3){
            //kafkaProducer1
            String filePath = "D:\\2大学\\大三下\\数据集成\\作业\\hw2\\data\\common\\"+(i)+".txt";
            FileInputStream fin = new FileInputStream(filePath);
            InputStreamReader reader = new InputStreamReader(fin);
            BufferedReader bufferedReader = new BufferedReader(reader);
            String strTmp = "";
            while((strTmp=bufferedReader.readLine())!=null){
                index++;
                count++;
                System.out.println(index);
                SimpleDateFormat formattermSec = new SimpleDateFormat("yyyyMMddHHmmssSS");
                Date date1 = new Date(System.currentTimeMillis());
                String time1 = formattermSec.format(date1);
                if(time1.length()==16) time1=time1+"0";
                long curTime = Long.parseLong(time1);
                long timeDiff = curTime - starTime;
                if(timeDiff>10000){
                    System.out.println("yes");
                    long speed = count/timeDiff*1000;
                    starTime = curTime;
                    count=0;
                    //clickhouse
                    ClickHouseProperties props = new ClickHouseProperties();
                    props.setUser("");
                    props.setPassword("");
                    BalancedClickhouseDataSource dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://121.36.229.160:8123/dm", props);
                    ClickHouseConnection conn = dataSource.getConnection();
                    //flink_speed table
                    PreparedStatement statement = conn.prepareStatement("insert into kafka_speed values(?, ?, ?)");
                    statement.setString(1, time1);
                    statement.setInt(2, (int)speed);
                    statement.setInt(3, index);
                    statement.execute();
                    statement.close();
                    conn.close();
                }
                sendMessage(producer1,strTmp);
            }

            //kafkaProducer2
            filePath = "D:\\2大学\\大三下\\数据集成\\作业\\hw2\\data\\common\\"+(i+1)+".txt";
            fin = new FileInputStream(filePath);
            reader = new InputStreamReader(fin);
            bufferedReader = new BufferedReader(reader);
            while((strTmp=bufferedReader.readLine())!=null){
                index++;
                count++;
                SimpleDateFormat formattermSec = new SimpleDateFormat("yyyyMMddHHmmssSS");
                Date date1 = new Date(System.currentTimeMillis());
                String time1 = formattermSec.format(date1);
                if(time1.length()==16) time1=time1+"0";
                long curTime = Long.parseLong(time1);
                long timeDiff = curTime - starTime;
                if(timeDiff>10000){
                    System.out.println("yes");
                    long speed = count/timeDiff*1000;
                    starTime = curTime;
                    count=0;
                    //clickhouse
                    ClickHouseProperties props = new ClickHouseProperties();
                    props.setUser("");
                    props.setPassword("");
                    BalancedClickhouseDataSource dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://121.36.229.160:8123/dm", props);
                    ClickHouseConnection conn = dataSource.getConnection();
                    //flink_speed table
                    PreparedStatement statement = conn.prepareStatement("insert into kafka_speed values(?, ?, ?)");
                    statement.setString(1, time1);
                    statement.setInt(2, (int)speed);
                    statement.setInt(3, index);
                    statement.execute();
                    statement.close();
                    conn.close();
                }
                sendMessage(producer2,strTmp);
            }

            //kafkaProducer3
            filePath = "D:\\2大学\\大三下\\数据集成\\作业\\hw2\\data\\common\\"+(i+2)+".txt";
            fin = new FileInputStream(filePath);
            reader = new InputStreamReader(fin);
            bufferedReader = new BufferedReader(reader);
            while((strTmp=bufferedReader.readLine())!=null){
                index++;
                count++;
                SimpleDateFormat formattermSec = new SimpleDateFormat("yyyyMMddHHmmssSS");
                Date date1 = new Date(System.currentTimeMillis());
                String time1 = formattermSec.format(date1);
                if(time1.length()==16) time1=time1+"0";
                long curTime = Long.parseLong(time1);
                long timeDiff = curTime - starTime;
                if(timeDiff>10000){
                    System.out.println("yes");
                    long speed = count/timeDiff*1000;
                    starTime = curTime;
                    count=0;
                    //clickhouse
                    ClickHouseProperties props = new ClickHouseProperties();
                    props.setUser("");
                    props.setPassword("");
                    BalancedClickhouseDataSource dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://121.36.229.160:8123/dm", props);
                    ClickHouseConnection conn = dataSource.getConnection();
                    //flink_speed table
                    PreparedStatement statement = conn.prepareStatement("insert into kafka_speed values(?, ?, ?)");
                    statement.setString(1, time1);
                    statement.setInt(2, (int)speed);
                    statement.setInt(3, index);
                    statement.execute();
                    statement.close();
                    conn.close();
                }
                sendMessage(producer3,strTmp);
            }
        }

    }
    public static void sendMessage(Producer<String, String> producer, String message){
            Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>("test", null, message));
    }
}
