import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class test {
	public static void main(String[] args) throws InterruptedException {
//		SimpleDateFormat formatter= new SimpleDateFormat("yyyyMMdd");
//		Date date = new Date(System.currentTimeMillis());
//		System.out.println(formatter.format(date));

//		double one = 9.99999999;
//		DecimalFormat format = new DecimalFormat("#.00");
//		String str = format.format(one);
//		double four = Double.parseDouble(str);
//		System.out.println(four);
//		System.out.println(System.currentTimeMillis());
//		Thread.sleep(1000);
//		System.out.println(System.currentTimeMillis());
//		Thread.sleep(1000);
//		System.out.println(System.currentTimeMillis());
		long t1 = System.currentTimeMillis();
		int a = 10;
		for(int m=0;m<10000;m++) a++;
//		Thread.sleep(1000);
		long t2 = System.currentTimeMillis();
//		Thread.sleep(1000);
		long t3 = System.currentTimeMillis();
//		Thread.sleep(1000);
		long t4 = System.currentTimeMillis();
//		Thread.sleep(1000);
		long t5 = System.currentTimeMillis();
		System.out.println((t2-t1)+" "+(t3-t2)+" "+(t4-t3)+" "+(t5-t4));

//		//每10000条数据计算一次速度
//		if((count[0]%5000)==0) {//current time
//			SimpleDateFormat formattermSec = new SimpleDateFormat("yyyyMMddHHmmssSS");
//			Date date = new Date(System.currentTimeMillis());
//			String time1 = formattermSec.format(date);
//			if(time1.length()==16) time1=time1+"0";
//			long curTime = Long.parseLong(time1);
//			long timeDiff = curTime - starTime[0];
//			long speed = (long)((double)count[0]/(double) timeDiff)*1000;
//			System.out.println("timeDiff: "+timeDiff + "speed: "+speed+"count: "+count[0]);
//			//clickhouse
//			ClickHouseProperties props = new ClickHouseProperties();
//			props.setUser("");
//			props.setPassword("");
//			BalancedClickhouseDataSource dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://172.19.102.95:8123/dm", props);
//			ClickHouseConnection conn = dataSource.getConnection();
//			//flink_speed table
//			PreparedStatement statement = conn.prepareStatement("insert into flink_speed values(?, ?, ?)");
//			statement.setString(1, time1);
//			statement.setInt(2, (int)speed);
//			statement.setInt(3, count[0]);
//			statement.execute();
//			statement.close();
//			conn.close();
//		}


	}
}
