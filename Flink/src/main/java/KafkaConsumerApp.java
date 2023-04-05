import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import POJO.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Logger;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import Utils.*;

public class KafkaConsumerApp {
	private static final Logger logger = Logger.getLogger(KafkaConsumerApp.class);
	//九种需要展示的交易类型的交易额总数
	static long[] transactions = {0,0,0,0,0,0,0,0,0};
	//程序启动的时间
	static long[] starTime = new long[1];
	//count[0]表示已处理的数据总条数 count[1]表示这一秒内处理的数据条数
	static int[] count = new int[2];
	public static void main(String[] args){
		LogConfig.initLog();
		try {
			//set the start time
			SimpleDateFormat formatter= new SimpleDateFormat("yyyyMMddHHmmssSS");
			Date date = new Date(System.currentTimeMillis());
			String temp = formatter.format(date);
			if(temp.length()==16) temp=temp+"0";
			starTime[0]=Long.parseLong(temp);
			//set Sink Producer
			SinkProducer sinkProducer = new SinkProducer();
			//get utils
			Tools myutils = new Tools();
			//set the environment of flink
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			Properties properties = new Properties();
			properties.setProperty("bootstrap.servers", "172.19.115.208:9092");
			properties.setProperty("group.id", "flink");
			Map<String, String> globalParameters = new HashMap<>();
			// ClickHouse cluster properties
			globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_HOSTS, "121.36.229.160:8123/");
			// sink common
			globalParameters.put(ClickHouseSinkConst.TIMEOUT_SEC, "1");
			globalParameters.put(ClickHouseSinkConst.FAILED_RECORDS_PATH, "d:/");
			globalParameters.put(ClickHouseSinkConst.NUM_WRITERS, "2");
			globalParameters.put(ClickHouseSinkConst.NUM_RETRIES, "2");
			globalParameters.put(ClickHouseSinkConst.QUEUE_MAX_CAPACITY, "10");
			globalParameters.put(ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, "false");
			ParameterTool parameters = ParameterTool.fromMap(globalParameters);
			env.getConfig().setGlobalJobParameters(parameters);

			//将kafka作为flink的数据源
			DataStream<String> stream = env.addSource(new FlinkKafkaConsumer011<String>(
					"test", new SimpleStringSchema(), properties) );
			//sideout分流策略，将每种不同的交易分流
			final OutputTag<String> contract = new OutputTag<String>("contract"){};
			final OutputTag<String> djk = new OutputTag<String>("djk"){};
			final OutputTag<String> dsf = new OutputTag<String>("dsf"){};
			final OutputTag<String> duebill = new OutputTag<String>("duebill"){};
			final OutputTag<String> etc = new OutputTag<String>("etc"){};
			final OutputTag<String> grwy = new OutputTag<String>("grwy"){};
			final OutputTag<String> gzdf = new OutputTag<String>("gzdf"){};
			final OutputTag<String> huanb = new OutputTag<String>("huanb"){};
			final OutputTag<String> huanx = new OutputTag<String>("huanx"){};
			final OutputTag<String> sa = new OutputTag<String>("sa"){};
			final OutputTag<String> sbyb = new OutputTag<String>("sbyb"){};
			final OutputTag<String> sdrq = new OutputTag<String>("sdrq"){};
			final OutputTag<String> sjyh = new OutputTag<String>("sjyh"){};
			final OutputTag<String> shop = new OutputTag<String>("shop"){};
			//处理1：分流
			SingleOutputStreamOperator<String> mainDataStream = stream.process(
					new ProcessFunction<String, String>() {
						@Override
						public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
							System.out.println("11");
							String type = myutils.mySplit(s)[0];
							switch (type){
								case "contract":
									context.output(contract,s);
									break;
								case "djk":
									context.output(djk,s);
									break;
								case "dsf":
									context.output(dsf,s);
									break;
								case "duebill":
									context.output(duebill,s);
									break;
								case "etc":
									context.output(etc,s);
									break;
								case "grwy":
									context.output(grwy,s);
									break;
								case "gzdf":
									context.output(gzdf,s);
									break;
								case "huanb":
									context.output(huanb,s);
									break;
								case "huanx":
									context.output(huanx,s);
									break;
								case "sa":
									context.output(sa,s);
									break;
								case "sbyb":
									context.output(sbyb,s);
									break;
								case "sdrq":
									context.output(sdrq,s);
									break;
								case "sjyh":
									context.output(sjyh,s);
									break;
								case "shop":
									context.output(shop,s);
									break;
							}
							collector.collect(s);
						}
					}
			);
			//处理2：etl处理，统计上述几种交易类型的交易总额，以及flink消费速度等相关的统计信息
			SingleOutputStreamOperator<String> etl = stream.map(new MapFunction<String, String>() {
				@Override
				public String map(String s) throws Exception {
					count[0]++;
					count[1]++;
					SimpleDateFormat formattermSec = new SimpleDateFormat("yyyyMMddHHmmssSS");
					Date date = new Date(System.currentTimeMillis());
					String time1 = formattermSec.format(date);
					if(time1.length()==16) time1=time1+"0";
					long curTime = Long.parseLong(time1);
					long timeDiff = curTime - starTime[0];
					System.out.println(curTime +" "+timeDiff);
					//每3秒钟统计一次
					if(timeDiff>3000){
						long speed = count[1]* 1000L /timeDiff;
						starTime[0] = curTime;
						count[1] = 0;
						//clickhouse
						ClickHouseProperties props = new ClickHouseProperties();
						props.setUser("");
						props.setPassword("");
						BalancedClickhouseDataSource dataSource = new BalancedClickhouseDataSource("jdbc:clickhouse://121.36.229.160:8123/dm", props);
						ClickHouseConnection conn = dataSource.getConnection();
						//flink_speed table
						PreparedStatement statement = conn.prepareStatement("insert into flink_speed values(?, ?, ?)");
						statement.setString(1, time1);
						statement.setInt(2, (int)speed);
						statement.setInt(3, count[0]);
						statement.execute();
						//transaction table
						statement = conn.prepareStatement("insert into transaction values(?,?,?,?,?,?,?,?,?,?)");
						SimpleDateFormat formatter= new SimpleDateFormat("yyyyMMddHHmmss");
						Date date1 = new Date(System.currentTimeMillis());
						statement.setString(1,formatter.format(date1));
						statement.setBigDecimal(2, BigDecimal.valueOf(transactions[0]));
						statement.setBigDecimal(3, BigDecimal.valueOf(transactions[1]));
						statement.setBigDecimal(4, BigDecimal.valueOf(transactions[2]));
						statement.setBigDecimal(5, BigDecimal.valueOf(transactions[3]));
						statement.setBigDecimal(6, BigDecimal.valueOf(transactions[4]));
						statement.setBigDecimal(7, BigDecimal.valueOf(transactions[5]));
						statement.setBigDecimal(8, BigDecimal.valueOf(transactions[6]));
						statement.setBigDecimal(9, BigDecimal.valueOf(transactions[7]));
						statement.setBigDecimal(10, BigDecimal.valueOf(transactions[8]));
						statement.execute();
						statement.close();
						conn.close();
					}
					//transaction table

					//返回结果为交易额统计信息，将sink进clickhouse
//					return "('" + formatter.format(date1) + "', " + transactions[0] + ", " + transactions[1] + ", " + transactions[2] + ", " + transactions[3] + ", " + transactions[4] + ", " + transactions[5] + ", " + transactions[6] + ", " + transactions[7] + ", " + transactions[8]+")";
					return null;
				}
			});
			//针对不同类型的交易，将交易信息注入到各种pojo类中，返回clickhouse语句
			DataStream<String> contractOut = mainDataStream.getSideOutput(contract).map((MapFunction<String, String>) s -> {
				String body = myutils.mySplit(s)[1];
				CONTRACT contract1 = new CONTRACT(myutils.getMap(body));
				return contract1.convert();
			});
			DataStream<String> djkOut = mainDataStream.getSideOutput(djk).map((MapFunction<String, String>) s -> {
				String body = myutils.mySplit(s)[1];
				Map<String, String> map = myutils.getMap(body);
				transactions[0]+=(int)Double.parseDouble(map.get("tran_amt"));
				DJK djk1 = new DJK(map);
				return djk1.convert();
			});
			DataStream<String> dsfOut = mainDataStream.getSideOutput(dsf).map((MapFunction<String, String>) s -> {
				String body = myutils.mySplit(s)[1];
				Map<String, String> map = myutils.getMap(body);
				transactions[1]+=(int)Double.parseDouble(map.get("tran_amt"));
				DSF dsf1 = new DSF(map);
				return dsf1.convert();
			});
			DataStream<String> duebillOut = mainDataStream.getSideOutput(duebill).map((MapFunction<String, String>) s -> {
				String body = myutils.mySplit(s)[1];
				DUEBILL duebill1 = new DUEBILL(myutils.getMap(body));
				return duebill1.convert();
			});
			DataStream<String> etcOut = mainDataStream.getSideOutput(etc).map((MapFunction<String, String>) s -> {
				String body = myutils.mySplit(s)[1];
				Map<String, String> map = myutils.getMap(body);
				transactions[2]+=(int)Double.parseDouble(map.get("tran_amt_fen"));
				ETC etc1 = new ETC(map);
				return etc1.convert();
			});
			DataStream<String> grwyOut = mainDataStream.getSideOutput(grwy).map((MapFunction<String, String>) s -> {
				String body = myutils.mySplit(s)[1];
				Map<String, String> map = myutils.getMap(body);
				transactions[3]+=(int)Double.parseDouble(map.get("tran_amt"));
				GRWY grwy1 = new GRWY(map);
				return grwy1.convert();
			});
			DataStream<String> gzdfOut = mainDataStream.getSideOutput(gzdf).map((MapFunction<String, String>) s -> {
				String body = myutils.mySplit(s)[1];
				GZDF gzdf1 = new GZDF(myutils.getMap(body));
				return gzdf1.convert();
			});
			DataStream<String> huanbOut = mainDataStream.getSideOutput(huanb).map((MapFunction<String, String>) s -> {
				String body = myutils.mySplit(s)[1];
				HUANB huanb1 = new HUANB(myutils.getMap(body));
				return huanb1.convert();
			});
			DataStream<String> huanxOut = mainDataStream.getSideOutput(huanx).map((MapFunction<String, String>) s -> {
				String body = myutils.mySplit(s)[1];
				HUANX huanx1 = new HUANX(myutils.getMap(body));
				return huanx1.convert();
			});
			DataStream<String> saOut = mainDataStream.getSideOutput(sa).map((MapFunction<String, String>) s -> {
				String body = myutils.mySplit(s)[1];
				Map<String, String> map = myutils.getMap(body);
				transactions[4]+=(int)Double.parseDouble(map.get("tran_amt"));
				SA sa1 = new SA(map);
				return sa1.convert();
			});
			DataStream<String> sbybOut = mainDataStream.getSideOutput(sbyb).map((MapFunction<String, String>) s -> {
				String body = myutils.mySplit(s)[1];
				Map<String, String> map = myutils.getMap(body);
				transactions[5]+=(int)Double.parseDouble(map.get("tran_amt_fen"));
				SBYB sbyb1 = new SBYB(map);
				return sbyb1.convert();
			});
			DataStream<String> sdrqOut = mainDataStream.getSideOutput(sdrq).map((MapFunction<String, String>) s -> {
				String body = myutils.mySplit(s)[1];
				Map<String, String> map = myutils.getMap(body);
				transactions[6]+=(int)Double.parseDouble(map.get("tran_amt_fen"));
				SDRQ sdrq1 = new SDRQ(map);
				return sdrq1.convert();
			});
			DataStream<String> sjyhOut = mainDataStream.getSideOutput(sjyh).map((MapFunction<String, String>) s -> {
				String body = myutils.mySplit(s)[1];
				Map<String, String> map = myutils.getMap(body);
				transactions[8]+=(int)Double.parseDouble(map.get("tran_amt"));
				SJYH sjyh1 = new SJYH(map);
				return sjyh1.convert();
			});
			DataStream<String> shopOut = mainDataStream.getSideOutput(shop).map((MapFunction<String, String>) s -> {
				String body = myutils.mySplit(s)[1];
				Map<String, String> map = myutils.getMap(body);
				transactions[7]+=(int)Double.parseDouble(map.get("tran_amt"));
				SHOP shop1 = new SHOP(map);
				return shop1.convert();
			});
			//为每个不同的流添加clickhouse目标表
			contractOut.addSink(sinkProducer.getSinkByName("dm.dm_v_tr_contract_mx"));
			saOut.addSink(sinkProducer.getSinkByName("dm.dm_v_tr_sa_mx"));
			djkOut.addSink(sinkProducer.getSinkByName("dm.dm_v_tr_djk_mx"));
			duebillOut.addSink(sinkProducer.getSinkByName("dm.dm_v_tr_duebill_mx"));
			etcOut.addSink(sinkProducer.getSinkByName("dm.dm_v_tr_etc_mx"));
			grwyOut.addSink(sinkProducer.getSinkByName("dm.dm_v_tr_grwy_mx"));
			gzdfOut.addSink(sinkProducer.getSinkByName("dm.dm_v_tr_gzdf_mx"));
			huanbOut.addSink(sinkProducer.getSinkByName("dm.dm_v_tr_huanb_mx"));
			huanxOut.addSink(sinkProducer.getSinkByName("dm.dm_v_tr_huanx_mx"));
			sbybOut.addSink(sinkProducer.getSinkByName("dm.dm_v_tr_sbyb_mx"));
			sdrqOut.addSink(sinkProducer.getSinkByName("dm.dm_v_tr_sdrq_mx"));
			sjyhOut.addSink(sinkProducer.getSinkByName("dm.dm_v_tr_sjyh_mx"));
			shopOut.addSink(sinkProducer.getSinkByName("dm.v_tr_shop_mx"));
			dsfOut.addSink(sinkProducer.getSinkByName("dm.dm_v_tr_dsf_mx"));
//			etl.addSink(sinkProducer.getSinkByName("dm.transaction"));

			env.execute("consumer");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
