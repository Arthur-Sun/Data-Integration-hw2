package Utils;

import ru.ivi.opensource.flinkclickhousesink.ClickHouseSink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst;
import java.util.Properties;

//根据表名返回clickhouse的api接口
public class SinkProducer {
	public ClickHouseSink getSinkByName(String tableName){
		Properties properties = new Properties();
		properties.put(ClickHouseSinkConst.TARGET_TABLE_NAME,tableName);
		properties.put(ClickHouseSinkConst.MAX_BUFFER_SIZE,"10000");
		return new ClickHouseSink(properties);
	}
}
