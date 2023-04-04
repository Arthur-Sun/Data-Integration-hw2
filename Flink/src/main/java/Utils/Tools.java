package Utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Tools implements Serializable {
	//将一个字段拆分为：0 type；1 body；2 data
	public String[] mySplit(String value){
		String temp = value.substring(14);
		String[] split = temp.split("},\"eventType\":");
		String body = split[0];
		String type1 = split[1].split(",")[0];
		if(type1.length()<2){
			System.out.println("??"+type1+"  "+value);
		}
		String type=type1.substring(1,type1.length()-1);
		String data = split[1].split(",")[1].substring(0,split[1].split(",")[1].length()-1);
		String[] outcome = new String[3];
		outcome[0] = type;
		outcome[1] = body;
		outcome[2] = data;
		return outcome;
	}
	//将一条数据拆成map键值对，方便在pojo类中将其转化成最终的clickhouse语句
	public Map<String,String> getMap(String input){
		Map<String,String> outcome = new HashMap<>();
		String[] sections = input.split(",");
		for(int i=0;i<sections.length;i++){
			String temp = sections[i];
			String keyT = temp.split("\":\"")[0];
			String valueT = temp.split("\":\"")[1];
			String key = keyT.substring(1);
			String value = valueT.substring(0,valueT.length()-1);
			while(value.indexOf('\'')!=-1){
				int index= value.indexOf('\'');
				value = value.substring(0,index)+value.substring(index+1);
			}
			outcome.put(key,value);
		}
		return outcome;
	}
}
