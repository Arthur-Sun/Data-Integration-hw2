package POJO;

import java.util.Map;

public class ETC {
	private String input;
	private Map<String,String> map;
	public ETC(Map<String, String> map){
		this.map=map;
	}
	public String convert(){
		StringBuilder builder = new StringBuilder();
		builder.append("('"+map.get("uid")+"', '"+map.get("etc_acct")+"', '"+map.get("card_no")+"', '"+map.get("car_no")+"', '"+map.get("cust_name")+"', '"
		+map.get("tran_date")+"', '"+map.get("tran_time")+"', "+map.get("tran_amt_fen")+", "+map.get("real_amt")+", "+map.get("conces_amt")
		+", '"+map.get("tran_place")+"', '"+map.get("mob_phone")+"', '"+map.get("etl_dt")+"')"
		);
		return builder.toString();
	}
}
