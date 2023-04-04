package POJO;

import java.util.Map;

public class SHOP {
	private String input;
	private Map<String,String> map;
	public SHOP(Map<String, String> map){
		this.map=map;
	}
	public String convert(){
		StringBuilder builder = new StringBuilder();
		builder.append("('");
		builder.append(map.get("tran_channel")+"', '"+map.get("order_code")+"', '"+map.get("shop_code")+"', '"+map.get("shop_name")
		+"', '"+map.get("h1w_tran_type")+"', '"+map.get("tran_date")+"', '"+map.get("tran_time")+"', "+map.get("tran_amt")+", '"
		+map.get("current_status")+"', "+map.get("score_num")+", '"+map.get("pay_channel")+"', '"+map.get("uid")+"', '"+map.get("legal_name")
		+"', '"+map.get("etl_dt")+"')"
		);
		return builder.toString();
	}
}
