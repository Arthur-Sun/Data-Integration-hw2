package POJO;

import java.util.Map;

public class SBYB {
	private String input;
	private Map<String,String> map;
	public SBYB(Map<String, String> map){
		this.map=map;
	}
	public String convert(){
		StringBuilder builder = new StringBuilder();
		builder.append("('"+map.get("uid")+"', '"+map.get("cust_name")+"', '"+map.get("tran_date")+"', '"+map.get("tran_sts")
				+"', '"+map.get("tran_org")+"', '"
				+map.get("tran_teller_no")+"', "+map.get("tran_amt_fen")+", '"+map.get("tran_type")+"', '"+map.get("return_msg")+"', '"
				+map.get("etl_dt")
				+"')"
		);
		return builder.toString();
	}
}
