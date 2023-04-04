package POJO;

import java.util.Map;

public class GRWY {
	private String input;
	private Map<String,String> map;
	public GRWY(Map<String, String> map){
		this.map=map;
	}
	public String convert(){
		StringBuilder builder = new StringBuilder();
		builder.append("('"+map.get("uid")+"', '"+map.get("mch_channel")+"', '"+map.get("login_type")+"', '"+map.get("ebank_cust_no")
				+"', '"+map.get("tran_date")+"', '"
				+map.get("tran_time")+"', '"+map.get("tran_code")+"', '"+map.get("tran_sts")+"', '"+map.get("return_code")+"', '"
				+map.get("return_msg")
				+"', '"+map.get("sys_type")+"', '"+map.get("payer_acct_no")+"', '"+map.get("payer_acct_name")+"', '"
				+map.get("payee_acct_no")
				+"', '"+map.get("payee_acct_name")+"', "+map.get("tran_amt")
				+", '"+map.get("etl_dt")
				+"')"
		);
		return builder.toString();
	}
}
