package POJO;

import java.util.Map;

public class SDRQ {
	private String input;
	private Map<String,String> map;
	public SDRQ(Map<String, String> map){
		this.map=map;
	}
	public String convert(){
		StringBuilder builder = new StringBuilder();
		builder.append("('"+map.get("hosehld_no")+"', '"+map.get("acct_no")+"', '"+map.get("cust_name")+"', '"+map.get("tran_type")
				+"', '"+map.get("tran_date")+"', "
				+map.get("tran_amt_fen")+", '"+map.get("channel_flg")+"', '"+map.get("tran_org")+"', '"+map.get("tran_teller_no")+"', '"
				+map.get("tran_log_no")
				+"', '"+map.get("batch_no")+"', '"+map.get("tran_sts")+"', '"+map.get("return_msg")+"', '"
				+map.get("etl_dt")
				+"', '"+map.get("uid")
				+"')"
		);
		return builder.toString();
	}
}
