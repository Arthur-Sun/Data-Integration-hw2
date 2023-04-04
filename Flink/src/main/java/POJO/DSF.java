package POJO;

import java.util.Map;

public class DSF {
	private String input;
	private Map<String,String> map;
	public DSF(Map<String, String> map){
		this.map=map;
	}
	public String convert(){
		StringBuilder builder = new StringBuilder();
		builder.append("('"+map.get("tran_date")+"', '"+map.get("tran_log_no")+"', '"+map.get("tran_code")+"', '"+map.get("channel_flg")
				+"', '"+map.get("tran_org")+"', '"
				+map.get("tran_teller_no")+"', '"+map.get("dc_flag")+"', "+map.get("tran_amt")+", '"+map.get("send_bank")+"', '"
				+map.get("payer_open_bank")
				+"', '"+map.get("payer_acct_no")+"', '"+map.get("payer_name")+"', '"+map.get("payee_open_bank")+"', '"
				+map.get("payee_acct_no")
				+"', '"+map.get("payee_name")+"', '"+map.get("tran_sts")
				+"', '"+map.get("busi_type")+"', '"+map.get("busi_sub_type")+"', '"+map.get("etl_dt")+"', '"
				+map.get("uid")
				+"')"
		);
		return builder.toString();
	}
}
