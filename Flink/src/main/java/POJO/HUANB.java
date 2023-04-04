package POJO;

import java.util.Map;

public class HUANB {
	private String input;
	private Map<String,String> map;
	public HUANB(Map<String, String> map){
		this.map=map;
	}
	public String convert(){
		StringBuilder builder = new StringBuilder();
		builder.append("('"+map.get("tran_flag")+"', '"+map.get("uid")+"', '"+map.get("cust_name")+"', '"+map.get("acct_no")
				+"', '"+map.get("tran_date")+"', '"
				+map.get("tran_time")+"', "+map.get("tran_amt")+", "+map.get("bal")+", '"+map.get("tran_code")+"', '"
				+map.get("dr_cr_code")
				+"', "+map.get("pay_term")+", '"+map.get("tran_teller_no")+"', "+map.get("pprd_rfn_amt")+", "
				+map.get("pprd_amotz_intr")
				+", '"+map.get("tran_log_no")+"', '"+map.get("tran_type")
				+"', '"+map.get("dscrp_code")+"', '"+map.get("remark")+"', '"+map.get("etl_dt")
				+"')"
		);
		return builder.toString();
	}
}
