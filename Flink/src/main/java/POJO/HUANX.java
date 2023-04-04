package POJO;

import java.util.Map;

public class HUANX {
	private String input;
	private Map<String,String> map;
	public HUANX(Map<String, String> map){
		this.map=map;
	}
	public String convert(){
		StringBuilder builder = new StringBuilder();
		builder.append("('"+map.get("tran_flag")+"', '"+map.get("uid")+"', '"+map.get("cust_name")+"', '"+map.get("acct_no")
				+"', '"+map.get("tran_date")+"', '"
				+map.get("tran_time")+"', "+map.get("tran_amt")+", "+map.get("cac_intc_pr")+", '"+map.get("tran_code")+"', '"
				+map.get("dr_cr_code")
				+"', "+map.get("pay_term")+", '"+map.get("tran_teller_no")+"', '"+map.get("intc_strt_date")+"', '"
				+map.get("intc_end_date")
				+"', "+map.get("intr")+", '"+map.get("tran_log_no")
				+"', '"+map.get("tran_type")+"', '"+map.get("dscrp_code")+"', '"+map.get("etl_dt")
				+"')"
		);
		return builder.toString();
	}
}
