package POJO;

import java.util.Map;

public class GZDF {
	private String input;
	private Map<String,String> map;
	public GZDF(Map<String, String> map){
		this.map=map;
	}
	public String convert(){
		StringBuilder builder = new StringBuilder();
		builder.append("('"+map.get("belong_org")+"', '"+map.get("ent_acct")+"', '"+map.get("ent_name")+"', '"+map.get("eng_cert_no")+"', '"
		+map.get("acct_no")+"', '"+map.get("cust_name")+"', '"+map.get("uid")+"', '"+map.get("tran_date")+"', "+map.get("tran_amt")+", '"
		+map.get("tran_log_no")+"', '"+map.get("is_secu_card")+"', '"+map.get("trna_channel")+"', '"+map.get("batch_no")+"', '"+map.get("etl_dt")
		+"')"
		);

		return builder.toString();
	}
}
