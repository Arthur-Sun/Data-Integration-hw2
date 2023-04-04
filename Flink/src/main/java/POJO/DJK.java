package POJO;

import java.util.Map;

public class DJK {
	private String input;
	private Map<String,String> map;
	public DJK(Map<String,String> map){
		this.map=map;
	}
	public String convert(){
		StringBuilder builder = new StringBuilder();
		builder.append("('"+map.get("uid")+"', '"+map.get("card_no")+"', '"+map.get("tran_type")+"', '"+map.get("tran_type_desc")
				+"', "+map.get("tran_amt")+", '"
				+map.get("tran_amt_sign")+"', '"+map.get("mer_type")+"', '"+map.get("mer_code")+"', '"+map.get("rev_ind")+"', '"
				+map.get("tran_desc")
				+"', '"+map.get("tran_date")+"', '"+map.get("val_date")+"', '"+map.get("pur_date")+"', '"
				+map.get("tran_time")
				+"', '"+map.get("acct_no")+"', '"+map.get("etl_dt")
				+"')"
		);
		return builder.toString();
	}
}
