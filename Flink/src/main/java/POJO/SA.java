package POJO;

import java.util.Map;

public class SA {
	private Map<String,String> map;
	public SA(Map<String, String> map){
		this.map=map;
	}
	public String convert(){
		StringBuilder builder = new StringBuilder();
		builder.append("('");
		builder.append(map.get("uid"));
		builder.append("', '");
		builder.append(map.get("card_no"));
		builder.append("', '");
		builder.append(map.get("cust_name"));
		builder.append("', '");
		builder.append(map.get("acct_no"));
		builder.append("', ");
		builder.append(map.get("det_n"));
		builder.append(", '");
		builder.append(map.get("curr_type"));
		builder.append("', '");
		builder.append(map.get("tran_teller_no"));
		builder.append("', ");
		builder.append(map.get("cr_amt"));
		builder.append(", ");
		builder.append(map.get("bal"));
		builder.append(", ");
		builder.append(map.get("tran_amt"));
		builder.append(", '");
		builder.append(map.get("tran_card_no"));
		builder.append("', '");
		builder.append(map.get("tran_type"));
		builder.append("', '");
		builder.append(map.get("tran_log_no"));
		builder.append("', ");
		builder.append(map.get("dr_amt"));
		builder.append(", '");
		builder.append(map.get("open_org"));
		builder.append("', '");
		builder.append(map.get("dscrp_code"));
		builder.append("', '");
		builder.append(map.get("remark"));
		builder.append("', '");
		builder.append(map.get("tran_time"));
		builder.append("', '");
		builder.append(map.get("tran_date"));
		builder.append("', '");
		builder.append(map.get("sys_date"));
		builder.append("', '");
		builder.append(map.get("tran_code"));
		builder.append("', '");
		builder.append(map.get("remark_1"));
		builder.append("', '");
		builder.append(map.get("oppo_cust_name"));
		builder.append("', '");
		builder.append(map.get("agt_cert_type"));
		builder.append("', '");
		builder.append(map.get("agt_cert_no"));
		builder.append("', '");
		builder.append(map.get("agt_cust_name"));
		builder.append("', '");
		builder.append(map.get("channel_flag"));
		builder.append("', '");
		builder.append(map.get("oppo_acct_no"));
		builder.append("', '");
		builder.append(map.get("oppo_bank_no"));
		builder.append("', '");
		builder.append(map.get("src_dt"));
		builder.append("', '");
		builder.append(map.get("etl_dt"));
		builder.append("')");


		return builder.toString();
	}
}
