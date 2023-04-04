package POJO;

import java.util.Map;

public class CONTRACT {
	private String input;
	private Map<String,String> map;
	public CONTRACT(Map<String, String> map){
		this.map=map;
	}
	public String convert(){
		StringBuilder builder = new StringBuilder();
		builder.append("('"+map.get("uid")+"', '"+map.get("contract_no")+"', '"+map.get("apply_no")+"', '"+map.get("artificial_no")+
				"', '"+map.get("occur_date")+"', '"
				+map.get("loan_cust_no")+"', '"+map.get("cust_name")+"', '"+map.get("buss_type")+"', '"+map.get("occur_type")
				+"', '"+map.get("is_credit_cyc")
				+"', '"+map.get("curr_type")+"', "+map.get("buss_amt")+", "+map.get("loan_pert")+
				", "+map.get("term_year")+", "
				+map.get("term_mth")+", "+map.get("term_day")+", '"+map.get("base_rate_type")+"', "+map.get("base_rate")
				+", '"+map.get("float_type")
				+"', "+map.get("rate_float")+", "+map.get("rate")+", "+map.get("pay_times")+
				", '"+map.get("pay_type")+"', '"
				+map.get("direction")+"', '"+map.get("loan_use")+"', '"+map.get("pay_source")+"', '"+map.get("putout_date")
				+"', '"+map.get("matu_date")
				+"', '"+map.get("vouch_type")+"', '"+map.get("is_oth_vouch")+"', '"+map.get("apply_type")+
				"', "+map.get("extend_times")+", "
				+map.get("actu_out_amt")+", "+map.get("bal")+", "+map.get("norm_bal")+", "+map.get("dlay_bal")
				+", "+map.get("dull_bal")
				+", "+map.get("owed_int_in")+", "+map.get("owed_int_out")+", "+map.get("fine_pr_int")+
				", "+map.get("fine_intr_int")+", "
				+map.get("dlay_days")+", '"+map.get("five_class")+"', '"+map.get("class_date")+"', '"+map.get("mge_org")
				+"', '"+map.get("mgr_no")
				+"', '"+map.get("operate_org")+"', '"+map.get("operator")+"', '"+map.get("operate_date")+
				"', '"+map.get("reg_org")+"', '"
				+map.get("register")+"', '"+map.get("reg_date")+"', '"+map.get("inte_settle_type")+"', '"+map.get("is_bad")
				+"', "+map.get("frz_amt")
				+", '"+map.get("con_crl_type")+"', '"+map.get("shift_type")+"', "+map.get("due_intr_days")+
				", '"+map.get("reson_type")+"', "
				+map.get("shift_bal")+", '"+map.get("is_vc_vouch")+"', '"+map.get("loan_use_add")+"', '"+map.get("finsh_type")
				+"', '"+map.get("finsh_date")
				+"', '"+map.get("sts_flag")+"', '"+map.get("src_dt")+"', '"+map.get("etl_dt")
				+"')"
		);
		return builder.toString();
	}
}
