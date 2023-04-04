package POJO;

import java.util.Map;

	public class DUEBILL {
		private String input;
		private Map<String,String> map;
		public DUEBILL(Map<String, String> map){
			this.map=map;
		}
		public String convert(){
			StringBuilder builder = new StringBuilder();
			builder.append("('"+map.get("uid")+"', '"+map.get("acct_no")+"', '"+map.get("receipt_no")+"', '"
					+map.get("contract_no")+"', '"+map.get("subject_no")+"', '"
					+map.get("cust_no")+"', '"+map.get("loan_cust_no")+"', '"+map.get("cust_name")+"', '"+
					map.get("buss_type")+"', '"+map.get("curr_type")
					+"', "+map.get("buss_amt")+", '"+map.get("putout_date")+"', '"+map.get("matu_date")+"', '"+
					map.get("actu_matu_date")+"', "+map.get("buss_rate")+", '"
					+map.get("actu_buss_rate")+"', '"+map.get("intr_type")+"', '"+map.get("intr_cyc")+"', "+
					map.get("pay_times")+", '"+map.get("pay_cyc")
					+"', "+map.get("extend_times")+", "+map.get("bal")+", "+map.get("norm_bal")
					+", "+map.get("dlay_amt")+", "+map.get("dull_amt")+", "+map.get("bad_debt_amt")
					+", "+map.get("owed_int_in")+", "+map.get("owed_int_out")+", "+map.get("fine_pr_int")
					+", "+map.get("fine_intr_int")+", "+map.get("dlay_days")
					+", '"+map.get("pay_acct")+"', '"+map.get("putout_acct")+"', '"+map.get("pay_back_acct")+"', "+
					map.get("due_intr_days")+", '"+map.get("operate_org")+"', '"
					+map.get("operator")+"', '"+map.get("reg_org")+"', '"+map.get("register")+"', '"+
					map.get("occur_date")+"', '"+map.get("loan_use")+
					"', '"+map.get("pay_type")+"', '"+map.get("pay_freq")+"', '"+map.get("vouch_type")+"', '"+
					map.get("mgr_no")+"', '"+map.get("mge_org")+"', '"
					+map.get("loan_channel")+"', '"+map.get("ten_class")+"', '"+map.get("src_dt")+"', '"+
					map.get("etl_dt")+
					"')"
			);
			return builder.toString();
		}
}
