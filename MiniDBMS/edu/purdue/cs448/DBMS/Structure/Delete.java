package edu.purdue.cs448.DBMS.Structure;

public class Delete extends Query{

	private String tableName;
	private Condition cond;	

	public Delete(String tableName, Condition cond){
		this.queryName = "DELETE";
		this.tableName = tableName;
		this.cond = cond;
	}

	public String getTableName(){
		return this.tableName;
	}

	public Condition getCondition(){
		return this.cond;
	}
}
