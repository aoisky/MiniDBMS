package edu.purdue.cs448.DBMS.Structure;

public class Drop extends Query{

	private String tableName;	

	public Drop(String tableName){
		this.queryName = "DROP";
		this.tableName = tableName;
	}

	public String getTableName(){
		return this.tableName;
	}

}
