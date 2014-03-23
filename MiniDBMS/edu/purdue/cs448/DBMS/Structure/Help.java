package edu.purdue.cs448.DBMS.Structure;



public class Help extends Query{

	public static enum HelpType{
		DESCRIBE, CREATE, INSERT, DELETE, DROP, UPDATE, SELECT, TABLES
	}

	private String tableName;
	private HelpType helpType;

	public Help(HelpType helpType){
		this.queryName = "HELP";
		this.helpType = helpType;
	}

	public Help(HelpType helpType, String tableName){
		this.queryName = "HELP";
		this.tableName = tableName;
		this.helpType = helpType;
	}

	public String getTableName(){
		return this.tableName;
	}

	public HelpType getHelpType(){
		return this.helpType;
	}
}
