package edu.purdue.cs448.DBMS.Structure;

import java.util.*;

public class CreateSubschema extends Query{

	private String tableName;
	private ArrayList<String> attrNameList;

	public CreateSubschema(String tableName, ArrayList<String> attrNameList){
		this.tableName = tableName;
		this.attrNameList = attrNameList;
	}

	public String getTableName(){
		return this.tableName;
	}

	public ArrayList<String> getAttrNameList(){
		return this.attrNameList;
	}

}
