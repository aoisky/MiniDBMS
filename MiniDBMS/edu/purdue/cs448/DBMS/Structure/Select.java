package edu.purdue.cs448.DBMS.Structure;

import java.util.*;

public class Select extends Query{

	private ArrayList<String> attrList;
	private ArrayList<String> tableNames;
	private Condition cond;
	private boolean selectAll;

	public Select(ArrayList<String> attrList, ArrayList<String> tableNames, Condition cond){
		this.queryName = "SELECT";
		this.tableNames = tableNames;
		this.attrList = attrList;
		this.cond = cond;
	}

	public Select(ArrayList<String> tableNames, Condition cond, boolean selectAll){
		this.queryName = "SELECT";
		this.tableNames = tableNames;
		this.cond = cond;
		this.selectAll = true;
	}

	public ArrayList<String> getTableNames(){
		return this.tableNames;
	}

	public ArrayList<String> getAttrStrList(){
		return this.attrList;
	}

	public Condition getCondition(){
		return this.cond;
	}

	public boolean isSelectAll(){
		return this.selectAll;
	}


}
