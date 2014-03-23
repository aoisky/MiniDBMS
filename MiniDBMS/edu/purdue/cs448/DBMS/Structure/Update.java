package edu.purdue.cs448.DBMS.Structure;

import java.util.*;

public class Update extends Query{

	private String tableName;
	private Condition cond;	
	private ArrayList<AttrAssign> attrAssignList;

	public Update(String tableName, ArrayList<AttrAssign> attrAssignList, Condition cond){
		this.queryName = "UPDATE";
		this.tableName = tableName;
		this.cond = cond;
		this.attrAssignList = attrAssignList;
	}

	public String getTableName(){

		return this.tableName;
	}

	public Condition getCondition(){

		return this.cond;
	}

	public ArrayList<AttrAssign> getAttrAssignList(){

		return this.attrAssignList;
	}

}
