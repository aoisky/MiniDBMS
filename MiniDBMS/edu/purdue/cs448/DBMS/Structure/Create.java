package edu.purdue.cs448.DBMS.Structure;

import java.util.*;

public class Create extends Query{

	private ArrayList<Attribute> attrList;
	private Hashtable<String, Integer> attrPosTable;
	private ArrayList<Integer> primaryList;
	private String tableName;
	private List referenceList;	

	public Create(String tableName, ArrayList<Attribute> attrList, ArrayList<Integer> primaryList, List referenceList, Hashtable<String, Integer> attrPosTable){
		this.queryName = "CREATE";
		this.tableName = tableName;
		this.attrList = attrList;
		this.primaryList = primaryList;
		this.referenceList = referenceList;
		this.attrPosTable = attrPosTable;
	}

	public String getTableName(){
		return this.tableName;
	}

	public ArrayList<Attribute> getAttributes(){
		return this.attrList;
	}

	public ArrayList<Integer> getPrimarys(){
		return this.primaryList;
	}

	public List getReferences(){
		return this.referenceList;
	}

	public Table getTable(){
		return new Table(tableName, attrList, primaryList, referenceList, attrPosTable);
	}


}
