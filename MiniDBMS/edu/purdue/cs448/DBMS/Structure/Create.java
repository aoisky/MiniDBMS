package edu.purdue.cs448.DBMS.Structure;

import java.util.*;

public class Create extends Query{

	private ArrayList<Attribute> attrList;
	private Hashtable<String, Integer> attrPosTable;
	private ArrayList<Integer> primaryList;
	private String tableName;
	private Hashtable<String, ForeignReferences>  referenceTable;	

	public Create(String tableName, ArrayList<Attribute> attrList, ArrayList<Integer> primaryList, Hashtable<String, ForeignReferences> referenceTable, Hashtable<String, Integer> attrPosTable){
		this.queryName = "CREATE";
		this.tableName = tableName;
		this.attrList = attrList;
		this.primaryList = primaryList;
		this.referenceTable = referenceTable;
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

	public Hashtable<String, ForeignReferences>  getReferences(){
		return this.referenceTable;
	}

	public int getAttrPos(String valueName){
		if(this.attrPosTable.containsKey(valueName)){
			return this.attrPosTable.get(valueName).intValue();
		}else{
			return -1;
		}	
	}

	public Table getTable(){
		return new Table(tableName, attrList, primaryList, referenceTable, attrPosTable);
	}


}
