package edu.purdue.cs448.DBMS.Structure;

import java.util.*;

public class Table implements java.io.Serializable {
private static final long serialVersionUID = 156782376578L;

	private String tableName;
	private ArrayList<Attribute> attrList;
	private Hashtable<String, Integer> attrPosTable;
	private ArrayList<Integer> primaryList;
	private Hashtable<String, ForeignReferences> referenceTable;
	private ArrayList<String> subSchemaList = null;

	public Table(String tableName, ArrayList<Attribute> attrList, ArrayList<Integer> primaryList, Hashtable<String, ForeignReferences> referenceTable, Hashtable<String, Integer> attrPosTable){
		this.tableName = tableName;
		this.attrList = attrList;
		this.attrPosTable = attrPosTable;
		this.primaryList = primaryList;
		this.referenceTable = referenceTable;
	}

	public ArrayList<Attribute> getAttrList(){
		return this.attrList;
	}

	public ArrayList<Integer> getPrimaryList(){
		return this.primaryList;
	}

	public Hashtable<String, ForeignReferences> getReferenceTable(){
		return this.referenceTable;
	}

	public String getTableName(){
		return this.tableName;
	}


	public int getAttrPos(String valueName){
		if(this.attrPosTable.containsKey(valueName)){
			return this.attrPosTable.get(valueName).intValue();
		}else{
			return -1;
		}	
	}

	public Hashtable<String, Integer> getAttrPosHashtable(){
		return this.attrPosTable;
	}

	public void setSubschema(ArrayList<String> subSchemaList){
		this.subSchemaList = subSchemaList;
	}

	public ArrayList<String> getSubschemaList(){
		return this.subSchemaList;
	}

}

