package edu.purdue.cs448.DBMS.Structure;

import java.util.*;

public class ForeignReferences implements java.io.Serializable{
	private static final long serialVersionUID = 334753856148L;

	private String tableName;
	private String attrName;

	public ForeignReferences(String tableName, String attrName){

		this.tableName = tableName;
		this.attrName = attrName;

	}

	public String getTableName(){
		return this.tableName;
	}

	public String getAttrName(){
		return this.attrName;
	}

}
