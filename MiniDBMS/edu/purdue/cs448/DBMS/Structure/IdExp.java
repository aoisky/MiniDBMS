package edu.purdue.cs448.DBMS.Structure;

import edu.purdue.cs448.DBMS.DBExecutor;
import java.util.*;
public class IdExp extends Exp{
	private String id;

	public IdExp(String id){
		this.id = id;
	}

	public String getId(){
		return this.id;
	}

	public Object accept(DBExecutor visitor, Value value) { return visitor.visit(this, value); }

	public Object accept(DBExecutor visitor, Hashtable<String, Integer> attrPosTable, ArrayList<Value> tuple) { return visitor.visit(this, attrPosTable, tuple); }
}
