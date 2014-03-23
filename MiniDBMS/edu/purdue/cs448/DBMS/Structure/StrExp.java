package edu.purdue.cs448.DBMS.Structure;

import edu.purdue.cs448.DBMS.DBExecutor;
public class StrExp extends Exp{

	private String str;

	public StrExp(String str){
		this.str = str;
	}

	public String getString(){
		return this.str;
	}
	public Object accept(DBExecutor visitor, Value value) { return visitor.visit(this, value); }

}
