package edu.purdue.cs448.DBMS.Structure;

import edu.purdue.cs448.DBMS.DBExecutor;

public class IntExp extends Exp{
	private static final long serialVersionUID = 2543146112341L;
	private int intNum;

	public IntExp(int intNum){
		this.intNum = intNum;
	}

	public int getInt(){
		return this.intNum;
	}

	public Object accept(DBExecutor visitor, Value value) { return visitor.visit(this, value); }

}
