package edu.purdue.cs448.DBMS.Structure;

import edu.purdue.cs448.DBMS.DBExecutor;

public class DoubleExp extends Exp{
	private double doubleNum;

	public DoubleExp(double doubleNum){
		this.doubleNum = doubleNum;
	}

	public double getDouble(){
		return this.doubleNum;
	}
	public Object accept(DBExecutor visitor, Value value) { return visitor.visit(this, value); }
}
