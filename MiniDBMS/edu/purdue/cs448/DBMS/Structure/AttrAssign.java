package edu.purdue.cs448.DBMS.Structure;

public class AttrAssign{

	private String left;
	private String op;
	private Value right;	

	public AttrAssign(String left, Value right){
		this.left = left;
		this.right = right;
	}

	public String getAttrName(){
		return this.left;
	}

	public Value getValue(){
		return this.right;
	}
}
