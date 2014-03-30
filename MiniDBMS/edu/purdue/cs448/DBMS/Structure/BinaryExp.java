package edu.purdue.cs448.DBMS.Structure;

import edu.purdue.cs448.DBMS.DBExecutor;
import java.util.*;

public class BinaryExp extends Exp{
	private static final long serialVersionUID = 1276455294L;
	private Exp right;
	private Exp left;
	private String op;

	public BinaryExp(Exp left, String op, Exp right){
		this.left = left;
		this.right = right;
		this.op = op;
	}

	public Exp getLeft(){
		return this.left;
	}

	public Exp getRight(){
		return this.right;
	}

	public String getOp(){
		return this.op;
	}

	public Object accept(DBExecutor visitor, Value value,Hashtable<String, Integer> attrPosTable, ArrayList<Value> tuple) { return visitor.visit(this, value, attrPosTable, tuple); }
}
