package edu.purdue.cs448.DBMS.Structure;

public class Attribute implements java.io.Serializable {
private static final long serialVersionUID = 77658635285L;
	public static enum Type {
		INT, CHAR, DECIMAL
	}

	private Type type;
	private String attrName;
	private Condition checkCond;
	private boolean primary;
	private int length = 0;

	public Attribute(Type type, String attrName, int length) {
		this.type = type;
		this.attrName = attrName;
		this.length = length;
	}

	public Attribute(Type type, String attrName){
		this.type = type;
		this.attrName = attrName;
	}

	public String getName(){
		return this.attrName;
	}	
	
	public Type getType(){
		return this.type;
	}	
	
	public void setPrimary(){
		this.primary = true;
	}

	public boolean isPrimary(){
		return primary;
	}

	public void setCheckCond(Condition cond){
		this.checkCond = cond;

	}

	public Condition getCheckCond(){
		return this.checkCond;

	}

	public void setLength(int length){
		this.length = length;
	}

	public int getLength(){
		return this.length;
	}

	
	@Override
	public boolean equals(Object attribute){
		if(attribute instanceof Attribute){
			if(((Attribute)attribute).getName().equals(this.getName())){
				return true;
			}else{
				return false;
			}
		}else if(attribute instanceof String){
			if(((String) attribute).equals(this.getName())){
				return true;
			}else{
				return false;
			}
		}else{
			return false;
		}
	}
}
