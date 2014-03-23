package edu.purdue.cs448.DBMS.Structure;

public class Value implements java.io.Serializable {

	private static final long serialVersionUID = 274565752285L;

	private Attribute.Type type;

	private int intValue;
	private String charValue;
	private double doubleValue;

	public Value(int intValue){
		this.intValue = intValue;
		this.type = Attribute.Type.INT;
	}

	public Value(String charValue){
		this.charValue = charValue;
		this.type = Attribute.Type.CHAR;
	}

	public Value(double doubleValue){
		this.doubleValue = doubleValue;
		this.type = Attribute.Type.DECIMAL;
	}

	public String toString(){
		if(this.type == Attribute.Type.INT){
			return String.valueOf(this.intValue);
		}else if(this.type == Attribute.Type.CHAR){
			return this.charValue;
		}else if(this.type == Attribute.Type.DECIMAL){
			return String.valueOf(this.doubleValue);
		}else{
			return null;
		}
	}

	public int getInt(){
		return intValue;
	}

	public String getChar(){
		return charValue;
	}

	public double getDouble(){
		return doubleValue;
	}

	public Attribute.Type getType(){
		return type;
	}

	public boolean equals(Value value){

		if(value.getType() == this.type){
			if(this.type == Attribute.Type.CHAR){
				if(value.getChar().equals(this.getChar())){
					return true;
				}
			}else if(this.type == Attribute.Type.INT){
				if(value.getInt() == this.getInt()){
					return true;
				}
			}else if(this.type == Attribute.Type.DECIMAL){
				if(value.getDouble() == this.getDouble()){
					return true;
				}
			}else{
				return false;
			}
		}else{
			return false;
		}

		return false;

	}
}
