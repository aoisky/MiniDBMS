package edu.purdue.cs448.DBMS.Structure;

public class User implements java.io.Serializable {
	
	private static final long serialVersionUID = 43267655134258L;

	public static enum UserType {
		USER_A, USER_B;
	}


	private String userName;
	private UserType userType;

	public User(UserType userType, String userName){

		this.userType = userType;
		this.userName = userName;

	}

	public String getUserName(){
		return this.userName;
	}

	public UserType getUserType(){
		return this.userType;
	}

}
