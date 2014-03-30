package edu.purdue.cs448.DBMS.Structure;

import java.util.*;

public class CreateUser extends Query{


	private String userName;
	private User.UserType userType;

	public CreateUser(User.UserType userType, String userName){
		this.userName = userName;
		this.userType = userType;
	}

	public String getUserName(){
		return this.userName;
	}

	public User.UserType getUserType(){
		return this.userType;
	}

}
