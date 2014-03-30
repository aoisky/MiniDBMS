package edu.purdue.cs448.DBMS.Structure;

import java.util.*;

public class DeleteUser extends Query{


	private String userName;

	public DeleteUser(String userName){
		this.userName = userName;
	}


	public String getUserName(){
		return this.userName;
	}

}
