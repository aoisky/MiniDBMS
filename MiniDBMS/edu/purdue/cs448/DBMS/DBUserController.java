package edu.purdue.cs448.DBMS;

import edu.purdue.cs448.DBMS.Structure.*;
import java.util.*;
import java.io.*;

public class DBUserController{

	private String userName;
	private Hashtable<String, User> userTable;
	
	private static final String userDefUrl = "userDef.dat";
	private static final String subSchemaUrl = "subSchemaDef.dat";
	private boolean isAdminUser = false;
	

	public DBUserController(String userName){
		this.userName = userName;

		if(userName.equals("admin")){
			return;
		}

		//Get user table
		Hashtable<String, User> userTable = null;
		File userTableFile = new File(userDefUrl);

		if(userTableFile.exists()){
			try{
				userTable = DBUserController.getUserDef();
			}catch(ClassNotFoundException ex){
				System.err.println("USER Control: Class not found when reading the data file");
				System.exit(1);
			}catch(IOException ex){
				System.err.println("USER Control: IO Exception when reading the data file");
				System.exit(1);
			}
		}else{
			System.err.println("USER CONTROL: No user defined, default user is admin");
			System.exit(1);
		}

		this.userTable = userTable;

		//Check if user exists, if error then exit
		DBUserController.checkUserExists(userName, userTable);

		//Check if is admin
		this.isAdminUser = DBUserController.checkUserType(userName, userTable);
	}

	public Query userCheck(Query query){

		//Default internal admin user
		if(this.userName.equals("admin")){
			return query;
		}

		if(query == null){
			return null;
		}

		String op = null;

		if(isAdminUser != true){

			if(query instanceof Create){
				
			}else if(query instanceof Insert){
				op = "Insert";
			}else if(query instanceof Drop){
				op = "Drop";
			}else if(query instanceof Select){
				//Set query to normal user to check subschema
				((Select)query).setNormalUser();
				return query;
			}else if(query instanceof Delete){
				op = "Delete";
			}else if(query instanceof Update){
				op = "Update";
			}else if(query instanceof Help){
				//Change to subschema
				((Help)query).setNormalUser();
				return query;
			}else if(query instanceof CreateUser){
				op = "Create User";
			}else if(query instanceof DeleteUser){
				op = "Delete User";
			}else if(query instanceof CreateSubschema){
				op = "Create Subschema";
			}

			this.printPermissionError(op);
			return null;

		}else{
			//Some checks of high-priority user
			if(query instanceof CreateUser){
				op = "Create User";
			}else if(query instanceof DeleteUser){
				op = "Delete User";
			}else if(query instanceof CreateSubschema){
				op = "Create Subschema";
			}
			if(op == null){
				return query;
			}else{
				this.printPermissionError(op);
				return null;
			}
		}

		

	}

	private void printPermissionError(String op){
		
		System.err.println("USER CONTROL: User " + userName + ": Operation " + op + " denied"); 

	}

	
	private static boolean checkUserType(String userName, Hashtable<String, User> userTable){
		User.UserType userType;
		User user = userTable.get(userName);
		
		userType = user.getUserType();

		if(userType == User.UserType.USER_A){
			return true;
		}else if(userType == User.UserType.USER_B){
			return false;
		}

		return false;
	}


	private static void checkUserExists(String userName, Hashtable<String, User> userTable){
		
		if(userTable.get(userName) == null){
			System.err.println("User Control: User " + userName + " does not exists");
			System.exit(1);
		}

	}

	private static Hashtable<String, User> getUserDef() throws IOException, ClassNotFoundException{
		Hashtable<String, User> userTable = null;
		File tableFile = new File(userDefUrl);

		FileInputStream fileIn = new FileInputStream(tableFile);
		ObjectInputStream in = new ObjectInputStream(fileIn);
		userTable = (Hashtable<String, User>) in.readObject();
		in.close();
		fileIn.close();

		return userTable;
	}

}
