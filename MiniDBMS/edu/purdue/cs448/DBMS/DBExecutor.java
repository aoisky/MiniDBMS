package edu.purdue.cs448.DBMS;

import edu.purdue.cs448.DBMS.Structure.*;
import java.util.*;
import java.io.*;

//DBExecutor Author: Yudong Yang
public class DBExecutor {
	public static final String databaseDefUrl = "databaseDef.dat";



	public void execute(Query query){
		try{
			if(query instanceof Create){
				create((Create)query);
			}else if(query instanceof Insert){
				insert((Insert) query);
			}else if(query instanceof Drop){
				drop((Drop) query);
			}else if(query instanceof Select){
				select((Select) query);
			}else if(query instanceof Delete){
				delete((Delete) query);
			}else if(query instanceof Update){
				update((Update) query);
			}else if(query instanceof Help){
				help((Help) query);
			}
		}catch(IOException ex){
			System.err.println(ex.getMessage());
		}catch(ClassNotFoundException ex){
			System.err.println(ex.getMessage());
		}catch(Error ex){
			System.err.println(ex.getMessage());
		}
	}


	private void create(Create query) throws IOException, Error, ClassNotFoundException{
		Hashtable<String, Table> tables = null;
		File tableFile = new File(databaseDefUrl);
		if(tableFile.exists()){
			tables = this.getTableDef();
		}else{
			tables = new Hashtable<String, Table>();
		}
		//Check if it already has a table
		if(tables.get(query.getTableName()) != null){
			throw new Error("CREATE TABLE: Table " + query.getTableName() + " already exists");
		}
		//Write table definition to the databaseDef.dat
		tables.put(query.getTableName(), query.getTable());
		this.writeTableDef(tableFile, tables);	
		System.out.println("Table created successfully");
	}


	public void insert(Insert query) throws IOException, Error, ClassNotFoundException {
		Hashtable<String, Table> tables = null;
		ArrayList<Value> valueList = null;
		ArrayList< ArrayList<Value> > tupleList; 
		Table table;
		File tableFile = new File(databaseDefUrl);
		if(tableFile.exists()){
			tables = this.getTableDef();
		}else{
			throw new Error("INSERT: No database defined");  
		}
		if((table = tables.get(query.getTableName()) )!= null ){
			valueList = this.convertInsertValuesType(table, query.getValueList());
			boolean constraintCheck = this.evaluateConstraintsCond(table, valueList);

			if(!constraintCheck){
				throw new Error("INSERT: Constraints check violated");
			}
			File tupleFile = new File(query.getTableName() + ".db");
			if(tupleFile.exists()){
				tupleList = this.getTupleList(tupleFile);
			}else{
				tupleList = new ArrayList<ArrayList<Value>>();
			}

			if(tupleList != null && valueList != null){
				//Check primary key constraints
				boolean isNotViolatePrimary = checkPrimarys(table.getPrimaryList(), tupleList, valueList);

				//Needs to check length
				//!!!!!!!!!!!!!!!!!!
				if(isNotViolatePrimary){
					tupleList.add(valueList);
				}else{
					throw new Error("INSERT: Primary key constraints violated");
				}
			}
			this.saveTupleList(tupleFile, tupleList);
			System.out.println("Tuple inserted successfully");
		}else{
			throw new Error("INSERT: No Table " + query.getTableName() + " Found");
		}


	}

	public void select(Select query) throws IOException, Error, ClassNotFoundException{
		Hashtable<String, Table> tables = null;

		File tableFile = new File(databaseDefUrl);
		//Check if database defined
		if(tableFile.exists()){
			tables = this.getTableDef();
		}else{
			throw new Error("SELECT: No Database Defined");
		}

		ArrayList<String> tableNames = query.getTableNames();
		ArrayList<String> attrStrList = query.getAttrStrList();
		Condition selectCond = query.getCondition();
		//Hash table and arraylist to save table
		Hashtable<String, Table> tableList = new Hashtable<String, Table>();
		ArrayList<Table> tableArrayList = new ArrayList<Table>();
		//Hash table to save tuples for each table
		Hashtable<String, ArrayList< ArrayList<Value> > > tupleHashTable = new Hashtable<String, ArrayList<ArrayList<Value>> >();

		//Check if the table defined
		for(String tableName : tableNames){
			if(!tables.containsKey(tableName)){
				throw new Error("SELECT: No table " + tableName + " Found");
			}else{
				tableList.put(tableName, tables.get(tableName));
				tableArrayList.add(tables.get(tableName));
			}

			File tupleFile = new File(tableName + ".db");
			if(!tupleFile.exists()){
				throw new Error("SELECT: No tuple in the table " + tableName); 
			}else{
				ArrayList< ArrayList<Value> > tupleList = this.getTupleList(tupleFile);
				tupleHashTable.put(tableName, tupleList);
			}
			
		}

		//Get conditional attributes if not null
		ArrayList<String> conditionAttributeList = null;
		if(selectCond != null){
			conditionAttributeList = selectCond.getIdList();
		}

		//Get all attributes without duplicates
		ArrayList<String> allAttributeList = null;
		if(!query.isSelectAll()){
			allAttributeList = new ArrayList<String>(attrStrList);

		}else{
			//If select all attributes
			allAttributeList = new ArrayList<String>();
			//attrStrList = new ArrayList<String>();
			for(String tableName : tableList.keySet()){
				Table table = tableList.get(tableName);
				for(Attribute attr : table.getAttrList()){
					allAttributeList.add(attr.getName());
					//
				}
			}

		}
			//Add condition attributes into all attributes if not added yet
			if(conditionAttributeList != null){
				for(String condStrAttr : conditionAttributeList){
					if(!allAttributeList.contains(condStrAttr)){
						allAttributeList.add(condStrAttr);
					}
				}
			}

		//Check if a selected attribute or conditional attribute in the table
		for(String attrName : allAttributeList){
			boolean containsAttr = false;
			for(String tableName : tableList.keySet()){
				Table table = tableList.get(tableName);
				if(table.getAttrPos(attrName) != -1){
					containsAttr = true;
				}
			}
			if(containsAttr == false){
				throw new Error("SELECT: Attribute " + attrName + " does not exists");
			}
		}

		//Start joining multiple tables to a single table that depends on all attributes needs to be in the new table
		TuplesWithNameTable combinedTable = combineTables(tableArrayList, tupleHashTable, allAttributeList, query.isSelectAll());

		//Evaluate condition
		if(selectCond != null){
			combinedTable = getTuplesBySelectedCond(selectCond, combinedTable);
		}
		
		//Obtain selected values tuples
		TuplesWithNameTable selectedValuesTable = null;

		if(!query.isSelectAll()){
			selectedValuesTable = this.getTuplesBySelectedValue(attrStrList, combinedTable);	
		}else{
			selectedValuesTable = combinedTable;
		}
		printTable(selectedValuesTable);
	}


	public void delete(Delete query) throws IOException, Error, ClassNotFoundException{
		Hashtable<String, Table> tables = null;
		Condition cond = query.getCondition();
		ArrayList< ArrayList<Value> > tupleList; 
		Table table;
		File tableFile = new File(databaseDefUrl);
		if(tableFile.exists()){
			tables = this.getTableDef();
		}else{
			throw new Error("DELETE: No database defined");  
		}
		if((table = tables.get(query.getTableName()) )!= null ){
			String tableName = table.getTableName();
			File tupleFile = new File(tableName + ".db");
			if(!tupleFile.exists()){
				throw new Error("DELETE: No tuple in the table " + tableName); 
			}else{
				tupleList = this.getTupleList(tupleFile);
			}
			int originalTupleListSize = tupleList.size();

			tupleList = this.removeTuplesByCond(cond, new TuplesWithNameTable(table.getAttrPosHashtable(), tupleList) );
			int deletedTupleListSize = tupleList.size();
			int tuplesDeleted = originalTupleListSize - deletedTupleListSize;

			//Save deleted tupleList
			this.saveTupleList(tupleFile, tupleList);
			System.out.println(tuplesDeleted + " rows affected");

		}
	}

	public void update(Update query) throws IOException, Error, ClassNotFoundException{
		Condition cond = query.getCondition();
		ArrayList< ArrayList<Value> > tupleList; 
		ArrayList<AttrAssign> attrAssignList = query.getAttrAssignList();
		Hashtable<String, Table> tables = null;
		Table table;
		File tableFile = new File(databaseDefUrl);
		if(tableFile.exists()){
			tables = this.getTableDef();
		}else{
			throw new Error("UPDATE: No database defined");  
		}

		if((table = tables.get(query.getTableName()) )!= null ){
			String tableName = table.getTableName();
			File tupleFile = new File(tableName + ".db");
			if(!tupleFile.exists()){
				throw new Error("UPDATE: No tuple in the table " + tableName); 
			}else{
				tupleList = this.getTupleList(tupleFile);
				if(tupleList.size() == 0){
					throw new Error("UPDATE: No tuple in the table " + tableName); 
				}
			}

			Hashtable<String, Integer> nameTable = table.getAttrPosHashtable();
			//Check if all attributes needed are in this table throw Error if invalid
			this.checkUpdateAttributesExist(nameTable, cond, attrAssignList);

			//Evaluate check constraints of updating values
			ArrayList<Attribute> attrList = table.getAttrList();
			for(AttrAssign attrAssign : attrAssignList){
				int attrPos = table.getAttrPos(attrAssign.getAttrName());
				Condition checkCond = attrList.get(attrPos).getCheckCond();
				Value assignValue = attrAssign.getValue();
				if(checkCond != null){
					Exp exp = checkCond.getExp();

					Object ret = exp.accept(this, assignValue);
					
					if(ret instanceof Boolean){
						if(((Boolean) ret).booleanValue() == false){
							throw new Error("UPDATE: Attribute " + attrAssign.getAttrName() + " Check constraints violated");
						}
					}else{
						//throw new Error("UPDATE: Check constraints evaluation failed");
					}
				}
			}
		
			//Update tuple list
			TuplesWithNameTable updatedTable = this.updateTuplesByCond(cond, attrAssignList, table.getPrimaryList(), new TuplesWithNameTable(nameTable, tupleList) );
			int updatedTuplesNum = updatedTable.getUpdatedTuplesNum();
			ArrayList<ArrayList<Value>> updatedTupleList = updatedTable.getTupleList();
			
			this.saveTupleList(tupleFile, updatedTupleList);
			System.out.println( updatedTuplesNum + " rows affected");

		}

	}


	public void help(Help query) throws IOException, Error, ClassNotFoundException{
		Help.HelpType helpType = query.getHelpType();

		switch(helpType){
			case DESCRIBE:

			break;

			case CREATE:
	
			break;

			case INSERT:

			break;

			case DELETE:

			break;

			case DROP:

			break;

			case UPDATE:
	
			break;

			case SELECT:

			break;
			
			case TABLES:

			break;
		}


	}

	private void checkUpdateAttributesExist(Hashtable<String, Integer> nameTable, Condition cond, ArrayList<AttrAssign> attrAssignList){

		ArrayList<String> condAttrStrList = null;
		ArrayList<String> attrAssignStrList = new ArrayList<String>();

		//Get attributes name in a condition if not null
		if(cond != null){
			condAttrStrList = cond.getIdList();
		}
		
		//Get attributes names in the assign attribute list
		for(AttrAssign attrAssign : attrAssignList){
			attrAssignStrList.add(attrAssign.getAttrName());
		}
	
		if(condAttrStrList != null){
				//Add condition attributes into assign attribute list if not added yet
				for(String condStrAttr : condAttrStrList){
					if(!attrAssignStrList.contains(condStrAttr)){
						attrAssignStrList.add(condStrAttr);
					}
				}
		}

		//Check if a selected attribute or conditional attribute in the table
		for(String attrName : attrAssignStrList){
			boolean containsAttr = false;
			for(String name : nameTable.keySet()){
				if(name.equals(attrName)){
					containsAttr = true;
				}
			}
			if(containsAttr == false){
				throw new Error("UPDATE: Attribute " + attrName + " does not exists");
			}
		}

	}

	private void printTable(TuplesWithNameTable tuplesTable){
		ArrayList<ArrayList<Value>> tupleList = tuplesTable.getTupleList();
		Hashtable<String, Integer> nameTable = tuplesTable.getNameTable();
		if(tupleList.size()== 0){
			throw new Error("No tuple selected");
		}
		String[] orderedAttrNames = new String[nameTable.size()];

		//Get ordered attribute names
		for(String attrName : nameTable.keySet()){
			orderedAttrNames[nameTable.get(attrName).intValue()] = attrName;
		}

		//Print attribute names
		for(int i = 0; i < orderedAttrNames.length; i++){
			System.out.print(orderedAttrNames[i] + "\t");
		}
		System.out.println();

		for(ArrayList<Value> tuple : tupleList){
			for(Value value : tuple){
				System.out.print(value.toString() + "\t");
			}
			System.out.println();
		}

		System.out.println(tupleList.size() + " tuples selected");
	}


	private TuplesWithNameTable combineTables(ArrayList<Table> tables, Hashtable<String, ArrayList< ArrayList<Value> > > tupleHashtable, ArrayList<String> allAttributes, boolean selectAll){

		ArrayList<ArrayList<Value>> combinedTupleList = new ArrayList<ArrayList<Value>>();
		Hashtable<String, Integer> combinedAttrNameList = new Hashtable<String, Integer>();		
 
		LinkedList<TuplesWithNameTable> allTables = new LinkedList<TuplesWithNameTable>();

		for(Table table : tables){
			ArrayList< ArrayList<Value> > tupleList = tupleHashtable.get(table.getTableName());

			//Get a table that contains all values needed
			TuplesWithNameTable neededValueTable = null;
			if(!selectAll){
				neededValueTable = this.getNeededValuesTuples(table, tupleList, allAttributes);
			}else{
				neededValueTable = new TuplesWithNameTable(table.getAttrPosHashtable(), tupleList);
			}
			allTables.add(neededValueTable);
		}


		return cartesianProduct(allTables);
	}

	private TuplesWithNameTable cartesianProduct(LinkedList<TuplesWithNameTable> allTables){
		//LinkedList<TuplesWithNameTable> cloneAllTables = new LinkedList<TuplesWithNameTable>(allTables);

		while(allTables.size() >= 2){
			TuplesWithNameTable combinedTable = _cartesianProduct(allTables.get(0), allTables.get(1));
			allTables.removeFirst();
			allTables.removeFirst();
			allTables.addFirst(combinedTable);
		}
		
		return allTables.get(0);
	}

	private TuplesWithNameTable _cartesianProduct(TuplesWithNameTable table1, TuplesWithNameTable table2){
			Hashtable<String, Integer> nameTable;
			ArrayList< ArrayList<Value> > tupleList;

			nameTable = new Hashtable<String, Integer>(table1.getNameTable());
			tupleList = new ArrayList<ArrayList<Value>>();

			int table1Size = nameTable.size();
			Hashtable<String, Integer> table2NameTable = table2.getNameTable();

			//Update name table position
			for(String key : table2NameTable.keySet()){
				nameTable.put(key, table2NameTable.get(key) + table1Size);
			}

			//Product table1 with table2
			for(ArrayList<Value> tuple1 : table1.getTupleList()){
				
				for(ArrayList<Value> tuple2 : table2.getTupleList()){
					ArrayList<Value> combinedTuple = new ArrayList<Value>(tuple1);
					combinedTuple.addAll(tuple2);
					tupleList.add(combinedTuple);
				}
			}

			return new TuplesWithNameTable(nameTable, tupleList);

	}

	private TuplesWithNameTable getNeededValuesTuples(Table table, ArrayList< ArrayList<Value> > tuples, ArrayList<String> allAttributes){

			ArrayList<ArrayList<Value>> newTupleList = new ArrayList<ArrayList<Value>>();
			Hashtable<String, Integer> newAttrNamePos = new Hashtable<String, Integer>();	
			ArrayList<Integer> neededAttrPos = new ArrayList<Integer>();

			//Save all attributes positions needed
			for(String attrName : allAttributes){
				int attrPos;
				if( (attrPos = table.getAttrPos(attrName)) != -1){
					newAttrNamePos.put(attrName, neededAttrPos.size());
					neededAttrPos.add(attrPos);
				}
			}


			//Save all needed values in each tuple
			for(ArrayList<Value> tuple : tuples){
				ArrayList<Value> newTuple = new ArrayList<Value>();
				for(Integer valuePos : neededAttrPos){
					newTuple.add(tuple.get(valuePos));
				}
				newTupleList.add(newTuple);
			}

			return new TuplesWithNameTable(newAttrNamePos, newTupleList);
	}


	public void drop(Drop query) throws IOException, Error, ClassNotFoundException{
		Hashtable<String, Table> tables = null;
		File tableFile = new File(databaseDefUrl);
		if(tableFile.exists()){
			tables = this.getTableDef();
		}else{
			throw new Error("DROP TABLE: No Database Defined");
		}
		if(tables.get(query.getTableName()) == null){
			throw new Error("DROP TABLE: No Table " + query.getTableName() + " Found");
		}else{
			tables.remove(query.getTableName());
			this.writeTableDef(tableFile, tables);
			File databaseFile = new File(query.getTableName() + ".db");
			if(databaseFile.exists()){
				databaseFile.delete();
			}
			System.out.println("Table dropped successfully");
		}

	}

	private ArrayList<Value> convertInsertValuesType(Table tableDef, ArrayList<String> values) throws Error{
		ArrayList<Value> valueList = new ArrayList<Value>();
		ArrayList<Attribute> attrList = tableDef.getAttrList();
		String tableName = tableDef.getTableName();
		int attrSize = attrList.size();

		if(attrSize != values.size()){
			throw new Error("INSERT: The Number Of Values Not Match, Table " + tableName + " Has " + attrList.size() + " Values");
		}

		for(int i = 0; i < attrSize; i++){
			Attribute attribute = attrList.get(i);
			String strValue = values.get(i);
			try{
				Attribute.Type type = attribute.getType();
				
					if( type == Attribute.Type.INT ){
						int intValue = Integer.parseInt(strValue);
						Value value = new Value(intValue);
						valueList.add(value);
					}
					else if(type == Attribute.Type.CHAR){
						//Check type and length
						if( (attribute.getLength() + 2 < strValue.length()) || strValue.charAt(0) != '"' && strValue.charAt(0) != '\'' ){
							throw new NumberFormatException();
						}
						Value charValue = new Value(strValue);
						valueList.add(charValue);
					}
					
					else if(type == Attribute.Type.DECIMAL){
						double doubleVal = Double.parseDouble(strValue);
						Value doubleValue = new Value(doubleVal);
						valueList.add(doubleValue);
					}
			} catch(NumberFormatException ex){
				throw new Error("INSERT: Value " + strValue + " Has Wrong Type Or Length Exceeded");
			}
		}
		
		return valueList;
	}	

	private ArrayList<ArrayList<Value>> getTupleList(File tupleFile)throws IOException, ClassNotFoundException{
		ArrayList<ArrayList<Value>> tupleList = null;
		FileInputStream fileIn = new FileInputStream(tupleFile);
		ObjectInputStream in = new ObjectInputStream(fileIn);
		tupleList = (ArrayList<ArrayList<Value>>) in.readObject();
		in.close();
		fileIn.close();
		
		return tupleList;
	}

	private void saveTupleList(File tupleFile, ArrayList<ArrayList<Value>> tupleList)throws IOException{
		FileOutputStream outFile = new FileOutputStream(tupleFile);
		ObjectOutputStream outObject = new ObjectOutputStream(outFile);
		outObject.writeObject(tupleList);
		outObject.close();
		outFile.close();
	}

	private Hashtable<String, Table> getTableDef() throws IOException, ClassNotFoundException{
		Hashtable<String, Table> tables = null;
		File tableFile = new File(databaseDefUrl);

		FileInputStream fileIn = new FileInputStream(tableFile);
		ObjectInputStream in = new ObjectInputStream(fileIn);
		tables = (Hashtable<String, Table>) in.readObject();
		in.close();
		fileIn.close();

		return tables;
	}

	private void writeTableDef(File tableFile, Hashtable<String, Table> tables) throws IOException {
		FileOutputStream outFile = new FileOutputStream(tableFile);
		ObjectOutputStream outObject = new ObjectOutputStream(outFile);
		outObject.writeObject(tables);
		outObject.close();
		outFile.close();
	}


	private boolean checkPrimarys(ArrayList<Integer> primaryList, ArrayList<ArrayList<Value>> tupleList, ArrayList<Value> newValueList){

		for(ArrayList<Value> tuple : tupleList){
			boolean isCheckCorrect = false;
			for(Integer primaryPos : primaryList){
				if(!newValueList.get(primaryPos).equals(tuple.get(primaryPos))){
					isCheckCorrect = true;
				}
			}
			if(isCheckCorrect == false){
				return false;
			}

		}

		return true;
	}


	//Evaluate condition
	private boolean evaluateConstraintsCond(Table table, ArrayList<Value> valueList){
		ArrayList<Attribute> attrList = table.getAttrList();
		Object ret;

		int count = 0;
		for(Attribute attribute : attrList){
			//System.err.println("Attrlist loop");
			Condition cond = attribute.getCheckCond();
			Value value = valueList.get(count);
			if(cond != null){
				
				Exp exp = cond.getExp();
				ret = exp.accept(this, value);
				if(ret instanceof Boolean){
					if(((Boolean)ret).booleanValue() == false){
						throw new Error("INSERT: Constraints check violated, Attribute " + attribute.getName() + " cannot be inserted into the Table " + table.getTableName()); 
					}
				}else{
					return false;
				}
			}
			count++;
		} 

		return true;

	}
	
	
	private TuplesWithNameTable updateTuplesByCond(Condition cond, ArrayList<AttrAssign> attrAssignList, ArrayList<Integer> PrimaryKeyList, TuplesWithNameTable tuples){
		Hashtable<String, Integer> nameTable = tuples.getNameTable();
		//Tuple list will be modified directly
		ArrayList< ArrayList<Value> > tupleList = tuples.getTupleList();
			

		//Check primary key list
		//NOT FINISHED!

		ArrayList<Value> firstTuple = tupleList.get(0);

		//Check update values type
		for(AttrAssign attrAssign : attrAssignList){
			Value updatingValue = attrAssign.getValue();
			String valueName = attrAssign.getAttrName();
			Integer valuePos = nameTable.get(valueName);
			
			Value currentValue = firstTuple.get(valuePos.intValue());
			if(currentValue.getType() != updatingValue.getType()){
				throw new Error("UPDATE: Updating Value " + valueName + " has invalid type");
			}
		}
		
		int updatedValueNum = 0;
		//If no update condition, update all values
		if(cond == null){
			for(ArrayList<Value> tuple : tupleList){
				for(AttrAssign attrAssign : attrAssignList){
					String attrName = attrAssign.getAttrName();
					Integer attrPos = nameTable.get(attrName);

					tuple.set(attrPos.intValue(), attrAssign.getValue());
					updatedValueNum++;
				}
			}
		}else{
			//Evaluate condition and update tuples that satisfies the condition
			Exp exp = cond.getExp();
			Object ret;

			for(ArrayList<Value> tuple : tupleList){
				ret = exp.accept(this, nameTable, tuple);
				
				if(ret instanceof Boolean){
					if( ((Boolean) ret).booleanValue() == true ){
						for(AttrAssign attrAssign : attrAssignList){
							String attrName = attrAssign.getAttrName();
							Value updatedValue = attrAssign.getValue();
							
							int namePos = nameTable.get(attrName).intValue();
							tuple.set(namePos, updatedValue);
							updatedValueNum++;
						}
					}
				}
			}
			
		}

		TuplesWithNameTable tupleTable = new TuplesWithNameTable(nameTable, tupleList);
		tupleTable.setUpdatedTuplesNum(updatedValueNum);

		return tupleTable;

	}

	private ArrayList<ArrayList<Value> > removeTuplesByCond(Condition cond, TuplesWithNameTable tuples){
		Hashtable<String, Integer> nameTable = tuples.getNameTable();
		ArrayList< ArrayList<Value> > tupleList = tuples.getTupleList();

		//If no condition, just remove all attributes
		if(cond == null){
			tupleList.clear();
			return tupleList;
		}

		Exp exp = cond.getExp();
		Object retBool;
		ArrayList<ArrayList<Value> > deletedTupleList = new ArrayList< ArrayList<Value> > ();
		for(ArrayList<Value> tuple : tupleList){
			retBool = exp.accept(this, nameTable, tuple);
			if(retBool instanceof Boolean){
				if( ((Boolean) retBool).booleanValue() == true){
					deletedTupleList.add(tuple);
				}
			}else{
				throw new Error("DELETE: Tuple delete condition evaluation failed");
			}
		}

		tupleList.removeAll(deletedTupleList);
		return tupleList;

	}

	private TuplesWithNameTable getTuplesBySelectedValue(ArrayList<String> selectedList, TuplesWithNameTable tuples){
		Hashtable<String, Integer> nameTable = tuples.getNameTable();
		Hashtable<String, Integer> newNameTable = new Hashtable<String, Integer>();

		ArrayList< ArrayList<Value> > tupleList = tuples.getTupleList();
		ArrayList< ArrayList<Value> > newTupleList = new ArrayList< ArrayList<Value> >();

		int nameCount = 0;		
		for(String selectedValue : selectedList){
			newNameTable.put(selectedValue, nameCount);
			nameCount++;
		}

		for(ArrayList<Value> tuple : tupleList){
			ArrayList<Value> newTuple = new ArrayList<Value>();
			for(String selectedValue : selectedList){
				newTuple.add(tuple.get( nameTable.get(selectedValue).intValue() ) );
			}
			newTupleList.add(newTuple);
		}

		return new TuplesWithNameTable(newNameTable, newTupleList);

	}

	private TuplesWithNameTable getTuplesBySelectedCond(Condition cond, TuplesWithNameTable tuples){
		Hashtable<String, Integer> nameTable = tuples.getNameTable();

		ArrayList< ArrayList<Value> > tupleList = tuples.getTupleList();
		ArrayList< ArrayList<Value> > newTupleList = new ArrayList< ArrayList<Value> >();
		
		Exp exp = cond.getExp();
		Object retBool;

		for(ArrayList<Value> tuple : tupleList){
			retBool = exp.accept(this, nameTable, tuple);
			if(retBool instanceof Boolean){
				if( ((Boolean) retBool).booleanValue() == true){
					newTupleList.add(tuple);
				}
			}else{
				throw new Error("SELECT: Tuple select condition evaluation failed");
			}

		}
		return new TuplesWithNameTable(nameTable, newTupleList);
	}

	public Object visit(BinaryExp exp, Value value, Hashtable<String, Integer> attrPosTable, ArrayList<Value> tuple){
		//System.err.println("Enter into BinaryExp ");//
		String op = exp.getOp();
		Object ret = null;

		if(exp == null){
			return Boolean.valueOf(true);
		}

		Exp left = exp.getLeft();
		Exp right = exp.getRight();

		Object leftOp = null;
		Object rightOp = null;

		if(left != null){
			//System.err.println("Left not null " + op);//
			if(tuple == null){
				leftOp = left.accept(this, value);
			}else{
				leftOp = left.accept(this, attrPosTable, tuple);
			}
			//System.err.println("Left visited ");//
		}

		if(right != null){
			//System.err.println("Right not null " + op);//
			if(tuple == null){
				rightOp = right.accept(this, value);
			}else{
				rightOp = right.accept(this, attrPosTable, tuple);
			}
		}

		if( ( (leftOp instanceof Integer) || (leftOp instanceof Double) ) && ( (rightOp instanceof Integer) || (rightOp instanceof Double) )){
			
			 double l, r;
			if(leftOp instanceof Integer){
				l = (double)((Integer) leftOp).intValue();
			}else{
				 l = ((Double) leftOp).doubleValue();
			}
			
			if(rightOp instanceof Integer){
				r = (double)((Integer) rightOp).intValue();
			}else{
				 r = ((Double) rightOp).doubleValue();
			}

			 if (op.equals("<")) {
                		ret = (l < r);
           		 } else if (op.equals("<=")) {
                		ret = (l <= r);
            		} else if (op.equals("=")) {
               			ret = (l == r);
            		} else if (op.equals("!=")) {
                		ret = (l != r);
            		} else if (op.equals(">")) {
                		ret = (l > r);
            		} else if (op.equals(">=")) {
                		ret = (l >= r);
            		} else if (op.equals("+")) {
                		ret = (l + r);
            		} else if (op.equals("-")) {
                		ret = (l - r);
            		} else if (op.equals("*")) {
                		ret = (l * r);
            		} else if (op.equals("/")) {
                		ret = (l / r);
            		}else{
                		throw new Error("Implement BinaryExp for " + op);
			}

        		return ret;
		}

		if((leftOp instanceof String) && (rightOp instanceof String)){
			if(op.equals("=")){
				if( ( (String)leftOp).equals((String) rightOp)){
					return Boolean.valueOf(true);
				}else{
					return Boolean.valueOf(false);
				}
			}else if (op.equals("!=")){
				if( ((String)leftOp).equals((String) rightOp)){
					return Boolean.valueOf(false);
				}else{
					return Boolean.valueOf(true);
				}
			}else{
				throw new Error("Condition error: String can only compare with \"=\" or \"!=\" operator"); 
			}
		}

		if( (leftOp instanceof Boolean) && (rightOp instanceof Boolean)){
			Boolean boolRet = false;
			if(op.toUpperCase().equals("AND")){
				boolRet = ((Boolean)leftOp).booleanValue() && ((Boolean)rightOp).booleanValue();
			}

			if(op.toUpperCase().equals("OR")){
				boolRet = ((Boolean)leftOp).booleanValue() || ((Boolean)rightOp).booleanValue();
			}
			return boolRet;
		}

		return leftOp;
		
	}

	public Object visit(IntExp exp, Value value){
		//System.err.println("Enter into intExp ");
		return Integer.valueOf(exp.getInt());
	}

	public Object visit(DoubleExp exp, Value value){
		//System.err.println("Enter into doubleExp ");
		return Double.valueOf(exp.getDouble());
	}

	public Object visit(StrExp exp, Value value){
		//System.err.println("Enter into StrExp ");
		return exp.getString();
	}

	
	public Object visit(IdExp exp, Value value){
		//System.err.println("Enter into idExp");
		if(value.getType() == Attribute.Type.INT){
			return Integer.valueOf(value.getInt());
		}else if(value.getType() == Attribute.Type.DECIMAL){
			return Double.valueOf(value.getDouble());
		}else if(value.getType() == Attribute.Type.CHAR){
			return value.getChar();
		}else{
			throw new Error("IdExp error");
		}
	}

	public Object visit(IdExp exp, Hashtable<String, Integer> attrPosTable, ArrayList<Value> tuple){
		String attrName = exp.getId();		
		Value value = tuple.get(attrPosTable.get(attrName).intValue());

		return visit(exp, value);
	}

	public Object visit(Exp exp,  Hashtable<String, Integer> attrPosTable, ArrayList<Value> tuple){

		if(exp instanceof BinaryExp){
			//System.err.println("Enter into visit binary");
			return ((BinaryExp) exp).accept(this, null, attrPosTable, tuple);
		}else if(exp instanceof StrExp){
			//System.err.println("Enter into visit str");
			return ((StrExp) exp).accept(this, null);
		}else if(exp instanceof IdExp){
			return ((IdExp) exp).accept(this, attrPosTable, tuple);
		}else if(exp instanceof DoubleExp){
			return ((DoubleExp) exp).accept(this, null);
		}else if(exp instanceof IntExp){
			//System.err.println("Enter into visit int");
			return ((IntExp) exp).accept(this, null);
		}else{
			return Boolean.valueOf(true);
		}

	}

	public Object visit(Exp exp, Value value){
		
		if(exp instanceof BinaryExp){
			//System.err.println("Enter into visit binary");
			return ((BinaryExp) exp).accept(this, value, null, null);
		}else if(exp instanceof StrExp){
			//System.err.println("Enter into visit str");
			return ((StrExp) exp).accept(this, value);
		}else if(exp instanceof IdExp){
			return ((IdExp) exp).accept(this, value);
		}else if(exp instanceof DoubleExp){
			return ((DoubleExp) exp).accept(this, value);
		}else if(exp instanceof IntExp){
			//System.err.println("Enter into visit int");
			return ((IntExp) exp).accept(this, value);
		}else{
			return Boolean.valueOf(true);
		}
	}

}
