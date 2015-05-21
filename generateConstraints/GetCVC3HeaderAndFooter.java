package generateConstraints;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

import parsing.Column;
import parsing.Conjunct;
import parsing.Node;
import parsing.Table;
import testDataGen.DataType;
import testDataGen.GenerateCVC1;
import testDataGen.QueryBlockDetails;
import util.Configuration;
import util.Utilities;

/**
 * FIXME: Add GOOD DOC FOR THIS
 * @author mahesh
 *
 */
public class GetCVC3HeaderAndFooter {

	/*
	 * Since we have commented the generation of modified query, we have also not considered generating ctid values.
	 * Hence we also comment the function generateCVC3_Header which uses the number of ctid values for the count of the total records of that table.
	 * We then create a new function as below which just adds the different values to the input database.
	 * Also, we are not considering the input database. Hence there is no need for the input tuples and its range.
	 */



	/**
	 * Generates CVC Data Type for each column
	 * First gets the name of equivalence columns from the file equivalenceColumns in the path  
	 */
	public static String  generateCVC3_Header( GenerateCVC1 cvc) throws Exception{
		DataType dt = new DataType();

		String cvc_tuple = "", tableName="";
		String tempStr = "";

		HashMap<Vector<String>,Boolean> equivalenceColumns = new HashMap<Vector<String>, Boolean>();
		
		String equivalenceColumnsFile=Configuration.homeDir+"/temp_cvc" +cvc.getFilePath() + "/equivalenceColumns";
		
		File f = new File(equivalenceColumnsFile);
		f.createNewFile(); //this will create the equivalence columns file iff it is not already present
		
		
		BufferedReader readEqCols =  new BufferedReader(new FileReader(equivalenceColumnsFile));
		String eqCol = "";

		while((eqCol = readEqCols.readLine()) != null){
			Vector<String> vecEqCol = new Vector<String>();
			String[] t;
			t = eqCol.split(" ");
			for(int i=0;i<t.length;i++){
				vecEqCol.add(t[i]);
			}
			equivalenceColumns.put(vecEqCol, false);
		}

		for(int i=1; i < cvc.getResultsetColumns().size(); i++){
			Column column = cvc.getResultsetColumns().get(i);
			String columnName = column.getColumnName();

			int columnType = 0;
			String cvc_datatype = "";

			Vector<String> columnValue = column.getColumnValues();
			//			System.out.println("column.getDataType() " + column.getDataType());
			columnType = dt.getDataType(column.getDataType());
			cvc_tuple += columnName+", ";

			boolean columnEquivalentAlreadyAdded = false;

			Iterator<Vector<String>> itr = equivalenceColumns.keySet().iterator();
			Vector<String> vs = null;
			while(itr.hasNext()){
				vs = itr.next();
				if(vs.contains(columnName) && equivalenceColumns.get(vs)){
					columnEquivalentAlreadyAdded = true;
					break;
				}
				else if(vs.contains(columnName)){
					columnEquivalentAlreadyAdded = false;
					equivalenceColumns.put(vs, true);
					break;
				}
			}
			String equivalentColumn = "";
			if(columnEquivalentAlreadyAdded == true){
				for(int j=0;j<vs.size();j++){
					for(int k=0; k < cvc.getDatatypeColumns().size();k++){
						if(vs.get(j).equalsIgnoreCase( cvc.getDatatypeColumns().get(k))){
							equivalentColumn = vs.get(j);
							cvc_datatype = columnName + " : TYPE = " + equivalentColumn + ";";
							column.setCvcDatatype(columnName);//Correct? test it
							columnType = -1;//for skipping the switch statement
							break;
						}
					}
					//					if(columnType == -1){//Probably the previous break is not sufficient. Need to break from both loops.
					//						break;
					//					}
				}
			}

			String isNullMembers = "";
			switch(columnType){//FIXME: ADD CORRECT CODE FOR MIN AND MAXIMUM VALUES OF COLUMNS
								//This approach cause problems in some cases
			case 1://case 2:
			{
				int min, max;
				min = max = 0;
				boolean limitsDefined = false;
				if(columnValue.size()>0){
					for(int j=0; j<columnValue.size(); j++){
						int colValue = (int)Float.parseFloat((String)columnValue.get(j));


						//Commented out by Biplab
						//								isNullMembers += "ASSERT NOT ISNULL_"+columnName+"("+colValue+");\n";


						if(!limitsDefined){
							min = colValue;
							max = colValue;
							limitsDefined = true;
							continue;
						}
						if(min > colValue)
							min = colValue;
						if(max < colValue)
							max = colValue;
					}
				}
				//Adding support for NULLs
				//Consider the range -99996 to -99999 as NULL integers


				//						cvc_datatype = "\n"+columnName+" : TYPE = SUBTYPE (LAMBDA (x: INT) : (x > "+(min-1)+" AND x < "+(max+1)+") OR (x > -100000 AND x < -99995));\n";
				//Modified by Biplab. Original code commented out above
				if(min > 0)
					cvc_datatype = "\n"+columnName+" : TYPE = SUBTYPE (LAMBDA (x: INT) : (x > 0) OR (x > -100000 AND x < -99995));\n";
				else
					cvc_datatype = "\n"+columnName+" : TYPE = SUBTYPE (LAMBDA (x: INT) : (x > "+(min-1)+") OR (x > -100000 AND x < -99995));\n";


				cvc_datatype += "ISNULL_" + columnName +" : "+ columnName + " -> BOOLEAN;\n";
				HashMap<String, Integer> nullValuesInt = new HashMap<String, Integer>();
				for(int k=-99996;k>=-99999;k--){
					isNullMembers += "ASSERT ISNULL_"+columnName+"("+k+");\n";
					nullValuesInt.put(k+"",0);
				}
				cvc.getColNullValuesMap().put(column, nullValuesInt);
				cvc_datatype += isNullMembers;
				column.setMinVal(min);
				column.setMaxVal(max);
				column.setCvcDatatype("INT");
				break;
			}
			case 2:
			{
				double min, max;
				min = max = 0;
				boolean limitsDefined = false;
				if(columnValue.size()>0){
					for(int j=0; j<columnValue.size(); j++){
						double colValue = (Double)Double.parseDouble((String)columnValue.get(j));
						String strColValue=util.Utilities.covertDecimalToFraction(colValue+"");


						//Commented out by Biplab
						//							isNullMembers += "ASSERT NOT ISNULL_"+columnName+"("+strColValue+");\n";


						if(!limitsDefined){
							min = colValue;
							max = colValue;
							limitsDefined = true;
							continue;
						}
						if(min > colValue)
							min = colValue;
						if(max < colValue)
							max = colValue;
					}
				}
				//Adding support for NULLs
				//Consider the range -99996 to -99999 as NULL integers
				String maxStr=util.Utilities.covertDecimalToFraction(max+"");
				String minStr=util.Utilities.covertDecimalToFraction(min+"");


									cvc_datatype = "\n"+columnName+" : TYPE = SUBTYPE (LAMBDA (x: REAL) : (x >= "+(minStr)+" AND x <= "+(maxStr)+") OR (x > -100000 AND x < -99995));\n";
				//Modified by Biplab. Original code commented out above
				/*if(min > 0)
					cvc_datatype = "\n"+columnName+" : TYPE = SUBTYPE (LAMBDA (x: REAL) : (x > 0) OR (x > -100000 AND x < -99995));\n";
				else
					cvc_datatype = "\n"+columnName+" : TYPE = SUBTYPE (LAMBDA (x: INT) : (x >= "+minStr+") OR (x > -100000 AND x < -99995));\n";

				*/
				cvc_datatype += "ISNULL_" + columnName +" : "+ columnName + " -> BOOLEAN;\n";
				HashMap<String, Integer> nullValuesInt = new HashMap<String, Integer>();
				for(int k=-99996;k>=-99999;k--){
					isNullMembers += "ASSERT ISNULL_"+columnName+"("+k+");\n";
					nullValuesInt.put(k+"",0);
				}
				cvc.getColNullValuesMap().put(column, nullValuesInt);
				cvc_datatype += isNullMembers;
				column.setMinVal(min);
				column.setMaxVal(max);
				column.setCvcDatatype("REAL");
				break;
			}
			case 3:
			{
				String colValue = "";
				///This part is used if strings are to be encoded as integer types
				/*cvc_datatype = "\n"+columnName+" : TYPE = SUBTYPE (LAMBDA (x: INT) : (x > "+0+" AND x<"+column.getColumnValues().size()+") OR (x > -100000 AND x < -99995));\n";
					cvc_datatype += "ISNULL_" + columnName +" : "+ columnName + " -> BOOLEAN;\n";
					HashMap<String, Integer> nullValuesChar = new HashMap<String, Integer>();

					for(int k=0;k<column.getColumnValues().size();k++)
						isNullMembers += "ASSERT NOT ISNULL_"+columnName+"("+k+");\n";

					for(int k=-99996;k>=-99999;k--){
						isNullMembers += "ASSERT ISNULL_"+columnName+"("+k+");\n";
						nullValuesChar.put(k+"",0);
					}
					colNullValuesMap.put(column, nullValuesChar);
					cvc_datatype += isNullMembers;

					column.setCvcDatatype(columnName);
				 */

				cvc_datatype = "\nDATATYPE \n"+columnName+" = ";

				if(columnValue.size()>0){
					//colValue = ((String)columnValue.get(0)).trim().replaceAll("[\\p{Punct}]+", " ").replaceAll("[\\s]+", " ").replaceAll(" ", "_");
					colValue =  columnName+"__"+Utilities.escapeCharacters(columnValue.get(0).trim());
					//						if(columnName.equals("DEPT_NAME"))
					//							System.out.println("DEPT_NAME values = " + columnValue.get(0).trim() + " " + "_"+colValue);
					cvc_datatype += "_"+colValue;
					isNullMembers += "ASSERT NOT ISNULL_"+columnName+"(_"+colValue+");\n";
					//						System.out.println(isNullMembers);


					//						for(int j=1; j<columnValue.size(); j++){
					//							//colValue = ((String)columnValue.get(j)).trim().replaceAll("[\\p{Punct}]+", " ").replaceAll("[\\s]+", " ").replaceAll(" ", "_");
					//							colValue =  Utilities.escapeCharacters(columnName)+"__"+Utilities.escapeCharacters(columnValue.get(j));
					//							cvc_datatype = cvc_datatype+" | "+"_"+colValue;
					//							isNullMembers += "ASSERT NOT ISNULL_"+columnName+"(_"+colValue+");\n";
					//						}
					//Modified by Biplab. Original code commented out above
					for(int j=1; j<columnValue.size() || j < 4; j++){
						if(j<columnValue.size())
						{
							//colValue = ((String)columnValue.get(j)).trim().replaceAll("[\\p{Punct}]+", " ").replaceAll("[\\s]+", " ").replaceAll(" ", "_");
							colValue =  Utilities.escapeCharacters(columnName)+"__"+Utilities.escapeCharacters(columnValue.get(j));
						}
						else
							colValue =  Utilities.escapeCharacters(columnName)+"__"+j;
						cvc_datatype = cvc_datatype+" | "+"_"+colValue;
						isNullMembers += "ASSERT NOT ISNULL_"+columnName+"(_"+colValue+");\n";
					}


				}
				//Adding support for NULLs
				if(columnValue.size()!=0){
					cvc_datatype += " | ";
				}
				for(int k=1;k<=4;k++){
					cvc_datatype += "NULL_"+columnName+"_"+k;
					if(k < 4){
						cvc_datatype += " | ";
					}
				}						
				cvc_datatype = cvc_datatype+" END\n;";

				//Adding function for testing NULL
				cvc_datatype += "ISNULL_" + columnName +" : "+ columnName + " -> BOOLEAN;\n";
				HashMap<String, Integer> nullValuesChar = new HashMap<String, Integer>();
				for(int k=1;k<=4;k++){
					isNullMembers += "ASSERT ISNULL_" + columnName+"(NULL_"+columnName+"_"+k+");\n";
					nullValuesChar.put("NULL_"+columnName+"_"+k, 0);
				}						
				cvc.getColNullValuesMap().put(column, nullValuesChar);
				cvc_datatype += isNullMembers;
				column.setCvcDatatype(columnName);

				break;
			}
			case 5: //DATE
			{
				long min, max;
				min = max = 0;
				boolean limitsDefined = false;
				if(columnValue.size()>0){
					for(int j=0; j<columnValue.size(); j++){
						java.sql.Date t= java.sql.Date.valueOf(columnValue.get(j));

						long colValue = t.getTime()/86400000;

						isNullMembers += "ASSERT NOT ISNULL_"+columnName+"("+colValue+");\n";
						if(!limitsDefined){
							min = colValue;
							max = colValue;
							limitsDefined = true;
							continue;
						}
						if(min > colValue)
							min = colValue;
						if(max < colValue)
							max = colValue;
					}
				}
				//Adding support for NULLs
				//Consider the range -99996 to -99999 as NULL integers
				cvc_datatype = "\n"+columnName+" : TYPE = SUBTYPE (LAMBDA (x: INT) : (x > "+(min-1)+" AND x < "+(max+1)+") OR (x > -100000 AND x < -99995));\n";
				cvc_datatype += "ISNULL_" + columnName +" : "+ columnName + " -> BOOLEAN;\n";
				HashMap<String, Integer> nullValuesInt = new HashMap<String, Integer>();
				for(int k=-99996;k>=-99999;k--){
					isNullMembers += "ASSERT ISNULL_"+columnName+"("+k+");\n";
					nullValuesInt.put(k+"",0);
				}
				cvc.getColNullValuesMap().put(column, nullValuesInt);
				cvc_datatype += isNullMembers;
				column.setMinVal(min);
				column.setMaxVal(max);
				column.setCvcDatatype("DATE");
				break;
			}
			case 6: //TIME
			{
				long min, max;
				min = max = 0;
				boolean limitsDefined = false;
				if(columnValue.size()>0){
					for(int j=0; j<columnValue.size(); j++){
						Time t= Time.valueOf(columnValue.get(j));

						long colValue = (t.getTime()%86400000)/1000;

						isNullMembers += "ASSERT NOT ISNULL_"+columnName+"("+colValue+");\n";
						if(!limitsDefined){
							min = colValue;
							max = colValue;
							limitsDefined = true;
							continue;
						}
						if(min > colValue)
							min = colValue;
						if(max < colValue)
							max = colValue;
					}
				}
				//Adding support for NULLs
				//Consider the range -99996 to -99999 as NULL integers
				cvc_datatype = "\n"+columnName+" : TYPE = SUBTYPE (LAMBDA (x: INT) : (x > "+(min-1)+" AND x < "+(max+1)+") OR (x > -100000 AND x < -99995));\n";
				cvc_datatype += "ISNULL_" + columnName +" : "+ columnName + " -> BOOLEAN;\n";
				HashMap<String, Integer> nullValuesInt = new HashMap<String, Integer>();
				for(int k=-99996;k>=-99999;k--){
					isNullMembers += "ASSERT ISNULL_"+columnName+"("+k+");\n";
					nullValuesInt.put(k+"",0);
				}
				cvc.getColNullValuesMap().put(column, nullValuesInt);
				cvc_datatype += isNullMembers;
				column.setMinVal(min);
				column.setMaxVal(max);
				column.setCvcDatatype("TIME");
				break;
			}
			case 7: //TIMESTAMP
			{
				long min, max;
				min = max = 0;
				boolean limitsDefined = false;
				if(columnValue.size()>0){
					for(int j=0; j<columnValue.size(); j++){
						Timestamp t= Timestamp.valueOf(columnValue.get(j));

						long colValue = t.getTime()/1000; //converting from milli sec to sec

						isNullMembers += "ASSERT NOT ISNULL_"+columnName+"("+colValue+");\n";
						if(!limitsDefined){
							min = colValue;
							max = colValue;
							limitsDefined = true;
							continue;
						}
						if(min > colValue)
							min = colValue;
						if(max < colValue)
							max = colValue;
					}
				}
				//Adding support for NULLs
				//Consider the range -99996 to -99999 as NULL integers
				cvc_datatype = "\n"+columnName+" : TYPE = SUBTYPE (LAMBDA (x: INT) : (x > "+(min-1)+" AND x < "+(max+1)+") OR (x > -100000 AND x < -99995));\n";
				cvc_datatype += "ISNULL_" + columnName +" : "+ columnName + " -> BOOLEAN;\n";
				HashMap<String, Integer> nullValuesInt = new HashMap<String, Integer>();
				for(int k=-99996;k>=-99999;k--){
					isNullMembers += "ASSERT ISNULL_"+columnName+"("+k+");\n";
					nullValuesInt.put(k+"",0);
				}
				cvc.getColNullValuesMap().put(column, nullValuesInt);
				cvc_datatype += isNullMembers;
				column.setMinVal(min);
				column.setMaxVal(max);
				column.setCvcDatatype("TIMESTAMP");
				break;
			}

			}
			//System.out.println(cvc_datatype);
			if(!cvc.getDatatypeColumns().contains(columnName)){//prevent repetition of data types in cvc file
				cvc.getDatatypeColumns().add(columnName);
				tempStr += cvc_datatype+"\n";
				HashMap<String, Integer> nullValuesChar = new HashMap<String, Integer>();
				for(int k=-99996;k>=-99999;k--){
					isNullMembers += "ASSERT ISNULL_"+columnName+"("+k+");\n";
					nullValuesChar.put(k+"",0);
				}
				//colNullValuesMap.put(column, nullValuesChar);
				cvc_datatype += isNullMembers;
			}
		}


		//Adjust datatypes for strings : In case of foreign keys or equivalence classes the colums may hav different names 
		//Still they must take same values when there is an equality implied
		Vector<Node> tempForeignKeys=new Vector<Node>();
		Vector<Node> tempForeignKeys1=new Vector<Node>();
		for(Node n :cvc.getForeignKeys()){
			if(n.getLeft().getColumn().getCvcDatatype() != null && n.getRight().getColumn().getCvcDatatype() != null && !n.getLeft().getColumn().getCvcDatatype().equals(n.getRight().getColumn().getCvcDatatype())){
				tempForeignKeys.add(n);
			}
		}

		/**FIXME: ISSUES WITH EQUIVALENCE CLASSES */
		Vector<Vector<Node>>  equivalenceClasses = new Vector<Vector<Node>>();
		for(Conjunct con: cvc.getOuterBlock().getConjuncts())
			equivalenceClasses.addAll(con.getEquivalenceClasses());
		
		for(QueryBlockDetails qb: cvc.getOuterBlock().getFromClauseSubQueries())
			for(Conjunct con: qb.getConjuncts())
				equivalenceClasses.addAll(con.getEquivalenceClasses());
		
		for(QueryBlockDetails qb: cvc.getOuterBlock().getWhereClauseSubQueries())
			for(Conjunct con: qb.getConjuncts())
				equivalenceClasses.addAll(con.getEquivalenceClasses());
		
		for(Vector<Node> v: equivalenceClasses){
			//Column c=v.get(0).getColumn();
			String s=v.get(0).getColumn().getCvcDatatype();
			if(s.equals("INT") || s.equals("REAL") || s.equals("TIME") || s.equals("DATE") || s.equals("TIMESTAMP"))
				continue;
			for(int i=0;i<v.size();i++){
				Column c=v.get(i).getColumn();
				if(!c.getCvcDatatype().equals(s))
					c.setCvcDatatype(s);
				for(Node n :tempForeignKeys){
					if(n.getLeft().getColumn().equals(c)){
						n.getRight().getColumn().setCvcDatatype(s);
						tempForeignKeys1.add(n);
					}
					else if(n.getRight().getColumn().equals(c)){
						n.getLeft().getColumn().setCvcDatatype(s);
						tempForeignKeys1.add(n);
					}
				}
				tempForeignKeys.removeAll(tempForeignKeys1);
				tempForeignKeys1=new Vector<Node>();

			}
		}

		for(Node n:tempForeignKeys){
			n.getLeft().getColumn().setCvcDatatype(n.getRight().getColumn().getCvcDatatype());
		}


		///////////////////////


		/*
		 * End building datatypes for all the columns
		 * Now build the tuple types
		 */
		tempStr += "\n%Tuple Types for Relations\n";
		Column c;
		Table t;
		String temp;
		Vector<String> tablesAdded = new Vector<String>();

		for(int i=0;i<cvc.getResultsetTables().size();i++){
			t = cvc.getResultsetTables().get(i);
			temp = t.getTableName();
			if(!tablesAdded.contains(temp)){
				tempStr += temp + "_TupleType: TYPE = [";
			}
			for(int j=0;j<cvc.getResultsetColumns().size();j++){
				c = cvc.getResultsetColumns().get(j);
				if(c.getTableName().equalsIgnoreCase(temp)){
					String s=c.getCvcDatatype();
					if(s.equals("INT") || s.equals("REAL") || s.equals("TIME") || s.equals("DATE") || s.equals("TIMESTAMP"))
						tempStr += c.getColumnName() + ", ";
					else
						tempStr+=c.getCvcDatatype()+", ";
				}
			}
			tempStr = tempStr.substring(0, tempStr.length()-2);
			tempStr += "];\n";
			/*
			 * Now create the Array for this TypleType
			 */
			tempStr += "O_" + temp + ": ARRAY INT OF " + temp + "_TupleType;\n";
		}
		//		tempStr += "\nHello\n";
		//		System.out.println("tempStr " + tempStr);
		//CVC3_HEADER = new String(tempStr);
		//		System.out.println("Hello");
		readEqCols.close();
		return tempStr;
	}

	public static String generateCvc3_Footer() {
		
		String temp="";
		temp += "\n\nQUERY FALSE;";			// need to right generalize one
		//temp += "\nCOUNTEREXAMPLE;";
		temp += "\nCOUNTERMODEL;";
		return temp;
	}
}
