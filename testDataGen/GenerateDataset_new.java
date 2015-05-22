package testDataGen;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Vector;

import util.Configuration;
import util.MyConnection;



/**
 * The main class that is called to get data sets for the query
 * @author mahesh
 *
 */
public class GenerateDataset_new {

	String filePath;

	public GenerateDataset_new(String filePath) throws Exception{		
		this.filePath = filePath;
	}
	
	public void generateDatasetForQuery(String queryId, String orderindependent, String query, String queryDesc) throws Exception {
		this.generateDatasetForQuery(queryId, orderindependent, query, queryDesc, 1);
	}


	public void generateDatasetForQuery(String queryId, String orderindependent, String query, String queryDesc, int assignmentId) throws Exception{


		System.out.println("------------------------------------------------------------------------------------------\n\n");
		System.out.println("QueryID:  "+queryId);
		System.out.println("GENERATING DATASET FOR QUERY: \n" + query);
		System.out.println("------------------------------------------------------------------------------------------\n\n");
		
		
		/** delete previous data sets*/		
		RelatedToPreprocessing.deletePreviousDatasets(this, query);
		
		/**Create object for generating data sets */
		GenerateCVC1 cvc = new GenerateCVC1();
		
		cvc.setFne(false);
		cvc.setIpdb(false);
		cvc.setFilePath(this.getFilePath());
		cvc.initializeConnectionDetails(assignmentId);
		
		/**Call pre processing functions before calling data generation methods */
		PreProcessingActivity.preProcessingActivity(cvc);
		
		cvc.closeConn();
		/** Check the data sets generated for this query */
		ArrayList<String> dataSets = RelatedToPreprocessing.getListOfDataset(this);
		
		System.out.println("\n\n***********************************************************************\n");
		System.out.println("DATA SETS FOR QUERY "+queryId+" ARE GENERATED");
		System.out.println("\n\n***********************************************************************\n");

		/**Update query info table */
		WriteFileAndUploadDatasets.updateQueryInfo(this, queryId, query, queryDesc);
		
		
		/**Upload the data sets into the database */
		WriteFileAndUploadDatasets.uploadDataset(this, queryId, dataSets);			

		System.out.println("\n***********************************************************************\n\n");
		System.out.println("DATASET FOR QUERY "+queryId+" ARE UPLOADED");
		System.out.println("\n***********************************************************************\n\n");
	}

	public void generateDatasetForBranchQuery(String queryId, String[] branchQuery,String[] branchResult, String query, String queryDesc) throws Exception
	{
		System.out.println("------------------------------------------------------------------------------------------\n\n");
		System.out.println("QueryID "+queryId);
		System.out.println("GENERATING DATASET FOR QUERY: \n" + query);
		
		
			
		/** delete previous data sets*/		
		RelatedToPreprocessing.deletePreviousDatasets(this, query);
	
		/**upload the details about the list of queries and the conditions between the branch queries*/
		BufferedWriter ord2 = new BufferedWriter(new FileWriter(Configuration.homeDir+"/temp_cvc"+filePath+"/branchQuery.txt"));
		for(String str : branchQuery)
		{
			ord2.write(str);
			ord2.newLine();
		}
		ord2.close();
		
		BufferedWriter ord3 = new BufferedWriter(new FileWriter(Configuration.homeDir+"/temp_cvc"+filePath+"/branchResult.txt"));
		for(String str : branchResult)
		{
			ord3.write(str);
			ord3.newLine();
		}
		ord3.close();
			
		/**Create object for generating data sets */
		GenerateCVC1 cvc = new GenerateCVC1();
		
		cvc.setFne(false);
		cvc.setIpdb(false);
		cvc.setFilePath(this.getFilePath() + "/" + queryId);
		
		/**Call pre processing functions before calling data generation methods */
		PreProcessingActivity.preProcessingActivity(cvc);
		
		
		/** Check the data sets generated for this query */
		ArrayList<String> dataSets = RelatedToPreprocessing.getListOfDataset(this);
		
		System.out.println("\n\n***********************************************************************\n");
		System.out.println("DATA SETS FOR QUERY "+queryId+" ARE GENERATED");
		System.out.println("\n\n***********************************************************************\n");

		/**Update query info table */
		WriteFileAndUploadDatasets.updateQueryInfo(this, queryId, query, queryDesc);
		
		
		/**Upload the data sets into the database */
		WriteFileAndUploadDatasets.uploadDataset(this, queryId, dataSets);			

		System.out.println("\n***********************************************************************\n\n");
		System.out.println("DATASET FOR QUERY "+queryId+" ARE UPLOADED");
		System.out.println("\n***********************************************************************\n\n");
	}
	
	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	
	public static void entry(String[] args) throws Exception{
		String filePath="4";

		String queryId = "A"+args[0]+"Q"+args[1];
		
		GenerateDataset_new g=new GenerateDataset_new(filePath + "/" + queryId);
		
        try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        
        int assignment_id = Integer.parseInt(args[0]);
		String question_id = args[1];     
        TestAnswer test=new TestAnswer();
	    Connection dbcon=null;	
	    dbcon = MyConnection.getExistingDatabaseConnection();
    	if(dbcon!=null){
    	    	  System.out.println("Connected successfullly");
    	    	  
    	 }
		String sel="Select correctquery from qinfo where assignmentid=? and questionid=?";
		PreparedStatement stmt=dbcon.prepareStatement(sel);
		stmt.setInt(1,assignment_id);
		stmt.setString(2, question_id);
		ResultSet rs=stmt.executeQuery();
		rs.next();
		String sql=rs.getString("correctquery");
		g.generateDatasetForQuery("A"+args[0]+"Q"+args[1], "true", sql, "", assignment_id);
		rs.close();
		dbcon.close();
	}
	

	public static void main(String[] args) throws Exception{
		//GenerateCVC1 gcv1 = new GenerateCVC1();
		//gcv1.initializeConnectionDetails(1);
		String filePath="4";
		GenerateDataset_new g=new GenerateDataset_new(filePath + "/" + "A1Q20");
	
	//g.generateDatasetForQuery("A1Q1", "true", "SELECT course_id, title FROM course", "");
	//g.generateDatasetForQuery("A1Q2", "true", "SELECT course_id, title FROM course WHERE dept_name= 'Comp. Sci.'", "");
	//g.generateDatasetForQuery("A1Q3", "true", "SELECT DISTINCT course.course_id, course.title, ID from course natural join teaches WHERE teaches.semester='Spring' AND teaches.year='2010'", "");
	//g.generateDatasetForQuery("A1Q4", "true", "SELECT DISTINCT student.id, student.name FROM takes natural join student WHERE course_id ='CS-101'", "");
	//g.generateDatasetForQuery("A1Q5", "true", "SELECT DISTINCT course.dept_name FROM course NATURAL JOIN section WHERE section.semester='Spring' AND section.year='2010'", "");
	//g.generateDatasetForQuery("A1Q6", "true", "SELECT course_id, title FROM course WHERE credits > 3", "");
	//g.generateDatasetForQuery("A1Q7", "true", "select course_id, count(distinct id) from course natural left outer join takes group by course_id", "");
	//g.generateDatasetForQuery("A1Q8", "true", "SELECT DISTINCT course_id, title FROM course NATURAL JOIN section WHERE semester = 'Spring' AND year = 2010 AND course_id NOT IN (SELECT course_id FROM prereq)", "");
	//g.generateDatasetForQuery("A1Q9", "true", "WITH s as (SELECT id,time_slot_id,year,semester FROM takes NATURAL JOIN section GROUP BY id,time_slot_id,year,semester HAVING count(time_slot_id)>1) SELECT DISTINCT id,name FROM s NATURAL JOIN student", "");
	//g.generateDatasetForQuery("A1Q9", "true", "SELECT distinct A.id, A.name FROM (SELECT * from student NATURAL JOIN takes NATURAL JOIN section) A, (SELECT * from student NATURAL JOIN takes NATURAL JOIN section) B WHERE A.name = B.name and A.time_slot_id = B.time_slot_id and A.course_id <> B.course_id and A.semester = B.semester and A.year = B.year", "");
	//g.generateDatasetForQuery("A1Q10", "true", "SELECT DISTINCT dept_name FROM course WHERE credits = (SELECT max(credits) FROM course)", "");
	//g.generateDatasetForQuery("A1Q11", "true", "SELECT DISTINCT instructor.ID,name,course_id FROM instructor LEFT OUTER JOIN TEACHES ON instructor.ID = teaches.ID", "");
	//g.generateDatasetForQuery("A1Q12", "true", "SELECT student.id, student.name FROM student WHERE lower(student.name) like '%sr%'", "");
	//g.generateDatasetForQuery("A1Q13", "true", "SELECT id, name FROM student NATURAL LEFT OUTER JOIN (SELECT id, name, course_id FROM student NATURAL LEFT OUTER JOIN takes WHERE year = 2010 and semester = 'Spring') a", "");
	//g.generateDatasetForQuery("A1Q14", "true", "SELECT DISTINCT * FROM takes T WHERE (NOT EXISTS (SELECT id,course_id FROM takes S WHERE S.grade <> 'F' AND T.id=S.id AND T.course_id=S.course_id) and T.grade IS NOT NULL) or (T.grade <> 'F' AND T.grade IS NOT NULL)", "");
		
		//g.generateDatasetForQuery("A1Q20", "true", "SELECT c.dept_name, SUM(i.salary), MAX(i.salary) FROM course c INNER JOIN department d ON (c.dept_name = d.dept_name) INNER JOIN instructor i ON (d.dept_name = i.dept_name) GROUP BY c.dept_name HAVING SUM(i.salary)>100000 AND MAX(i.salary)<75000", "");
	g.generateDatasetForQuery("A1Q21", "true", "SELECT c.dept_name, SUM(i.salary), MAX(i.salary) FROM course c INNER JOIN department d ON (c.dept_name = d.dept_name) INNER JOIN instructor i ON (d.dept_name = i.dept_name) ", "");
	}

}
