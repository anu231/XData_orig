package util;

import java.util.ArrayList;
import java.util.Vector;

import parsing.AggregateFunction;
import parsing.Column;
import parsing.Node;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLEncoder;

public class Utilities {
	
	public static String escapeCharacters(String str) throws Exception{
		str= str.replace(" ","_b");
		str=URLEncoder.encode(str,"UTF-8");
		
		
		str= str.replace("_","_u");
		str= str.replace("*","_s");
		str= str.replace("-","_m");
		str= str.replace(".","_d");
		str= str.replace("+","_a");
		str= str.replace("%","_p");
		
		/*str= str.replace("_","_u");
		str= str.replace(",","_c");
		str= str.replace(".","_d");
		str= str.replace(" ","_s");
		str= str.replace("&","_a");
		str= str.replace("-","_m");
		str= str.replace("+","_p");*/
		return str;
	}
	
	public static String covertDecimalToFraction(String str){
		String arr[]=str.split("\\.");
		if(arr.length==1)	return str;
		else {
			String numerator=arr[0]+arr[1];
			String denominator=((int)Math.pow(10,arr[1].length()))+"";
			return numerator+"/"+denominator;
		}
	}
	
	public static String getHexVal(int n, int numDigits){
		   String hex = Integer.toHexString(n);
		   int zerosToAppend = numDigits-hex.length(); 
		   for(int i=0;i<zerosToAppend;i++){
			   hex = "0" + hex;
		   }
		   hex = "0hex" + hex;
		   return hex;
	   }

	
	public static String getBinVal(long n, int numDigits){
		   
		String bin = Long.toBinaryString(n);
		   
		   int zerosToAppend = numDigits-bin.length(); 
		   for(int i=0;i<zerosToAppend;i++){
			   bin = "0" + bin;
		   }
		   bin = "0bin" + bin;
		   return bin;
	   }
	
	
	   public static int factorizeAndGetCount(int v){
			for(int i=2;i<v;i++){
				if(v%i==0){
					return i;
				}
			}
			return v;
		}

	public static void flattenConstraints(Vector<Node> constraints, Node n){
        //Currently assuming that the having clause is not very complex. It is of the form AggFunc(Col) RelOp Constant
        if(n==null){
            return;
        }
        if(n.getType()== null){
        	return;
        }
        if(n.getType().equalsIgnoreCase(Node.getBroNodeType())){
            constraints.add(n);
        }
        else{
            flattenConstraints(constraints, n.getLeft());
            flattenConstraints(constraints, n.getRight());
        }
    }
	
	/*
	 * Here Node n is a column reference R.a and we need to check if there is any aggregation constraint on relation R.
	 */
	public static boolean nodeContainsConsAgg(Node n, Vector<Node> aggsCons){
		String tableName = n.getColumn().getTableName();
		for(int i=0;i<aggsCons.size();i++){
			Vector<Node> aggs = aggsCons.get(i).getAggsFromAggConstraint();
			for(int j=0;j<aggs.size();j++){
				Vector<Column> cols = aggs.get(j).getAgg().getAggExp().getColumnsFromNode();
				for(int k=0;k<cols.size();k++){
					if(cols.get(k).getTableName().equalsIgnoreCase(tableName)){
						return true;
					}
				}
			}
		}
		return false;
	}

	public static void closeProcessStreams(Process p){
		try{
			p.getOutputStream().close();
		}
		catch(IOException io){}
		
		try{
			p.getInputStream().close();
		}
		catch(IOException io){}
		
		try{
			p.getErrorStream().close();
		}
		catch(IOException io){}		
	}
	
	public static ArrayList<String> createQueries(String path) throws Exception    
    {   
        String queryLine =      new String();  
        StringBuffer sBuffer =  new StringBuffer();  
        ArrayList<String> listOfQueries = new ArrayList<String>();  
          
        try    
        {    
            FileReader fr =     new FileReader(new File(path));         
            BufferedReader br = new BufferedReader(fr);    
        
            //read the SQL file line by line  
            while((queryLine = br.readLine()) != null)    
            {    
                // ignore comments beginning with #  
                int indexOfCommentSign = queryLine.indexOf('#');  
                if(indexOfCommentSign != -1)  
                {  
                    if(queryLine.startsWith("#"))  
                    {  
                        queryLine = new String("");  
                    }  
                    else   
                        queryLine = new String(queryLine.substring(0, indexOfCommentSign-1));  
                }  
                // ignore comments beginning with --  
                indexOfCommentSign = queryLine.indexOf("--");  
                if(indexOfCommentSign != -1)  
                {  
                    if(queryLine.startsWith("--"))  
                    {  
                        queryLine = new String("");  
                    }  
                    else   
                        queryLine = new String(queryLine.substring(0, indexOfCommentSign-1));  
                }  
                // ignore comments surrounded by /* */  
                indexOfCommentSign = queryLine.indexOf("/*");  
                if(indexOfCommentSign != -1)  
                {  
                    if(queryLine.startsWith("#"))  
                    {  
                        queryLine = new String("");  
                    }  
                    else  
                        queryLine = new String(queryLine.substring(0, indexOfCommentSign-1));  
                      
                    sBuffer.append(queryLine + " ");   
                    // ignore all characters within the comment  
                    do  
                    {  
                        queryLine = br.readLine();  
                    }  
                    while(queryLine != null && !queryLine.contains("*/"));  
                    indexOfCommentSign = queryLine.indexOf("*/");  
                    if(indexOfCommentSign != -1)  
                    {  
                        if(queryLine.endsWith("*/"))  
                        {  
                            queryLine = new String("");  
                        }  
                        else  
                            queryLine = new String(queryLine.substring(indexOfCommentSign+2, queryLine.length()-1));  
                    }  
                }  
                  
                //  the + " " is necessary, because otherwise the content before and after a line break are concatenated  
                // like e.g. a.xyz FROM becomes a.xyzFROM otherwise and can not be executed   
                if(queryLine != null)  
                    sBuffer.append(queryLine + " ");    
            }    
            br.close();  
              
            // here is our splitter ! We use ";" as a delimiter for each request   
            String[] splittedQueries = sBuffer.toString().split(";");  
              
            // filter out empty statements  
            for(int i = 0; i<splittedQueries.length; i++)    
            {  
                if(!splittedQueries[i].trim().equals("") && !splittedQueries[i].trim().equals("\t"))    
                {  
                    listOfQueries.add(new String(splittedQueries[i]));  
                }  
            }  
        }    
        catch(Exception e)    
        {    
            System.out.println("*** Error : "+e.toString());    
            System.out.println("*** ");    
            System.out.println("*** Error : ");    
            e.printStackTrace();    
            System.out.println("################################################");    
            System.out.println(sBuffer.toString());    
            throw e;
        }  
        return listOfQueries;  
    }

}
