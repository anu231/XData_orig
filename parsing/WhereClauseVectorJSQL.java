package parsing;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.derby.impl.sql.compile.AggregateNode;
import org.apache.derby.impl.sql.compile.BinaryRelationalOperatorNode;
import org.apache.derby.impl.sql.compile.ColumnReference;
import org.apache.derby.impl.sql.compile.FromSubquery;
import org.apache.derby.impl.sql.compile.QueryTreeNode;
import org.apache.derby.impl.sql.compile.ResultColumn;
import org.apache.derby.impl.sql.compile.ResultColumnList;

import parsing.AggregateFunction;
import parsing.FromListElement;
import parsing.JoinTreeNode;
import parsing.Node;
import parsing.Table;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
//import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

public class WhereClauseVectorJSQL {
	
	public static void getAggregationDataStructures(PlainSelect rsNode,FromListElement queryAliases, Map<String,Table> fromTables,boolean fromSubquery,boolean whereSubquery, QueryParser qParser) throws Exception {

		// Get group by columns
		Vector< Node> tempGroupBy= new Vector< Node>();
		int queryType = 0;
		if(fromSubquery) queryType = 1;
		if(whereSubquery) queryType = 2;


		if (rsNode.getGroupByColumnReferences() != null) {
			//GroupByList gbl = ((SelectNode) rsNode).getGroupByList();
			List<Object> gbl = rsNode.getGroupByColumnReferences();
			//Vector<QueryTreeNode> vgbc = gbl.getNodeVector();
			for (int i = 0; i < gbl.size(); i++) {
				Column gbc;
				if (gbl.get(i) instanceof Column){
					gbc = (Column)gbl.get(i);
				} else {
					continue;
				}
				//GroupByColumn gbc = (GroupByColumn) vgbc.get(i);
				System.out.println("vgbc " + i + " = " + gbc.getTable().getWholeTableName() + "." + gbc.getColumnName());
				//ColumnReference cr = (ColumnReference) gbc.getColumnExpression();
				
				//System.out.println("cr " + i + " = " + cr.getTableName() + "." + cr.getColumnName());
				
				System.out.println("queryAliases" + queryAliases.getAliasName());



				//				Node n = getColumnFromOccurenceInJC(cr.getColumnName(), cr
				//						.getTableName(), queryAliases, 1);
				//				System.out.println("nSingle.getTable() " + n.getTable());
				//				groupByNodes.add(n);
				//Original code commented out by Biplab above
				Vector<Node> n = null;
				//n = Util.getColumnListFromOccurenceInJC(cr.getColumnName(), cr.getTableName(), queryAliases, 1, qParser);
				n = Util.getColumnListFromOccurenceInJC(gbc.getColumnName(), gbc.getTable().getWholeTableName(), queryAliases, 1, qParser);
				if(n==null|| n.size()==0)//This raises because group by column may be an aliased column
				{//FIXME: MAHESH CHANGE THE BELOW CODE TO USE BUILT IN FUNCTIONS
					//String colName=cr.getColumnName();
					String colName=gbc.getColumnName();
					System.out.println("mahesh: null BEGIN split: "+"as "+colName.toLowerCase());
					System.out.println(" colName: "+colName+" ");

					String []subQuerySelect=qParser.getQuery().getQueryString().toLowerCase().split("as "+colName.toLowerCase());
					colName=subQuerySelect[0].toLowerCase().trim().toLowerCase();
					int lastIndex=colName.lastIndexOf(' ');
					/*//colName=subQuerySelect[0].toLowerCase().trim().toLowerCase().substring(subQuerySelect[0].lastIndexOf(' ')+1);
					for(int i=0;i<subQuerySelect.length;i++)
						System.out.println(subQuerySelect[i]);*/
					System.out.println("last: "+lastIndex+" colName: "+colName);
					colName=colName.substring(lastIndex+1);
					System.out.println("last: "+lastIndex+" colName: "+colName);
					Node n1;
					if(colName.contains("max")||colName.contains("count")||colName.contains("sum")||colName.contains("avg")||colName.contains("min")){
						//convert this colName into aggregate node type and call getWhereClause()
						AggregateFunction af = new AggregateFunction();

						String aggName=colName.split("\\(")[0].trim();
						System.out.println("agg: "+aggName);
						af.setFunc(aggName.toUpperCase());
						colName=colName.substring(colName.indexOf("(")+1,colName.indexOf(")"));
					}
					colName=colName.toUpperCase();
					n=Util.getColumnListFromOccurenceInJC(colName,gbc.getTable().getWholeTableName(), queryAliases,1,qParser);
					

					System.out.println("mahesh: null END");
					//FIXME: Mahesh Add to sub query if subquery references
				}

				for(Node nSingle : n)
				{
					//Storing sub query details
					nSingle.setQueryType(queryType);
					if(queryType == 1) nSingle.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
					if(queryType == 2) nSingle.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

					System.out.println("nSingle.getTable() " + nSingle.getTable());
					//groupByNodes.add(nSingle);
					//FIXME: Mahesh- Add the group by nodes to its list
					tempGroupBy.add(nSingle);
				}		

			}
		}
		if(whereSubquery)
			qParser.getWhereClauseSubqueries().get(qParser.getWhereClauseSubqueries().size()-1).groupByNodes = tempGroupBy;//.add(tempGroupBy);//FIXME: mahesh change this
		else if(fromSubquery)
			qParser.getFromClauseSubqueries().get(qParser.getFromClauseSubqueries().size()-1).groupByNodes = tempGroupBy;//add(tempGroupBy);
		else
			qParser.groupByNodes = tempGroupBy;//.add(tempGroupBy);

		//Get Aggregations
		//ResultColumnList rcList = rsNode.getResultColumns();
		List<SelectItem> rcList = rsNode.getSelectItems();
		Vector<AggregateFunction> tempAggFunc=new Vector<AggregateFunction>();
		for(int i=0;i<rcList.size();i++){

			if(rcList.get(i) instanceof SelectExpressionItem){

				SelectExpressionItem rc = (SelectExpressionItem)rcList.get(i);
				System.out.println("rc-" + i + " = " + rc.getExpression());

				if(rc.getExpression() instanceof Function){					
					AggregateFunction af = new AggregateFunction();
					Function an = (Function)rc.getExpression();
					String aggName = an.getName();

					//					System.out.println("hi = " + i);
					
					//QueryTreeNode qtn = (QueryTreeNode)an.getOperand();//FIXME
					//Node n = WhereClauseVectorJSQL.getWhereClauseVector(qtn, null,queryAliases,false, queryType,qParser);//mahesh: change null to actual name
					Node n = WhereClauseVectorJSQL.getWhereClauseVector(an, null,queryAliases,false, queryType,qParser);//mahesh: change null to actual name
					af.setAggExp(n);
					af.setFunc(aggName);
					af.setDistinct(an.isDistinct());
					af.setAggAliasName(rc.getAlias());
					//aggFunc.add(af);
					tempAggFunc.add(af);
				}
			}
		}
		//add this to list
		if(whereSubquery)
			qParser.getWhereClauseSubqueries().get(qParser.getWhereClauseSubqueries().size()-1).aggFunc = tempAggFunc;//.add(tempAggFunc);
		else if(fromSubquery)
			qParser.getFromClauseSubqueries().get(qParser.getFromClauseSubqueries().size()-1).aggFunc = tempAggFunc;//.add(tempAggFunc);
		else
			qParser.aggFunc = tempAggFunc;//.add(tempAggFunc);

		System.out.println("hi");

		// get having clause
		//ValueNode hc = ((SelectNode) rsNode).havingClause;
		Expression hc = rsNode.getHaving();
		//havingClause = getWhereClauseVector(hc, queryAliases,false);
		//add this to having list
		//FIXME: Mahesh Add to sub query if subquery references
		Node n = WhereClauseVectorJSQL.getWhereClauseVector(hc, null,queryAliases,false,queryType,qParser);

		if(whereSubquery)
			//this.WhereClauseSubqueries.get(WhereClauseSubqueries.size()-1).HavingClause.add(getWhereClauseVector(hc, null,queryAliases,false));
			qParser.getWhereClauseSubqueries().get(qParser.getWhereClauseSubqueries().size()-1).havingClause = n;
		else if(fromSubquery)
			qParser.getFromClauseSubqueries().get(qParser.getFromClauseSubqueries().size()-1).havingClause = n;
		else{

			n = Util.modifyNode(n,qParser);
			qParser.havingClause = n;
		}

	}
	
	public static Node getWhereClauseVector(Object clause, String exposedName, FromListElement fle,boolean isWhereClause, int queryType, QueryParser qParser)
			throws Exception {

		if (clause == null) {
			return null;
		} else if (clause instanceof Parenthesis){			
			return getWhereClauseVector(((Parenthesis)clause).getExpression(), exposedName, fle, isWhereClause, queryType, qParser);
		}
		else if (clause instanceof Function) {
			Function an = (Function)clause;
			String aggName = an.getName();
			//ColumnReference cr = (ColumnReference)an.getOperand();
			AggregateFunction af = new AggregateFunction();
			//QueryTreeNode qtn = (QueryTreeNode)an.getOperand();
			if (an.getParameters()!=null){
				ExpressionList anList = an.getParameters();
				List<Expression> expList = anList.getExpressions();//FIXME not only 1 expression but all expressions
				Node n = getWhereClauseVector(expList.get(0),null, fle, isWhereClause, queryType, qParser);
				af.setAggExp(n);
			} else {
				af.setAggExp(null);
			}
			
			af.setFunc(aggName);
			af.setDistinct(an.isDistinct());
			af.setAggAliasName(exposedName);

			Node agg = new Node();
			agg.setAgg(af);
			agg.setType(Node.getAggrNodeType());

			//Storing sub query details
			agg.setQueryType(queryType);
			if(queryType == 1) agg.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
			if(queryType == 2) agg.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);
			//Adding this to the list of aliased names
			if(exposedName !=null){
				Vector<Node> present = new Vector<Node>();
				if( qParser.getAliasedToOriginal().get(exposedName) != null)
					present = qParser.getAliasedToOriginal().get(exposedName);
				present.add(agg);
				qParser.getAliasedToOriginal().put(exposedName, present);
			}

			return agg;

		} else if (clause instanceof DoubleValue) {
			Node n = new Node();
			n.setType(Node.getValType());
			String s=((((DoubleValue)clause).getValue()))+"";
			//String str=(BigIntegerDecimal)((((NumericConstantNode) clause).getValue()).getDouble()).toString();
			s=util.Utilities.covertDecimalToFraction(s);
			n.setStrConst(s);
			n.setLeft(null);
			n.setRight(null);
			return n;

		} else if (clause instanceof LongValue){
			Node n = new Node();
			n.setType(Node.getValType());
			String s=((((LongValue)clause).getValue()))+"";
			s=util.Utilities.covertDecimalToFraction(s);
			n.setStrConst(s);
			n.setLeft(null);
			n.setRight(null);
			return n;
		}
		else if (clause instanceof StringValue) {
			Node n = new Node();
			n.setType(Node.getValType());
			n.setStrConst(((StringValue) clause).getValue());
			//n.setStrConst("'"+((CharConstantNode) clause).getString()+"'");
			n.setLeft(null);
			n.setRight(null);
			return n;
		} else if (clause instanceof Column) {
			Column columnReference = (Column) clause;
			Node n = new Node();
			String colName= columnReference.getColumnName();			
			String tableName = columnReference.getTable().getWholeTableName();

			if(qParser.getQuery().getQueryString().toLowerCase().contains(("as "+colName.toLowerCase()))){//Handling aliased columns
				//			String subQueryRefrence=columnReference.getTableName();

				Vector< Node > value = qParser.getAliasedToOriginal().get(colName);
				System.out.println(value.toString());
				n = value.get(0);//FIXME: vector of nodes not a single node
				return n;

			}			
			else{
				System.out.println("colName = " + colName + " tablename = " + columnReference.getTable().getWholeTableName() + " fle = " + fle.getTableName() + " " + fle.getTableName() + " " + fle.getTableNameNo() + " " + fle.getTabs().elementAt(0).getTableName());
				n = Util.getColumnFromOccurenceInJC(colName,columnReference.getTable().getWholeTableName(), fle, qParser);
				if (n == null) {//then probably the query is correlated				
					n = Util.getColumnFromOccurenceInJC(colName,columnReference.getTable().getWholeTableName(), qParser.getQueryAliases(),qParser);
				}	
			}
			//System.out.println(Node.getColRefType());
			if(n == null) {
				System.out.println("n = null");
				//return null;
			}
			
			n.setType(Node.getColRefType());
			if (columnReference.getTable().getWholeTableName() != null) {
				n.setTableAlias(columnReference.getTable().getWholeTableName());
			} else {
				n.setTableAlias("");
			}

			if(n.getColumn() != null){
				n.getColumn().setAliasName(exposedName);
				n.setTable(n.getColumn().getTable());
			}

			n.setLeft(null);
			n.setRight(null);


			//Storing sub query details
			if(qParser.getSubQueryNames().containsKey(tableName)){//If this node is inside a sub query
				n.setQueryType(1);
				n.setQueryIndex(qParser.getSubQueryNames().get(tableName));
			}
			else if(qParser.getTableNames().containsKey(tableName)){
				n.setQueryType(qParser.getTableNames().get(tableName)[0]);
				n.setQueryIndex(qParser.getTableNames().get(tableName)[1]);
			}
			else{
				n.setQueryType(queryType);
				if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
				if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);
			}
			if(exposedName !=null){
				Vector<Node> present = new Vector<Node>();
				if( qParser.getAliasedToOriginal().get(exposedName) != null)
					present = qParser.getAliasedToOriginal().get(exposedName);
				present.add(n);
				qParser.getAliasedToOriginal().put(exposedName, present);
			}
			return n;

		} else if (clause instanceof AndExpression) {
			BinaryExpression andNode = ((BinaryExpression) clause);
			if (andNode.getLeftExpression() != null
					&& andNode.getRightExpression() != null) {
				Node n = new Node();
				Node left = new Node();
				Node right = new Node();
				n.setType(Node.getAndNodeType());
				n.setOperator("AND");
				left = getWhereClauseVector(andNode.getLeftExpression(), exposedName, fle, isWhereClause, queryType,qParser);
				right = getWhereClauseVector(andNode.getRightExpression(), exposedName, fle, isWhereClause, queryType,qParser);
				/*
				//Mahesh
				if(left.queryType != 0){//If column is an aliased column of sub query
				    //If aggregate then add to havingClause of sub query
					addToSubQuery(left);
					left = null;
				}

				if(right.queryType != 0){//If column is an aliased column of sub query
				    //If aggregate then add to havingClause of sub query
					addToSubQuery(right);
					right=null;
				}

				if(left == null)
					return right;
				if(right == null)
					return left;
				 */
				n.setLeft(left);
				n.setRight(right);

				//Storing sub query details
				n.setQueryType(queryType);
				if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
				if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

				return n;
			}

		} else if (clause instanceof OrExpression) {
			BinaryExpression orNode = ((BinaryExpression) clause);
			if (orNode.getLeftExpression() != null
					&& orNode.getRightExpression() != null) {
				Node n = new Node();
				n.setType(Node.getOrNodeType());
				n.setOperator("OR");
				n.setLeft(getWhereClauseVector(orNode.getLeftExpression(), exposedName, fle, isWhereClause, queryType,qParser));

				n.setRight(getWhereClauseVector(orNode.getRightExpression(), exposedName, fle, isWhereClause, queryType,qParser));

				//Storing sub query details
				n.setQueryType(queryType);
				if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
				if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

				return n;
			}
		} else if (clause instanceof net.sf.jsqlparser.expression.operators.relational.Between) {
			/*BinaryRelationalOperatorNode broNode = ((BinaryRelationalOperatorNode) clause);			
			Node n = new Node();
			n.setType(Node.getBroNodeType());
			n.setOperator(qParser.cvcRelationalOperators[broNode.getOperator()]);
			n.setLeft(getWhereClauseVector(broNode.getLeftOperand(), exposedName, fle, isWhereClause, queryType,qParser));
			n.setRight(getWhereClauseVector(broNode.getRightOperand(), exposedName, fle, isWhereClause, queryType,qParser));

			//Storing sub query details
			n.setQueryType(queryType);
			if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
			if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

			/*
			//FIXME: Mahesh when original subquery calls this then it is added to havingClause of sub query 
			if(n.getRight().getQueryType() != 0 || n.getLeft().getQueryType() != 0){//Aliased column name is used in outer query
				addToSubQuery(n);
				n = null;
			}*/
			throw new Exception("getWhereClauseVector needs more programming \n"+clause.getClass()+"\n"+clause.toString());
		} 

		//Added by Bikash ---------------------------------------------------------------------------------
		else if(clause instanceof LikeExpression){
			BinaryExpression likeNode=((BinaryExpression)clause);
			if (likeNode.getLeftExpression() !=null && likeNode.getRightExpression()!=null )
			{
				//if(likeNode.getReceiver() instanceof ColumnReference && (likeNode.getLeftOperand() instanceof CharConstantNode || likeNode.getLeftOperand() instanceof ParameterNode))
				{
					Node n=new Node();
					n.setType(Node.getLikeNodeType());
					n.setOperator("~");
					n.setLeft(getWhereClauseVector(likeNode.getLeftExpression(), exposedName, fle, isWhereClause, queryType,qParser));
					n.setRight(getWhereClauseVector(likeNode.getRightExpression(), exposedName, fle, isWhereClause, queryType,qParser));
					/*
					//Mahesh
					if(n.getRight().getQueryType() != 0 || n.getLeft().getQueryType() != 0){//Aliased column name is used in outer query
						addToSubQuery(n);
						n = null;
					}
					 */
					//Storing sub query details
					n.setQueryType(queryType);
					if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
					if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);
					return n;
				}
			}
		}

		else if(clause instanceof JdbcParameter){
			Node n = new Node();
			n.setType(Node.getValType());		
			n.setStrConst("$"+qParser.paramCount);
			qParser.paramCount++;
			n.setLeft(null);
			n.setRight(null);

			//Storing sub query details
			n.setQueryType(queryType);
			if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
			if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

			return n;
		}

		//**********************************************************************************/
		else if (clause instanceof Addition){
			BinaryExpression baoNode = ((BinaryExpression)clause);
			Node n = new Node();
			n.setType(Node.getBaoNodeType());
			n.setOperator("+");
			n.setLeft(getWhereClauseVector(baoNode.getLeftExpression(), exposedName, fle, isWhereClause, queryType,qParser));
			n.setRight(getWhereClauseVector(baoNode.getRightExpression(), exposedName, fle, isWhereClause, queryType,qParser));
			//Storing sub query details
			n.setQueryType(queryType);
			if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
			if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);
			return n;
		}
		else if (clause instanceof Subtraction){
			BinaryExpression baoNode = ((BinaryExpression)clause);
			Node n = new Node();
			n.setType(Node.getBaoNodeType());
			n.setOperator("-");
			n.setLeft(getWhereClauseVector(baoNode.getLeftExpression(), exposedName, fle, isWhereClause, queryType,qParser));
			n.setRight(getWhereClauseVector(baoNode.getRightExpression(), exposedName, fle, isWhereClause, queryType,qParser));
			//Storing sub query details
			n.setQueryType(queryType);
			if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
			if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);
			return n;
		}
		else if (clause instanceof Multiplication){
			BinaryExpression baoNode = ((BinaryExpression)clause);
			Node n = new Node();
			n.setType(Node.getBaoNodeType());
			n.setOperator("*");
			n.setLeft(getWhereClauseVector(baoNode.getLeftExpression(), exposedName, fle, isWhereClause, queryType,qParser));
			n.setRight(getWhereClauseVector(baoNode.getRightExpression(), exposedName, fle, isWhereClause, queryType,qParser));
			//Storing sub query details
			n.setQueryType(queryType);
			if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
			if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);
			return n;
		}
		else if (clause instanceof Division){
			BinaryExpression baoNode = ((BinaryExpression)clause);
			Node n = new Node();
			n.setType(Node.getBaoNodeType());
			n.setOperator("/");
			n.setLeft(getWhereClauseVector(baoNode.getLeftExpression(), exposedName, fle, isWhereClause, queryType,qParser));
			n.setRight(getWhereClauseVector(baoNode.getRightExpression(), exposedName, fle, isWhereClause, queryType,qParser));
			//Storing sub query details
			n.setQueryType(queryType);
			if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
			if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);
			return n;
		}
		/*
		 * else if (clause instanceof InListOperatorNode) { //In List: Where we
		 * have a list of concrete values within parenthesis }
		 */
		else if (clause instanceof NotEqualsTo) {
			NotEqualsTo broNode = (NotEqualsTo)clause;
			
			//BinaryRelationalOperatorNode broNode = ((BinaryRelationalOperatorNode) clause);			
			Node n = new Node();
			n.setType(Node.getBroNodeType());
			n.setOperator(qParser.cvcRelationalOperators[2]);
			n.setLeft(getWhereClauseVector(broNode.getLeftExpression(), exposedName, fle, isWhereClause, queryType,qParser));
			n.setRight(getWhereClauseVector(broNode.getRightExpression(), exposedName, fle, isWhereClause, queryType,qParser));

			//Storing sub query details
			n.setQueryType(queryType);
			if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
			if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

			/*
			//FIXME: Mahesh when original subquery calls this then it is added to havingClause of sub query 
			if(n.getRight().getQueryType() != 0 || n.getLeft().getQueryType() != 0){//Aliased column name is used in outer query
				addToSubQuery(n);
				n = null;
			}*/
			return n;
			//throw new Exception("getWhereClauseVector needs more programming \n"+clause.getClass()+"\n"+clause.toString());
		} else if (clause instanceof IsNullExpression) {
			IsNullExpression isNullNode = (IsNullExpression) clause;
			Node n = new Node();
			n.setType(Node.getIsNullNodeType());
			n.setLeft(getWhereClauseVector(isNullNode.getLeftExpression(),exposedName, fle,isWhereClause, queryType,qParser));
			n.setOperator("=");
			n.setRight(null);
			//Storing sub query details
			n.setQueryType(queryType);
			if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
			if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);
			return n;
		} else if (clause instanceof InExpression){ 
			//handles NOT and NOT IN both
			InExpression sqn = (InExpression)clause;
			
			Node lhs = getWhereClauseVector(sqn.getLeftExpression(),exposedName, fle,isWhereClause, queryType,qParser);
			Vector<Node> thisSubQConds = new Vector<Node>();
			//FromSubquery subq = new FromSubquery();
			//subq.setSubquery(sqn.getResultSet());
			//FromListElement fle1 = OperateOnSubquery(subq, thisSubQConds,new JoinTreeNode());
			//FIXME: mahesh changed this.But not sure
			if (sqn.getItemsList() instanceof SubSelect){
				OperateOnSubQueryJSQL.OperateOnSubquery((SubSelect)sqn.getItemsList(), thisSubQConds,new JoinTreeNode(),false,isWhereClause,qParser);
			}
			
			FromListElement fle1 =qParser.getWhereClauseSubqueries().get(qParser.getWhereClauseSubqueries().size()-1).getQueryAliases();



			//System.out.println("SUB QUERY FOUND "+sqn);

			// Extract the projected column and create a colref node
			SubSelect subS = (SubSelect)sqn.getItemsList();
			List<SelectItem> rcList = ((PlainSelect)subS.getSelectBody()).getSelectItems();
			
			//ResultColumnList rcl = subq.getSubquery().getResultColumns();
			SelectExpressionItem rc = (SelectExpressionItem)rcList.get(0);
			Column cr ;
			String aggName="" ;
			Function an = new Function();
			
			//mahesh chane
			//ValueNode exp = rc.getExpression();
			Expression exp = rc.getExpression();
			if( exp instanceof Function ){
				an = (Function)exp;
				aggName = an.getName();
				ExpressionList expL = an.getParameters();
				cr = (net.sf.jsqlparser.schema.Column)expL.getExpressions().get(0);
			}
			else {
				cr = (net.sf.jsqlparser.schema.Column) exp;
			}
				
			Node rhs;
			//rhs= getWhereClauseVector(exp, fle,isWhereClause);


			rhs = Util.getColumnFromOccurenceInJC(cr.getColumnName(), cr.getTable().getWholeTableName(), fle1, qParser);
			if (rhs == null) {
				rhs = Util.getColumnFromOccurenceInJC(cr.getColumnName(), cr.getTable().getWholeTableName(), qParser.getQueryAliases(),qParser);
			}
			rhs.setType(Node.getColRefType());
			if (cr.getTable().getWholeTableName() != null) {
				rhs.setTableAlias(cr.getTable().getWholeTableName());
			} else {
				rhs.setTableAlias("");
			}

			if( exp instanceof Function ){
				AggregateFunction af = new AggregateFunction();
				af.setAggExp(rhs);
				af.setFunc(aggName);
				af.setDistinct(an.isDistinct());
				Node rhs1= new Node();
				rhs1.setAgg(af);
				rhs1.setType(Node.getAggrNodeType());
				rhs1.setTableAlias(rhs.getTableAlias());
				rhs1.setColumn(rhs.getColumn());
				rhs1.setTableNameNo(rhs.getTableNameNo());
				rhs1.setTable(rhs.getTable());

				System.out.println("Hello");
				rhs=rhs1;
			}

			rhs.setQueryType(2);
			rhs.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);
			Node cond = new Node();
			cond.setType(Node.getBroNodeType());
			cond.setLeft(lhs);
			cond.setRight(rhs);
			cond.setOperator("=");
			cond.setAgg(rhs.getAgg());
			// create the final subquery node and return it
			Node sqNode = new Node();
			sqNode.setType(Node.getInNodeType());
			sqNode.setSubQueryConds(thisSubQConds);
			sqNode.setLhsRhs(cond);
			return sqNode;
			
		} else if (clause instanceof ExistsExpression){
			ExistsExpression sqn = (ExistsExpression)clause;
			
			Node lhs = null;// getWhereClauseVector(sqn.getRightExpression(),exposedName, fle,isWhereClause, queryType,qParser);
			Vector<Node> thisSubQConds = new Vector<Node>();
			
			//FromSubquery subq = new FromSubquery();
			//subq.setSubquery(sqn.getResultSet());
			//FromListElement fle1 = OperateOnSubquery(subq, thisSubQConds,new JoinTreeNode(),this.FromClauseSubqueries, !isWhereClause);
			//FIXME: mahesh changed this.But not sure
			SubSelect subS = (SubSelect)sqn.getRightExpression();
			OperateOnSubQueryJSQL.OperateOnSubquery(subS, thisSubQConds,new JoinTreeNode(),false,isWhereClause,qParser);
			FromListElement fle1 =qParser.getWhereClauseSubqueries().get(qParser.getWhereClauseSubqueries().size()-1).getQueryAliases();


			// Extract the projected column and create a colref node
			// ResultColumnList rcl = subq.getSubquery().getResultColumns();
			// ResultColumn rc = (ResultColumn)rcl.getNodeVector().get(0);
			// ColumnReference cr = (ColumnReference)rc.getExpression();
			// Column c = getColumnFromOccurenceInJC(cr.getColumnName(),
			// cr.getTableName(), fle1);
			// if(c==null){
			// c = getColumnFromOccurenceInJC(cr.getColumnName(),
			// cr.getTableName(), queryAliases);
			// }
			// Node rhs = new Node();
			// rhs.setType(Node.getColRefType());
			// rhs.setColumn(c);
			// //create the condition node as X = Y for X in (select Y from
			// ...)
			// Node cond = new Node();
			// cond.setType(Node.getBroNodeType());
			// cond.setLeft(lhs);
			// cond.setRight(rhs);
			// cond.setOperator("=");
			// create the final subquery node and return it
			Node sqNode = new Node();


			sqNode.setType(Node.getExistsNodeType());
			sqNode.setSubQueryConds(thisSubQConds);


			Node rhs = new Node();
			rhs.setQueryType(2);
			rhs.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

			sqNode.setLhsRhs(rhs);

			// sqNode.setLhsRhs(cond);
			return sqNode;
			
		}
		else if (clause instanceof SubSelect) {
			/*
			 * System.out.println("Found Subquery"); int currentCounterVal =
			 * subQueryCounter;
			 * if(subQueryConds.containsKey(currentCounterVal)){
			 * currentCounterVal++; subQueryCounter = currentCounterVal; } else{
			 * currentCounterVal = (currentCounterVal*10) + 1; subQueryCounter =
			 * currentCounterVal; }
			 */
			/*Start of Comment on 13th May  FIXME*/
			SubSelect sqn = (SubSelect) clause;
			
			//added by bikash
			//Node lhs = getWhereClauseVector(sqn.getLeftOperand(), fle);
			Vector<Node> thisSubQConds = new Vector<Node>();
			//FromSubquery subq = new FromSubquery();
			//subq.setSubquery(sqn.getResultSet());

			//FromListElement fle1 = OperateOnSubquery(subq, thisSubQConds,new JoinTreeNode(),this.FromClauseSubqueries, !isWhereClause);

			//FIXME: mahesh changed this.But not sure
			OperateOnSubQueryJSQL.OperateOnSubquery(sqn, thisSubQConds,new JoinTreeNode(),false,isWhereClause,qParser);
			FromListElement fle1 =qParser.getWhereClauseSubqueries().get(qParser.getWhereClauseSubqueries().size()-1).getQueryAliases();

			//PlainSelect ps = sqn.getSelectBody();
			List<SelectItem> rcList = ((PlainSelect)sqn.getSelectBody()).getSelectItems();
			SelectExpressionItem rc = (SelectExpressionItem)rcList.get(0);
			//ResultColumn rc = (ResultColumn) rcl.getNodeVector().get(0);
			if(rc.getExpression() instanceof Function){
				//sqn.getResultSet();
				//Node rhs=getWhereClauseVector((AggregateNode)rc.getExpression(),fle1);

				Function an = (Function)rc.getExpression();
				String aggName = an.getName();
				ExpressionList expL = an.getParameters();
				//net.sf.jsqlparser.schema.Column cr = (net.sf.jsqlparser.schema.Column)expL.getExpressions().get(0);
				AggregateFunction af = new AggregateFunction();
				//QueryTreeNode qtn = (QueryTreeNode)an.getOperand();
				Node n = getWhereClauseVector(expL.getExpressions().get(0), exposedName,fle1,isWhereClause, 2, qParser);
				af.setAggExp(n);
				af.setFunc(aggName);
				af.setDistinct(an.isDistinct());


				Node rhs = new Node();
				rhs.setAgg(af);
				rhs.setType(Node.getAggrNodeType());

				rhs.setQueryType(2);
				rhs.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

				//Node cond = new Node();
				//cond.setType(Node.getBroNodeSubQType());
				//cond.setLeft(lhs);
				//cond.setRight(rhs);
				//String operator = sqn.subQType(sqn.getSubqueryType());
				//cond.setOperator(operator);
				// create the final subquery node and return it
				Node sqNode = new Node();
				sqNode.setType(Node.getBroNodeSubQType());
				sqNode.setSubQueryConds(thisSubQConds);
				sqNode.setLhsRhs(rhs);
				return sqNode;
			}
			else if(rc.getExpression() instanceof ColumnReference){
				//the result of subquery must be a single tuple
				System.out.println("CCCCCCCCCCCCCCCCCCCCCCCc");
				System.out.println("the result of subquery must be a single tuple");
			}
			
			/*
			if (sqn.getSubqueryType() == 1 || sqn.getSubqueryType() == 2) { // IN
				// SubQuery
				// Type
				Node lhs = getWhereClauseVector(sqn.getLeftOperand(),exposedName, fle,isWhereClause, queryType,qParser);
				Vector<Node> thisSubQConds = new Vector<Node>();
				FromSubquery subq = new FromSubquery();
				subq.setSubquery(sqn.getResultSet());
				//FromListElement fle1 = OperateOnSubquery(subq, thisSubQConds,new JoinTreeNode());
				//FIXME: mahesh changed this.But not sure
				OperateOnSubQuery.OperateOnSubquery(subq, thisSubQConds,new JoinTreeNode(),false,isWhereClause,qParser);
				FromListElement fle1 =qParser.getWhereClauseSubqueries().get(qParser.getWhereClauseSubqueries().size()-1).getQueryAliases();



				//System.out.println("SUB QUERY FOUND "+sqn);

				// Extract the projected column and create a colref node
				ResultColumnList rcl = subq.getSubquery().getResultColumns();
				ResultColumn rc = (ResultColumn) rcl.getNodeVector().get(0);
				ColumnReference cr ;
				String aggName="" ;
				AggregateNode an=new AggregateNode() ;
				//mahesh chane
				ValueNode exp = rc.getExpression();
				if( exp instanceof AggregateNode ){
					an = (AggregateNode)exp;
					aggName = an.getAggregateName();
					cr = (ColumnReference)an.getOperand();
				}
				else
					cr = (ColumnReference) exp;
				Node rhs;
				//rhs= getWhereClauseVector(exp, fle,isWhereClause);


				rhs = Util.getColumnFromOccurenceInJC(cr.getColumnName(), cr
						.getTableName(), fle1, qParser);
				if (rhs == null) {
					rhs = Util.getColumnFromOccurenceInJC(cr.getColumnName(), cr
							.getTableName(), qParser.getQueryAliases(),qParser);
				}
				rhs.setType(Node.getColRefType());
				if (cr.getTableName() != null) {
					rhs.setTableAlias(cr.getTableName());
				} else {
					rhs.setTableAlias("");
				}

				if( exp instanceof AggregateNode ){
					AggregateFunction af = new AggregateFunction();
					af.setAggExp(rhs);
					af.setFunc(aggName);
					af.setDistinct(an.isDistinct());
					Node rhs1= new Node();
					rhs1.setAgg(af);
					rhs1.setType(Node.getAggrNodeType());
					rhs1.setTableAlias(rhs.getTableAlias());
					rhs1.setColumn(rhs.getColumn());
					rhs1.setTableNameNo(rhs.getTableNameNo());
					rhs1.setTable(rhs.getTable());

					System.out.println("Hello");
					rhs=rhs1;
				}

				rhs.setQueryType(2);
				rhs.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);
				Node cond = new Node();
				cond.setType(Node.getBroNodeType());
				cond.setLeft(lhs);
				cond.setRight(rhs);
				cond.setOperator("=");
				cond.setAgg(rhs.getAgg());
				// create the final subquery node and return it
				Node sqNode = new Node();
				sqNode.setType(Node.getInNodeType());
				sqNode.setSubQueryConds(thisSubQConds);
				sqNode.setLhsRhs(cond);
				return sqNode;
				// subQueryConds.add(rhs);
				// subQueryConds.add(thisSubQConds);
				// return null;
			} else if (sqn.getSubqueryType() >= 3
					&& sqn.getSubqueryType() <= 14) { // (=/>/</..) ALL/ANY
				// SubQury Type
				Node lhs = getWhereClauseVector(sqn.getLeftOperand(), exposedName,fle,isWhereClause, queryType,qParser);
				Vector<Node> thisSubQConds = new Vector<Node>();
				FromSubquery subq = new FromSubquery();
				subq.setSubquery(sqn.getResultSet());
				//FromListElement fle1 = OperateOnSubquery(subq, thisSubQConds ,new JoinTreeNode(),this.FromClauseSubqueries, !isWhereClause);//FIXME JoinTreeNode is a patch. Have to pass it here, but we won't use this because its in the where clause subquery.

				//FIXME: mahesh changed this.But not sure
				OperateOnSubQuery.OperateOnSubquery(subq, thisSubQConds,new JoinTreeNode(),false, isWhereClause,qParser);
				FromListElement fle1 =qParser.getWhereClauseSubqueries().get(qParser.getWhereClauseSubqueries().size()-1).getQueryAliases();


				// Extract the projected column and create a colref node
				ResultColumnList rcl = subq.getSubquery().getResultColumns();
				ResultColumn rc = (ResultColumn) rcl.getNodeVector().get(0);
				ColumnReference cr = (ColumnReference) rc.getExpression();
				Node rhs;
				rhs = Util.getColumnFromOccurenceInJC(cr.getColumnName(), cr
						.getTableName(), fle1, qParser);
				if (rhs == null) {
					rhs = Util.getColumnFromOccurenceInJC(cr.getColumnName(), cr
							.getTableName(), qParser.getQueryAliases(), qParser);
				}
				rhs.setType(Node.getColRefType());
				// create the condition node as X = Y for X in (select Y from
				// ...)

				rhs.setQueryType(2);
				rhs.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

				Node cond = new Node();
				cond.setType(Node.getBroNodeType());
				cond.setLeft(lhs);
				cond.setRight(rhs);

				String operator = sqn.subQType(sqn.getSubqueryType());
				cond.setOperator(operator.substring(0, 1));
				//cond.setOperator("=");

				// create the final subquery node and return it
				Node sqNode = new Node();
				sqNode.setType(Node.getAllAnyNodeType());
				sqNode.setSubQueryConds(thisSubQConds);
				sqNode.setLhsRhs(cond);
				return sqNode;
			} else if (sqn.getSubqueryType() == 15
					|| sqn.getSubqueryType() == 16) { // EXISTS SubQuery Type
				Node lhs = getWhereClauseVector(sqn.getLeftOperand(),exposedName, fle,isWhereClause, queryType,qParser);
				Vector<Node> thisSubQConds = new Vector<Node>();
				FromSubquery subq = new FromSubquery();
				subq.setSubquery(sqn.getResultSet());
				//FromListElement fle1 = OperateOnSubquery(subq, thisSubQConds,new JoinTreeNode(),this.FromClauseSubqueries, !isWhereClause);
				//FIXME: mahesh changed this.But not sure
				OperateOnSubQuery.OperateOnSubquery(subq, thisSubQConds,new JoinTreeNode(),false,isWhereClause,qParser);
				FromListElement fle1 =qParser.getWhereClauseSubqueries().get(qParser.getWhereClauseSubqueries().size()-1).getQueryAliases();


				// Extract the projected column and create a colref node
				// ResultColumnList rcl = subq.getSubquery().getResultColumns();
				// ResultColumn rc = (ResultColumn)rcl.getNodeVector().get(0);
				// ColumnReference cr = (ColumnReference)rc.getExpression();
				// Column c = getColumnFromOccurenceInJC(cr.getColumnName(),
				// cr.getTableName(), fle1);
				// if(c==null){
				// c = getColumnFromOccurenceInJC(cr.getColumnName(),
				// cr.getTableName(), queryAliases);
				// }
				// Node rhs = new Node();
				// rhs.setType(Node.getColRefType());
				// rhs.setColumn(c);
				// //create the condition node as X = Y for X in (select Y from
				// ...)
				// Node cond = new Node();
				// cond.setType(Node.getBroNodeType());
				// cond.setLeft(lhs);
				// cond.setRight(rhs);
				// cond.setOperator("=");
				// create the final subquery node and return it
				Node sqNode = new Node();


				sqNode.setType(Node.getExistsNodeType());
				sqNode.setSubQueryConds(thisSubQConds);


				Node rhs = new Node();
				rhs.setQueryType(2);
				rhs.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

				sqNode.setLhsRhs(rhs);

				// sqNode.setLhsRhs(cond);
				return sqNode;


			}else if (sqn.getSubqueryType() >=17
					|| sqn.getSubqueryType() <= 22) {
				//added by bikash
				//Node lhs = getWhereClauseVector(sqn.getLeftOperand(), fle);
				Vector<Node> thisSubQConds = new Vector<Node>();
				FromSubquery subq = new FromSubquery();
				subq.setSubquery(sqn.getResultSet());

				//FromListElement fle1 = OperateOnSubquery(subq, thisSubQConds,new JoinTreeNode(),this.FromClauseSubqueries, !isWhereClause);

				//FIXME: mahesh changed this.But not sure
				OperateOnSubQuery.OperateOnSubquery(subq, thisSubQConds,new JoinTreeNode(),false,isWhereClause,qParser);
				FromListElement fle1 =qParser.getWhereClauseSubqueries().get(qParser.getWhereClauseSubqueries().size()-1).getQueryAliases();


				ResultColumnList rcl = subq.getSubquery().getResultColumns();
				ResultColumn rc = (ResultColumn) rcl.getNodeVector().get(0);
				if(rc.getExpression() instanceof AggregateNode){
					sqn.getResultSet();
					//Node rhs=getWhereClauseVector((AggregateNode)rc.getExpression(),fle1);

					AggregateNode an = (AggregateNode)rc.getExpression();
					String aggName = an.getAggregateName();
					ColumnReference cr = (ColumnReference)an.getOperand();
					AggregateFunction af = new AggregateFunction();
					QueryTreeNode qtn = (QueryTreeNode)an.getOperand();
					Node n = getWhereClauseVector(qtn, exposedName,fle1,isWhereClause, 2, qParser);
					af.setAggExp(n);
					af.setFunc(aggName);
					af.setDistinct(an.isDistinct());


					Node rhs = new Node();
					rhs.setAgg(af);
					rhs.setType(Node.getAggrNodeType());

					rhs.setQueryType(2);
					rhs.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

					//Node cond = new Node();
					//cond.setType(Node.getBroNodeSubQType());
					//cond.setLeft(lhs);
					//cond.setRight(rhs);
					String operator = sqn.subQType(sqn.getSubqueryType());
					//cond.setOperator(operator);
					// create the final subquery node and return it
					Node sqNode = new Node();
					sqNode.setType(Node.getBroNodeSubQType());
					sqNode.setSubQueryConds(thisSubQConds);
					sqNode.setLhsRhs(rhs);
					return sqNode;
				}
				else if(rc.getExpression() instanceof ColumnReference){
					//the result of subquery must be a single tuple
					System.out.println("CCCCCCCCCCCCCCCCCCCCCCCc");
					System.out.println("the result of subquery must be a single tuple");
				}
			}

			// subQueryConds.put(currentCounterVal, thisSubQConds);
		}*/
			/*
		else if(clause instanceof SimpleStringOperatorNode){
			SimpleStringOperatorNode sso=(SimpleStringOperatorNode) clause;
			Node n=new Node();
			n.setOperator(sso.getOperatorString());
			n.setLeft(getWhereClauseVector(sso.getOperand(),exposedName, fle,isWhereClause, queryType,qParser));
			n.setType(Node.getStringFuncNodeType());

			//Storing sub query details
			n.setQueryType(queryType);
			if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
			if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

			if( n.getLeft().getQueryType() != 0){//Aliased column name is used in outer query
				Util.addToSubQuery(n,qParser);
				n = null;
			}
			
			return n;
			*/
			//throw new Exception("getWhereClauseVector needs more programming \n"+clause.getClass()+"\n"+clause.toString());
			System.out.println("getWhereClauseVector needs more programming \n"+clause.getClass()+"\n"+clause.toString());
		}
		else if(clause instanceof Between){
			
			//FIXME: Mahesh If aggregate in where (due to aliased) then add to list of having clause of the subquery

			/*
			Between bn=(Between)clause;
			Node n=new Node();
			n.setType(Node.getAndNodeType());
			Node l=new Node();
			l.setLeft(getWhereClauseVector(bn.getLeftOperand(),exposedName,fle,isWhereClause, queryType,qParser));
			l.setOperator(">=");
			l.setRight(getWhereClauseVector(bn.getRightOperandList().getNodeVector().get(0),exposedName,fle,isWhereClause, queryType,qParser));
			l.setType(Node.getBroNodeType());
			n.setLeft(l);

			Node r=new Node();
			r.setLeft(getWhereClauseVector(bn.getLeftOperand(),exposedName,fle,isWhereClause, queryType,qParser));
			r.setOperator("<=");
			r.setRight(getWhereClauseVector(bn.getRightOperandList().getNodeVector().get(1),exposedName,fle,isWhereClause, queryType,qParser));
			r.setType(Node.getBroNodeType());
			n.setRight(r);

			//Storing sub query details
			n.setQueryType(queryType);
			if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
			if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

			return n;*/
			throw new Exception("getWhereClauseVector needs more programming \n"+clause.getClass()+"\n"+clause.toString());
		} else if (clause instanceof EqualsTo){
			BinaryExpression bne = (BinaryExpression)clause;
			Node n = new Node();
			n.setType(Node.getBroNodeType());
			n.setOperator("=");
			n.setLeft(getWhereClauseVector(bne.getLeftExpression(), exposedName, fle, isWhereClause, queryType,qParser));
			n.setRight(getWhereClauseVector(bne.getRightExpression(), exposedName, fle, isWhereClause, queryType,qParser));

			//Storing sub query details
			n.setQueryType(queryType);
			if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
			if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

			/*
			//FIXME: Mahesh when original subquery calls this then it is added to havingClause of sub query 
			if(n.getRight().getQueryType() != 0 || n.getLeft().getQueryType() != 0){//Aliased column name is used in outer query
				addToSubQuery(n);
				n = null;
			}*/
			
			return n;
		} else if (clause instanceof GreaterThan){
			GreaterThan broNode = (GreaterThan)clause;
			
			//BinaryRelationalOperatorNode broNode = ((BinaryRelationalOperatorNode) clause);			
			Node n = new Node();
			n.setType(Node.getBroNodeType());
			n.setOperator(qParser.cvcRelationalOperators[3]);
			n.setLeft(getWhereClauseVector(broNode.getLeftExpression(), exposedName, fle, isWhereClause, queryType,qParser));
			n.setRight(getWhereClauseVector(broNode.getRightExpression(), exposedName, fle, isWhereClause, queryType,qParser));

			//Storing sub query details
			n.setQueryType(queryType);
			if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
			if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

			/*
			//FIXME: Mahesh when original subquery calls this then it is added to havingClause of sub query 
			if(n.getRight().getQueryType() != 0 || n.getLeft().getQueryType() != 0){//Aliased column name is used in outer query
				addToSubQuery(n);
				n = null;
			}*/
			return n;
		}
		else if (clause instanceof MinorThan){
			BinaryExpression bne = (BinaryExpression)clause;
			Node n = new Node();
			n.setType(Node.getBroNodeType());
			n.setOperator("<");
			n.setLeft(getWhereClauseVector(bne.getLeftExpression(), exposedName, fle, isWhereClause, queryType,qParser));
			n.setRight(getWhereClauseVector(bne.getRightExpression(), exposedName, fle, isWhereClause, queryType,qParser));

			//Storing sub query details
			n.setQueryType(queryType);
			if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
			if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

			/*
			//FIXME: Mahesh when original subquery calls this then it is added to havingClause of sub query 
			if(n.getRight().getQueryType() != 0 || n.getLeft().getQueryType() != 0){//Aliased column name is used in outer query
				addToSubQuery(n);
				n = null;
			}*/
			
			return n;
		} else if (clause instanceof MinorThanEquals){
			BinaryExpression bne = (BinaryExpression)clause;
			Node n = new Node();
			n.setType(Node.getBroNodeType());
			n.setOperator("<=");
			n.setLeft(getWhereClauseVector(bne.getLeftExpression(), exposedName, fle, isWhereClause, queryType,qParser));
			n.setRight(getWhereClauseVector(bne.getRightExpression(), exposedName, fle, isWhereClause, queryType,qParser));

			//Storing sub query details
			n.setQueryType(queryType);
			if(queryType == 1) n.setQueryIndex(qParser.getFromClauseSubqueries().size()-1);
			if(queryType == 2) n.setQueryIndex(qParser.getWhereClauseSubqueries().size()-1);

			/*
			//FIXME: Mahesh when original subquery calls this then it is added to havingClause of sub query 
			if(n.getRight().getQueryType() != 0 || n.getLeft().getQueryType() != 0){//Aliased column name is used in outer query
				addToSubQuery(n);
				n = null;
			}*/
			
			return n;
		}
		else {
			throw new Exception("getWhereClauseVector needs more programming \n"+clause.getClass()+"\n"+clause.toString());
			//return new Node();
		}
		return null;
	}
	
}
