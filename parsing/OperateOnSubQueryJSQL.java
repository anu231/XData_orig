package parsing;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import parsing.FromListElement;
import parsing.JoinTreeNode;
import parsing.Node;
import parsing.QueryParser;
import parsing.Table;
import util.TableMap;
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

public class OperateOnSubQueryJSQL {

	public static FromListElement OperateOnSubquery(SubSelect subquery,Vector<Node> allConds, JoinTreeNode jtn,boolean fromSubquery, boolean whereSubquery, parsing.QueryParser qParser) throws Exception {

		String aliasName = subquery.getAlias();
		FromListElement sqa = new FromListElement();
		Vector<FromListElement> t = new Vector<FromListElement>();
		Vector<Node> tempProjectedCols = new Vector<Node>();
		//ValueNode sqWhereClause=null;
		Expression sqWhereClause=null;

		Node whereClausePred = new Node();
		//	mahesh: create new query parser for from clause subquery
		parsing.QueryParser fromClause=new parsing.QueryParser(qParser.getTableMap());
		parsing.QueryParser whereClause=new parsing.QueryParser(qParser.getTableMap());
		int queryType = 0;
		if(fromSubquery) queryType = 1;
		if(whereSubquery) queryType = 2;

		if(fromSubquery){
			fromClause.queryAliases=new FromListElement();
			fromClause.queryAliases.setAliasName(subquery.getAlias());
			fromClause.queryAliases.setTableName(null);
			qParser.getFromClauseSubqueries().add(fromClause);
			qParser.getSubQueryNames().put(aliasName, qParser.getFromClauseSubqueries().size()-1);
		}
		if(whereSubquery){
			whereClause.queryAliases=new FromListElement();
			whereClause.queryAliases.setAliasName(subquery.getAlias());
			whereClause.queryAliases.setTableName(null);
			qParser.getWhereClauseSubqueries().add(whereClause);
		}
		//FIXME
		/*
		if(subquery.getSubquery() instanceof IntersectOrExceptNode ){//FIXME currently doesn't handle intersect or Except
			Vector<Node> leftSubQConds = new Vector<Node>();
			Vector<Node> rightSubQConds = new Vector<Node>();
			IntersectOrExceptNode Exceptnode = ((IntersectOrExceptNode) subquery.getSubquery());
			SelectNode leftselect=(SelectNode) Exceptnode.getLeftResultSet();
			SelectNode rightselect=(SelectNode) Exceptnode.getRightResultSet();
			//	leftSubQConds = OperateOnSubquery(leftselect,new JoinTreeNode());
			//rightSubQConds = OperateOnSubquery(rightselect,new JoinTreeNode());
			FromListElement leftSqa = new FromListElement();
			leftSqa.setAliasName(subquery.getCorrelationName());
			leftSqa.setTableName(null);
			Vector<FromListElement> leftT = new Vector<FromListElement>();

			FromList leftSqFromList = leftselect.getFromList();
			Vector<QueryTreeNode> leftSqFromTableList = leftSqFromList.getNodeVector();

			for (int i = 0; i < leftSqFromTableList.size(); i++) {
				if (leftSqFromTableList.get(i) instanceof FromBaseTable) {
					FromBaseTable fbt = (FromBaseTable) leftSqFromTableList.get(i);
					FromListElement temp = OperateOnBaseTable.OperateOnBaseTable((FromBaseTable) leftSqFromTableList.get(i), false,subquery.getCorrelationName(),new JoinTreeNode(),qParser, fromSubquery, whereSubquery);
					leftT.add(temp);
				} else if (leftSqFromTableList.get(i) instanceof JoinNode) {
					FromListElement temp = new FromListElement();
					//	temp = OperateOnJoinNode((JoinNode) leftSqFromTableList.get(i),	subquery.getCorrelationName(), fromClause.allConds,jtn);
					leftT.add(temp);
				} else if (leftSqFromTableList.get(i) instanceof FromSubquery) {
					FromListElement temp = new FromListElement();
					//	temp = OperateOnSubquery((FromSubquery) sqFromTableList.get(i),allConds,jtn);

					leftT.add(temp);
				}
			}

			leftSqa.setTabs(leftT);

			ValueNode leftSqWhereClause = leftselect.getWhereClause();
			if (leftSqWhereClause != null) {
				Node leftwhereClausePred = new Node();
				leftwhereClausePred = WhereClauseVector.getWhereClauseVector(leftSqWhereClause, null, leftSqa, whereSubquery, queryType,qParser);
				leftSubQConds.add(leftwhereClausePred);
			}

			FromListElement rightSqa = new FromListElement();
			rightSqa.setAliasName(subquery.getCorrelationName());
			rightSqa.setTableName(null);
			Vector<FromListElement> rightT = new Vector<FromListElement>();

			FromList rightSqFromList = rightselect.getFromList();
			Vector<QueryTreeNode> rightSqFromTableList = rightSqFromList.getNodeVector();

			for (int i = 0; i < rightSqFromTableList.size(); i++) {
				if (rightSqFromTableList.get(i) instanceof FromBaseTable) {
					FromBaseTable fbt = (FromBaseTable) rightSqFromTableList.get(i);
					FromListElement temp = OperateOnBaseTable.OperateOnBaseTable((FromBaseTable) rightSqFromTableList.get(i), false,subquery.getCorrelationName(),new JoinTreeNode(),qParser, false, false);
					rightT.add(temp);
				} else if (rightSqFromTableList.get(i) instanceof JoinNode) {
					FromListElement temp = new FromListElement();
					//	temp = OperateOnJoinNode((JoinNode) leftSqFromTableList.get(i),	subquery.getCorrelationName(), fromClause.allConds,jtn);
					rightT.add(temp);
				} else if (rightSqFromTableList.get(i) instanceof FromSubquery) {
					FromListElement temp = new FromListElement();
					//	temp = OperateOnSubquery((FromSubquery) sqFromTableList.get(i),allConds,jtn);

					rightT.add(temp);
				}
			}

			rightSqa.setTabs(rightT);

			ValueNode rightSqWhereClause = rightselect.getWhereClause();
			if (rightSqWhereClause != null) {
				Node rightwhereClausePred = new Node();
				rightwhereClausePred = WhereClauseVector.getWhereClauseVector(rightSqWhereClause, null, rightSqa, whereSubquery, queryType,qParser);
				rightSubQConds.add(rightwhereClausePred);
			}

			//	System.out.println(leftselect.statementToString());
			//	leftQuery
			ResultColumnList leftRcList = leftselect.getResultColumns();
			ResultColumnList rightRcList = rightselect.getResultColumns();
			Vector<Node> leftProjectedCols = new Vector<Node>();
			Vector<Node> rightProjectedCols = new Vector<Node>();
			for (int k=0;k<leftRcList.size();k++) {

				if (leftRcList.getNodeVector().get(k) instanceof AllResultColumn) {
					leftProjectedCols.addAll(Util.addAllProjectedColumns(leftSqa,queryType,qParser));
				}
				else if (leftRcList.getNodeVector().get(k) instanceof ResultColumn){				
					ResultColumn rc = (ResultColumn)leftRcList.getNodeVector().get(k);
					ValueNode exp = rc.getExpression();			
					String exposedName=rc.getName();
					//	projectedCols.add(getWhereClauseVector(exp, sqa,false));
					leftProjectedCols.add(WhereClauseVector.getWhereClauseVector(exp,exposedName, leftSqa, whereSubquery, queryType,qParser));
				}
			}
			for (int k=0;k<rightRcList.size();k++) {

				if (rightRcList.getNodeVector().get(k) instanceof AllResultColumn) {
					rightProjectedCols.addAll(Util.addAllProjectedColumns(rightSqa,queryType,qParser));
				}
				else if (rightRcList.getNodeVector().get(k) instanceof ResultColumn){				
					ResultColumn rc = (ResultColumn)rightRcList.getNodeVector().get(k);
					ValueNode exp = rc.getExpression();			
					String exposedName=rc.getName();
					//	projectedCols.add(getWhereClauseVector(exp, sqa,false));
					rightProjectedCols.add(WhereClauseVector.getWhereClauseVector(exp,exposedName, rightSqa, whereSubquery, queryType,qParser));
				}
			}
			System.out.println(leftselect.toString());
			parsing.QueryParser leftQueryParser = new parsing.QueryParser(qParser.getTableMap());
			parsing.QueryParser rightQuryParser = new parsing.QueryParser(qParser.getTableMap());
			leftQueryParser.queryAliases=new FromListElement();
			rightQuryParser.queryAliases=new FromListElement();
			leftQueryParser.queryAliases.setTableName(null);
			rightQuryParser.queryAliases.setTableName(null);
			leftQueryParser.allConds=leftSubQConds;
			rightQuryParser.allConds=rightSubQConds;
			for(Node pc:leftProjectedCols)
				leftQueryParser.projectedCols.add(pc);
			for(Node pc:rightProjectedCols)
				rightQuryParser.projectedCols.add(pc);
			if(whereSubquery){
				whereClause.leftQuery=leftQueryParser;
				whereClause.rightQuery=rightQuryParser;
				whereClause.isIntersectOrExcept=true;
			}
			else if(fromSubquery){
				fromClause.leftQuery=leftQueryParser;
				fromClause.rightQuery=rightQuryParser;
				whereClause.isIntersectOrExcept=true;
			}
		}
		else*/ {

			sqa.setAliasName(subquery.getAlias());
			sqa.setTableName(null);

			//FromList sqFromList = subquery.getSubquery().getFromList();
			//Vector<QueryTreeNode> sqFromTableList = sqFromList.getNodeVector();

			//	JoinTree
			jtn.setNodeAlias(subquery.getAlias());

			/*for (int i = 0; i < sqFromTableList.size(); i++) {
				if (sqFromTableList.get(i) instanceof FromBaseTable) {
					FromBaseTable fbt = (FromBaseTable) sqFromTableList.get(i);
					FromListElement temp = OperateOnBaseTable.OperateOnBaseTable((FromBaseTable) sqFromTableList.get(i), false,aliasName,jtn, qParser, fromSubquery, whereSubquery);
					t.add(temp);
					//Util.updateTableOccurrences(fromSubquery, whereSubquery, temp,qParser);
				} else if (sqFromTableList.get(i) instanceof JoinNode) {
					FromListElement temp = new FromListElement();
					temp = OperateOnJoin.OperateOnJoinNode((JoinNode) sqFromTableList.get(i),	aliasName, (fromSubquery)?fromClause.allConds:whereClause.allConds,jtn, fromSubquery, whereSubquery, qParser);
					t.add(temp);
				} else if (sqFromTableList.get(i) instanceof FromSubquery) {
					FromListElement temp = new FromListElement();
					//	temp = OperateOnSubquery((FromSubquery) sqFromTableList.get(i),allConds,jtn);

					t.add(temp);
				}
			}*/
			PlainSelect plainSelect = (PlainSelect)subquery.getSelectBody();
			FromItem prev = plainSelect.getFromItem();
			if (plainSelect.getJoins()==null){
				//only one item
				FromListElement temp = OperateOnBaseTable.OperateOnBaseTableJSQL(prev,false, aliasName, jtn, qParser, fromSubquery, whereSubquery);
				t.add(temp);
			} else {
				for (Iterator joinsIt = plainSelect.getJoins().iterator(); joinsIt.hasNext();) {
					Join join = (Join) joinsIt.next();
					FromItem rt = join.getRightItem();
					FromListElement temp = OperateOnJoinJSQL.OperateOnJoinNode(join,rt,prev, aliasName,(fromSubquery)?fromClause.allConds:whereClause.allConds, jtn, fromSubquery, whereSubquery,qParser);
					t.add(temp);
					prev = rt;
				}
			}
			//	if(!fromSubquery)
			sqa.setTabs(t);
			//	else if(fromSubquery)
			//	fromClause.queryAliases.setTabs(t);


			// 	Geting SelectionClause and JoinClause equalities		
			sqWhereClause = plainSelect.getWhere();
			/*
			 * 	This will also collect data for killing IN clause mutants
			 */
			if (sqWhereClause != null) {
				//		if(!fromSubquery){

				whereClausePred = WhereClauseVectorJSQL.getWhereClauseVector(sqWhereClause,null, sqa ,true, queryType, qParser);
				//	allConds.add(whereClausePred);//FIXME: Mahesh change here
				//}
				//	else if (fromSubquery){
				//			whereClausePred = getWhereClauseVector(sqWhereClause,null, fromClause.queryAliases,true);

				//		fromClause.allConds.add(whereClausePred);
				//	}	

			}
			/*
			//get equivalence classes of this subquery
			fromClause.equivalenceClasses.addAll(createEquivalenceClasses());//FIXME: modify this mahesh
			 */
			//FIXME: Mahesh Add code to collect projected columns,groupby and having clause of subquery
			//ResultSetNode rs=subquery.getSubquery();//FIXME
			//System.out.println("hello"+rs);
			WhereClauseVectorJSQL.getAggregationDataStructures(plainSelect, sqa, qParser.getQuery().getFromTables(),fromSubquery,whereSubquery,qParser);
			/*getAggregationDataStructures(rsNode, queryAliases, query.getFromTables());

			if (debug) {
			System.out.println("\nJoin Tables : " + query.getJoinTables());
			System.out.println("Undirected Join Graph : "
					+ query.getJoinGraph());
			}

			 */

			//	Add projected columns	for this subquery	
			//ResultColumnList rcList = rs.getResultColumns();
			List<SelectItem> rcList = plainSelect.getSelectItems();
			
			
			for (int k=0;k<rcList.size();k++) {

				if (rcList.get(k) instanceof AllColumns) {
					tempProjectedCols.addAll(Util.addAllProjectedColumns(sqa, queryType,qParser));
				}
				else if (rcList.get(k) instanceof SelectExpressionItem){				
					SelectExpressionItem rc = (SelectExpressionItem)rcList.get(k);
					Expression exp = rc.getExpression();			
					String exposedName=rc.getAlias();
					//	projectedCols.add(getWhereClauseVector(exp, sqa,false));
					tempProjectedCols.add(WhereClauseVectorJSQL.getWhereClauseVector(exp,exposedName, sqa,false, queryType,qParser));

					if(qParser.isUpdateNode){
						//FIXME
						//qParser.updateColumn.add(WhereClauseVectorJSQL.getWhereClauseVector(rc.getReference(),rc.getName(),qParser.queryAliases,false, queryType,qParser));					
					}


				}
			}
			if(whereSubquery){
				if(sqWhereClause != null && whereClausePred != null){
					if(whereClause.allConds.isEmpty())
						whereClause.allConds.add(whereClausePred);
					else {
						Node n1=whereClause.allConds.get(0);
						Node n2=whereClausePred;
						Node n=new Node();
						n.setLeft(n1);
						n.setRight(n2);
						n.setType(Node.getAndNodeType());
						whereClause.allConds.removeAllElements();
						whereClause.allConds.add(n);
					}
				}
				for(Node pc: tempProjectedCols)
					whereClause.projectedCols.add(pc);
				whereClause.queryAliases.setTabs(t);
			}
			else if(fromSubquery){
				if(sqWhereClause != null && whereClausePred != null) {
					if(fromClause.allConds.isEmpty())
						fromClause.allConds.add(whereClausePred);
					else {
						Node n1=fromClause.allConds.get(0);
						Node n2=whereClausePred;
						Node n=new Node();
						n.setLeft(n1);
						n.setRight(n2);
						n.setType(Node.getAndNodeType());
						fromClause.allConds.removeAllElements();
						fromClause.allConds.add(n);
					}
				}
				for(Node pc: tempProjectedCols)
					fromClause.projectedCols.add(pc);
				fromClause.queryAliases.setTabs(t);
			}
			else
			{
				if(sqWhereClause != null) allConds.add(whereClausePred);
				for(Node pc: tempProjectedCols)
					qParser.getProjectedCols().add(pc);
				//	this.ProjectedColumns.add(tempProjectedCols);

			}
		}
		//	add this to list

		//FromClauseSubqueries.add(fromClause);

		return sqa;
	}
}
