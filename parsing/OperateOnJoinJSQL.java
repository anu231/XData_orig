package parsing;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.derby.impl.sql.compile.AndNode;
import org.apache.derby.impl.sql.compile.BinaryRelationalOperatorNode;
import org.apache.derby.impl.sql.compile.FromBaseTable;
import org.apache.derby.impl.sql.compile.FromSubquery;
import org.apache.derby.impl.sql.compile.HalfOuterJoinNode;
import org.apache.derby.impl.sql.compile.JoinNode;
import org.apache.derby.impl.sql.compile.LikeEscapeOperatorNode;
import org.apache.derby.impl.sql.compile.QueryTreeNode;
import org.apache.derby.impl.sql.compile.ResultColumn;
import org.apache.derby.impl.sql.compile.ResultColumnList;
import org.apache.derby.impl.sql.compile.ValueNode;

import parsing.Column;
import parsing.FromListElement;
import parsing.JoinClause;
import parsing.JoinClauseInfo;
import parsing.JoinClauseNew;
import parsing.JoinTreeNode;
import parsing.Node;

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
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

public class OperateOnJoinJSQL {


	public static FromListElement OperateOnJoinNode(Join joinNode,FromItem rt,FromItem lt,
			String aliasName, Vector<Node> allConds, JoinTreeNode jtn, boolean isFromSubquery, boolean isWhereSubquery, QueryParser qParser)
					throws Exception {
		Vector<Column> joinColumns1 = null;
		Vector<Column> joinColumns2 = null;
		FromListElement jtan = new FromListElement();
		jtan.setAliasName(null);
		jtan.setTableName(null);

		FromListElement rightF = new FromListElement();
		FromListElement leftF = new FromListElement();

		Vector<FromListElement> t = new Vector<FromListElement>();
		Vector<FromListElement> leftFle = new Vector<FromListElement>();
		Vector<FromListElement> rightFle = new Vector<FromListElement>();

		String joinType = null;

		if (joinNode.isOuter()) { // checks if join is
			// HalfOuterJoin
			if (joinNode.isRight()) // checks f join type is
				// right outer join
				joinType = JoinClauseInfo.rightOuterJoin;
			else
				joinType = JoinClauseInfo.leftOuterJoin;
		} else {
			// if(joinNode.getJoinClause().getTypeId() == 1);
			joinType = JoinClauseInfo.innerJoin; // full outer join is not
			// wroking .. so assume all
			// others are inner join
		}

		// JoinTree processing

		jtn.setNodeType(joinType);
		if (joinType.equalsIgnoreCase(JoinClauseInfo.innerJoin)) {
			jtn.setInnerJoin(true);
		} else{
			jtn.setInnerJoin(false);
		}

		JoinTreeNode leftChild = new JoinTreeNode();
		JoinTreeNode rightChild = new JoinTreeNode();
		// if left child is a join node then recurssively traverse the node
		/*if (joinNode.getLeftResultSet() instanceof JoinNode) {
			FromListElement temp = new FromListElement();

			temp = OperateOnJoinNode((JoinNode) joinNode.getLeftResultSet(),
					aliasName, allConds, leftChild, isFromSubquery,isWhereSubquery, qParser);
			leftFle.add(temp);
			t.add(temp);
		} else*/
		if (lt instanceof net.sf.jsqlparser.schema.Table) {
			FromListElement temp = OperateOnBaseTable.OperateOnBaseTableJSQL(lt
					, true, aliasName, leftChild,qParser, isFromSubquery, isWhereSubquery);
			t.add(temp);
			leftFle.add(temp);
			//Util.updateTableOccurrences(isFromSubquery, isWhereSubquery, temp,qParser);
		} else if (lt instanceof SubSelect) {
			FromListElement temp;
			//temp = 
			OperateOnSubQueryJSQL.OperateOnSubquery( (SubSelect)lt, allConds, leftChild, true, false,qParser);
			temp=qParser.getFromClauseSubqueries().get(qParser.getFromClauseSubqueries().size()-1).getQueryAliases();
			t.add(temp);
			leftFle.add(temp);
			leftF.setTabs(leftFle);
		}

		// if right child is a join node then recurssively traverse the node
		/*if (joinNode.getRightResultSet() instanceof JoinNode) {
			FromListElement temp = new FromListElement();
			temp = OperateOnJoinNode((JoinNode) joinNode.getRightResultSet(),
					aliasName, allConds, rightChild, isFromSubquery,isWhereSubquery,qParser);
			t.add(temp);
			rightFle.add(temp);
			//Util.updateTableOccurrences(isFromSubquery, isWhereSubquery, temp,qParser);
		} else*/
		if (rt instanceof net.sf.jsqlparser.schema.Table) {
			FromListElement temp;
			temp = OperateOnBaseTable.OperateOnBaseTableJSQL(rt
					, true, aliasName, rightChild, qParser, isFromSubquery, isWhereSubquery);
			t.add(temp);
			rightFle.add(temp);
		} else if (rt instanceof SubSelect) {
			FromListElement temp;
			//temp = 
			OperateOnSubQueryJSQL.OperateOnSubquery((SubSelect) rt, allConds, rightChild,true,false,qParser);
			temp=qParser.getFromClauseSubqueries().get(qParser.getFromClauseSubqueries().size()-1).getQueryAliases();
			t.add(temp);
			rightFle.add(temp);
			rightF.setTabs(rightFle);
		}

		jtan.setTabs(t);

		Vector<JoinClause> joinClauses = new Vector<JoinClause>();
		JoinClauseNew joinClauseNew = new JoinClauseNew(joinType);
		JoinClauseInfo joinClauseInfo;
		Vector<Node > leftSet, rightSet, set;//To handle aliased columns involved in joins
		leftSet = new Vector<Node>();
		rightSet = new Vector<Node>();
		set = new Vector<Node>();

		if (joinNode.getUsingColumns() != null || joinNode.isNatural()) {

			//ResultColumnList columnList=null;
			List<net.sf.jsqlparser.schema.Column> columnList = null;
			List<String> li = null;
			//FIXME: mahesh...what if aliased name and original name are same ??Then that join condition is stored in both lists (join columns and left/rightSet)

			/*
			 * If any side of join is sub query then 
			 * the join conditions should contain only the projected columns, which
			 * may be aliased,  of the sub query
			 */

			if (rt instanceof SubSelect && lt instanceof SubSelect ){//If sub query then we should add only the projected columns

				li = getCommonColumnsForNaturalJoin(qParser.getFromClauseSubqueries().get(qParser.getFromClauseSubqueries().size()-1).getProjectedCols(),qParser.getFromClauseSubqueries().get(qParser.getFromClauseSubqueries().size()-2).getProjectedCols());
				//l = getJoinCondsWithColumnAliases(l, joinNode, jtn, jtan);//Get join conditions with aliased names
				//get left and right aliased set
				set = Util.getAliasedNodes(leftFle, li,qParser);
				if(set != null)
					leftSet = set;
				set = Util.getAliasedNodes(rightFle, li,qParser);
				if(set != null)
					rightSet = set;
				//columnList=joinNode.getUsingColumns(l);//FIXME
				
			}
			else if (rt instanceof SubSelect ) {//If sub query then we should add only the projected columns

				li = getCommonColumnsForNaturalJoin(t.get(0), qParser.getFromClauseSubqueries().get(qParser.getFromClauseSubqueries().size()-1).getProjectedCols(),qParser); 
				//l = getJoinCondsWithColumnAliases(l, joinNode, jtn, jtan);//Get join conditions with aliased names
				//get left and right aliased set
				set = Util.getAliasedNodes(leftFle, li,qParser);
				if(set != null)
					leftSet = set;
				set = Util.getAliasedNodes(rightFle, li,qParser);
				if(set != null)
					rightSet = set;
				//columnList=joinNode.getUsingColumns();
			}
			else if( lt instanceof SubSelect ){

				li = getCommonColumnsForNaturalJoin(t.get(1), qParser.getFromClauseSubqueries().get(qParser.getFromClauseSubqueries().size()-1).getProjectedCols(),qParser); 
				//l = getJoinCondsWithColumnAliases(l, joinNode, jtn, jtan);//Get join conditions with aliased names
				//get left aliased set
				set = Util.getAliasedNodes(leftFle, li,qParser);
				if(set != null)
					leftSet = set;
				set = Util.getAliasedNodes(rightFle, li,qParser);
				if(set != null)
					rightSet = set;
				//columnList=joinNode.getUsingColumns();
			}
			else if(joinNode.getUsingColumns() != null){
				columnList = new ArrayList<net.sf.jsqlparser.schema.Column>(joinNode.getUsingColumns());
				//FIXME ANURAG
				//jtn.setUsingClause(columnList);

			}else{
				li=getCommonColumnsForNaturalJoin(t,qParser);
				//columnList=joinNode.getCommonColumnsForNaturalJoin(l);//FIXME ANURAG				
			}
			
			if (columnList==null){
				//populate column list
				columnList = new ArrayList<net.sf.jsqlparser.schema.Column>();
				for (int j=0; j<li.size(); j++){
					net.sf.jsqlparser.schema.Table tempTab = new net.sf.jsqlparser.schema.Table("null","null"); 
					columnList.add(new net.sf.jsqlparser.schema.Column(tempTab,li.get(j)));
				}
			}
			
			Node joinCond = null;

			for (int i = 0; i < columnList.size(); i++) {

				net.sf.jsqlparser.schema.Column column = columnList.get(i);
				joinColumns1 = Util.getJoinColumnsJSQL(column.getColumnName().toUpperCase(), lt,qParser);
				joinColumns2 = Util.getJoinColumnsJSQL(column.getColumnName().toUpperCase(), rt,qParser);
				/*
				 * Here Cross join is needed. This is because a using clause
				 * does not really specify what tables it is joining. We may
				 * have A Join B Using(x). Now when we join (A Join B) with
				 * another able say R Using (x), we cannot say which x is
				 * involved in the join because from (A Join B), we get only one
				 * x. Hence we need to specify 2 join conditions as: A.x = R.x
				 * and B.x = R.x. These joins are just used to build equivalence
				 * classes.
				 */

				//FIXME: Mahesh add join conditions to the sub queries separately
				for (int j = 0; j < joinColumns1.size(); j++) {
					for (int k = 0; k < joinColumns2.size(); k++) {
						JoinClause joinClause = null;
						// TODO
						joinClauseInfo = new JoinClauseInfo(
								joinColumns1.get(j), joinColumns2.get(k),
								JoinClauseInfo.equiJoinType);
						joinClauseNew.add(joinClauseInfo);
						qParser.getJoinClauseInfoVector().add(joinClauseInfo);

						boolean leftSubquery = false;
						/*
						 * Get equi joins also in the Node form
						 */
						Column leftCol = joinColumns1.get(j);
						Node left = new Node();
						if (lt instanceof SubSelect){ 
							left = Util.getColumnFromOccurenceInJC(leftCol
									.getColumnName(), leftCol.getTableName(), leftF, qParser);
							leftSubquery = true;
						}
						else{
							left = Util.getColumnFromOccurenceInJC(leftCol
									.getColumnName(), leftCol.getTableName(), jtan,qParser);
						}

						left.setTableAlias("");
						left.setTable(leftCol.getTable());
						left.setType(Node.getColRefType());
						left.setLeft(null);
						left.setRight(null);

						Util.setQueryTypeAndIndex(isFromSubquery, isWhereSubquery,
								leftSubquery, left,qParser);

						boolean rightSubquery =false;

						Column rightCol = joinColumns2.get(k);
						Node right = new Node();
						if (rt instanceof SubSelect){ 
							right = Util.getColumnFromOccurenceInJC(rightCol
									.getColumnName(), rightCol.getTableName(), rightF, qParser);
							rightSubquery = true;
						}
						else{
							right = Util.getColumnFromOccurenceInJC(rightCol
									.getColumnName(), rightCol.getTableName(), jtan, qParser);
						}
						right.setTableAlias("");
						right.setTable(rightCol.getTable());
						right.setType(Node.getColRefType());
						right.setLeft(null);
						right.setRight(null);


						Util.setQueryTypeAndIndex(isFromSubquery, isWhereSubquery,
								rightSubquery, right,qParser);

						Node usingJoin = new Node();
						usingJoin.setType(Node.getBroNodeType());
						usingJoin.setOperator("=");
						usingJoin.setLeft(left);
						usingJoin.setRight(right);
						usingJoin.setJoinType(joinType);
						Util.setQueryTypeAndIndex(isFromSubquery, isWhereSubquery,
								rightSubquery, usingJoin,qParser);
						if(joinCond==null)
							joinCond=usingJoin;
						else{
							Node Temp= new Node();
							Temp.setType( Node.getAndNodeType());
							Temp.setLeft(joinCond);
							Temp.setRight(usingJoin);
							Util.setQueryTypeAndIndex(isFromSubquery, isWhereSubquery,
									rightSubquery, Temp, qParser);
							joinCond=Temp;
						}
						//allConds.add(usingJoin);//FIXME: need to change this to correctly
						jtn.addJoinPred(usingJoin);
						//FIXME: Mahesh cross product this left and right with left and right aliased nodes
						//Need to update joinClauseInfo (get column from Node). What about aggregate Node??
						for(int l=0; l<leftSet.size(); l++){
							Node n = new Node();
							n = leftSet.get(l);

							if(n.getType().equalsIgnoreCase(Node.getAggrNodeType()))
								joinClauseInfo = new JoinClauseInfo(
										joinColumns1.get(j), n.getAgg().getAggExp().getColumn() ,//FIXME: Second column name, we are not storing aggregate name
										JoinClauseInfo.equiJoinType);

							else
								joinClauseInfo = new JoinClauseInfo(
										joinColumns1.get(j), n.getColumn(),//FIXME: If aggregate Node
										JoinClauseInfo.equiJoinType);
							joinClauseNew.add(joinClauseInfo);
							qParser.getJoinClauseInfoVector().add(joinClauseInfo);

							usingJoin = new Node();
							usingJoin.setType(Node.getBroNodeType());
							usingJoin.setOperator("=");
							usingJoin.setLeft(left);
							usingJoin.setRight(n);
							usingJoin.setJoinType(joinType);
							allConds.add(usingJoin);
							jtn.addJoinPred(usingJoin);

						}
						for(int m=0; m<rightSet.size(); m++){
							Node n = new Node();
							n = rightSet.get(m);

							if(n.getType().equalsIgnoreCase(Node.getAggrNodeType()))
								joinClauseInfo = new JoinClauseInfo(
										n.getAgg().getAggExp().getColumn() ,joinColumns2.get(j),//FIXME: Second column name, we are not storing aggregate name
										JoinClauseInfo.equiJoinType);

							else
								joinClauseInfo = new JoinClauseInfo(
										n.getColumn() , joinColumns2.get(k),//FIXME: If aggregate Node
										JoinClauseInfo.equiJoinType);
							joinClauseNew.add(joinClauseInfo);
							qParser.getJoinClauseInfoVector().add(joinClauseInfo);

							usingJoin = new Node();
							usingJoin.setType(Node.getBroNodeType());
							usingJoin.setOperator("=");
							usingJoin.setLeft(n);
							usingJoin.setRight(right);
							usingJoin.setJoinType(joinType);
							allConds.add(usingJoin);
							jtn.addJoinPred(usingJoin);
						}
					}
				}

			}
			allConds.add(joinCond);
		} else if (joinNode.getOnExpression()!=null) {
			/*
			 * For join conditions specified as "Join On (Expr)" where Expr is
			 * an expression involving attributes from the two children of the
			 * join. R.a = S.b + 1 R.a = S.b + T.c R.a + Q.d = S.b + T.c R.a +
			 * Q.d -1 = S.b + T.c + 1 etc.
			 */
			// TODO modify the equivalence class in case of subquery
			//Vector<BinaryRelationalOperatorNode> nodes = null;
			//nodes = operateOnJoinClause(joinNode.getJoinClause());
			//ValueNode preds = joinNode.getJoinClause();
			Expression preds = joinNode.getOnExpression();
			//eqCreation(joinNode, preds, jtan);




			Node n = new Node();
			n = WhereClauseVectorJSQL.getWhereClauseVector(preds, null,jtan,false,0,qParser);//mahesh:fix this null and 0
			allConds.add(n);

			jtn.addJoinPred(n);
			//jtn.setOnNode(preds);FIXME ANURAG
		}
		//JoinTreeNode processing
		jtn.setLeft(leftChild);
		jtn.setRight(rightChild);

		//To handle left and right set cross product
		for(int i=0; i<leftSet.size(); i++){
			for(int j=0; j<rightSet.size(); j++){

				Node n = new Node();
				n = leftSet.get(i);
				Node n1 = new Node();
				n1 = rightSet.get(j);

				Column c1,c2;

				if(n.getType().equalsIgnoreCase(Node.getAggrNodeType()))
					c1 = n.getAgg().getAggExp().getColumn();
				else 
					c1 = n.getColumn();

				if(n1.getType().equalsIgnoreCase(Node.getAggrNodeType()))
					c2 = n1.getAgg().getAggExp().getColumn();
				else 
					c2 = n1.getColumn();

				joinClauseInfo = new JoinClauseInfo(
						c1, c2,//FIXME: If aggregate Node
						JoinClauseInfo.equiJoinType);


				joinClauseNew.add(joinClauseInfo);
				qParser.getJoinClauseInfoVector().add(joinClauseInfo);

				Node usingJoin = new Node();
				usingJoin.setType(Node.getBroNodeType());
				usingJoin.setOperator("=");
				usingJoin.setLeft(n);
				usingJoin.setRight(n1);
				usingJoin.setJoinType(joinType);
				allConds.add(usingJoin);
				jtn.addJoinPred(usingJoin);

			}
		}

		return jtan;
	}

	//If one side of join is sub query
	public static List<String> getCommonColumnsForNaturalJoin(FromListElement f1, Vector<Node> projectedCols,QueryParser qParser) throws Exception{//one part sub query and other part base query
		//get columns of right base table
		ArrayList <String> tableColumn1, tableColumn2;
		tableColumn1 = new ArrayList<String>();
		tableColumn2 = new ArrayList<String>();    	

		Vector<FromListElement> fle = new Vector<FromListElement>();
		fle.add(f1);
		tableColumn1 = Util.getAllColumnofElement(fle,qParser);

		tableColumn2 = Util.getColumnNames(projectedCols);

		tableColumn1.retainAll(tableColumn2);

		return tableColumn1;
	}

	//If both sides of join are sub queries
	public static List<String> getCommonColumnsForNaturalJoin(	Vector<Node> projectedCols1, Vector<Node> projectedCols2) throws Exception{
		ArrayList <String> tableColumn1, tableColumn2;
		tableColumn1 = new ArrayList<String>();
		tableColumn2 = new ArrayList<String>();

		tableColumn1 = Util.getColumnNames(projectedCols1);
		tableColumn2 = Util.getColumnNames(projectedCols2);

		tableColumn1.retainAll(tableColumn2);			

		return tableColumn1;
	}

	//FIXME: Mahesh....change this function to include only the columns that are projected by from clause subqueries
	public static List<String> getCommonColumnsForNaturalJoin(Vector<FromListElement> t,QueryParser qParser) throws Exception{    	

		ArrayList <String>tableColumn1,tableColumn2;
		tableColumn1=new ArrayList<String>();    		
		tableColumn2=new ArrayList<String>();


		FromListElement f1=(FromListElement)t.get(0);
		FromListElement f2=(FromListElement)t.get(1);

		if(f1.tableName != null && f2.tableName!=null){

			for(String columnName : qParser.getTableMap().getTable(f1.tableName.toUpperCase()).getColumns().keySet()){
				tableColumn1.add(columnName);            	
			}			
			for(String columnName : qParser.getTableMap().getTable(f2.tableName.toUpperCase()).getColumns().keySet()){
				tableColumn2.add(columnName);            	
			}
			tableColumn1.retainAll(tableColumn2);			
			return tableColumn1;
		}else if(f1.tableName==null && f2.tableName!=null){

			tableColumn1=Util.getAllColumnofElement(f1.getTabs(),qParser);
			for(String columnName : qParser.getTableMap().getTable(f2.tableName.toUpperCase()).getColumns().keySet()){
				tableColumn2.add(columnName);            	
			}
			tableColumn1.retainAll(tableColumn2);			
			return tableColumn1;				

		}else if(f1.tableName!=null && f2.tableName==null){

			for(String columnName : qParser.getTableMap().getTable(f1.tableName.toUpperCase()).getColumns().keySet()){
				tableColumn1.add(columnName);            	
			}			
			tableColumn2=Util.getAllColumnofElement(f2.getTabs(),qParser);			
			tableColumn1.retainAll(tableColumn2);			
			return tableColumn1;							
		}
		tableColumn1 = Util.getAllColumnofElement(f1.getTabs(),qParser);
		tableColumn2=Util.getAllColumnofElement(f1.getTabs(),qParser);
		tableColumn1.retainAll(tableColumn2);			
		return tableColumn1;

	}

	// returns all the Binary operator node from the query tree node
	private static Vector<BinaryRelationalOperatorNode> operateOnJoinClause(
			QueryTreeNode node) {
		Vector<BinaryRelationalOperatorNode> nodes = new Vector<BinaryRelationalOperatorNode>();
		if (node instanceof BinaryRelationalOperatorNode) {
			nodes.add((BinaryRelationalOperatorNode) node);
		} else if(node instanceof LikeEscapeOperatorNode) {
			//nodes.add((LikeEscapeOperatorNode) node);
		}
		else {
			// TODO consider OR nodes also??
			AndNode andNode = (AndNode) node;
			// recurssively call for left node and right node
			Vector<BinaryRelationalOperatorNode> leftNodes = operateOnJoinClause(andNode
					.getLeftOperand());
			Vector<BinaryRelationalOperatorNode> rightNodes = operateOnJoinClause(andNode
					.getRightOperand());
			// add the leftnodes to nodes
			for (BinaryRelationalOperatorNode bNode : leftNodes)
				nodes.add(bNode);
			// add the rightnodes to nodes
			for (BinaryRelationalOperatorNode bNode : rightNodes)
				nodes.add(bNode);
		}
		return nodes;
	}

}
