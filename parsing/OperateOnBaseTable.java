package parsing;

import org.apache.derby.impl.sql.compile.FromBaseTable;

import parsing.Table;
import parsing.JoinTreeNode;
import parsing.FromListElement;

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
import net.sf.jsqlparser.statement.select.*;

public class OperateOnBaseTable {


	public static FromListElement OperateOnBaseTableJSQL(FromItem node,
			boolean isJoinTable, String subqueryAlias, JoinTreeNode jtn, QueryParser qParser, boolean isFromSubQuery, boolean isWhereSubQuery) throws Exception {

		net.sf.jsqlparser.schema.Table tab = (net.sf.jsqlparser.schema.Table)node; 
		String tableName = tab.getWholeTableName();
		Table table = qParser.getTableMap().getTable(tableName.toUpperCase());
		
		
		String aliasName = "";
		if (tab.getAlias() == null) {
			aliasName = tableName;
		} else {
			aliasName = tab.getAlias();
		}
		
	//	Jointree processing
		jtn.setNodeType(JoinTreeNode.relation);
		jtn.setLeft(null);
		jtn.setRight(null);
		jtn.setRelName(tableName);
		jtn.setOc(0);//setting output cardinality
		jtn.setNodeAlias(aliasName);
		
		//	FIXME: Mahesh some bug while adding baseRelation
		Util.addFromTable(table,qParser);

		if (aliasName != null) {
			qParser.getQuery().putBaseRelation(aliasName, tableName);
		} else {
			qParser.getQuery().putBaseRelation(tableName, tableName);
		}
		
		if (qParser.getQuery().getRepeatedRelationCount().get(tableName) != null) {
			qParser.getQuery().putRepeatedRelationCount(tableName, qParser.getQuery()
					.getRepeatedRelationCount().get(tableName) + 1);
			//	query.putTableNameToQueryIndex(tableName +  (query.getRepeatedRelationCount().get(tableName)), queryType, queryIndex);
		} else {
			qParser.getQuery().putRepeatedRelationCount(tableName, 1);
			//query.putTableNameToQueryIndex(tableName +  "1", queryType, queryIndex);
		}
		
		qParser.getQuery().putCurrentIndexCount(tableName, qParser.getQuery().getRepeatedRelationCount()
				.get(tableName) - 1);
		
		FromListElement temp = new FromListElement();
		if (tab.getAlias() != null) {
			temp.setAliasName(tab.getAlias() );
		} else {
			temp.setAliasName(tab.getWholeTableName());
		}
		temp.setTableName(tab.getWholeTableName());
		String tableNameNo = tableName
				+ qParser.getQuery().getRepeatedRelationCount().get(tableName);
		temp.setTableNameNo(tableNameNo);
		temp.setTabs(null);
		jtn.setTableNameNo(tableNameNo);
		
		Util.updateTableOccurrences(isFromSubQuery, isWhereSubQuery, tableNameNo, qParser);
		
		if (qParser.getQuery().getCurrentIndex().get(tableName) == null)
			qParser.getQuery().putCurrentIndex(tableName, 0);
		
		return temp;
	}

	
	
	public static FromListElement OperateOnBaseTable(FromBaseTable node,
			boolean isJoinTable, String subqueryAlias, JoinTreeNode jtn, QueryParser qParser, boolean isFromSubQuery, boolean isWhereSubQuery) throws Exception {

		String tableName = node.getBaseTableName();
		Table table = qParser.getTableMap().getTable(tableName);
		
		
		String aliasName = "";
		if (node.getCorrelationName() == null) {
			aliasName = tableName;
		} else {
			aliasName = node.getCorrelationName();
		}
		
	//	Jointree processing
		jtn.setNodeType(JoinTreeNode.relation);
		jtn.setLeft(null);
		jtn.setRight(null);
		jtn.setRelName(tableName);
		jtn.setOc(0);//setting output cardinality
		jtn.setNodeAlias(aliasName);
		
		//	FIXME: Mahesh some bug while adding baseRelation
		Util.addFromTable(table,qParser);

		if (aliasName != null) {
			qParser.getQuery().putBaseRelation(aliasName, tableName);
		} else {
			qParser.getQuery().putBaseRelation(tableName, tableName);
		}
		
		if (qParser.getQuery().getRepeatedRelationCount().get(tableName) != null) {
			qParser.getQuery().putRepeatedRelationCount(tableName, qParser.getQuery()
					.getRepeatedRelationCount().get(tableName) + 1);
			//	query.putTableNameToQueryIndex(tableName +  (query.getRepeatedRelationCount().get(tableName)), queryType, queryIndex);
		} else {
			qParser.getQuery().putRepeatedRelationCount(tableName, 1);
			//query.putTableNameToQueryIndex(tableName +  "1", queryType, queryIndex);
		}
		
		qParser.getQuery().putCurrentIndexCount(tableName, qParser.getQuery().getRepeatedRelationCount()
				.get(tableName) - 1);
		
		FromListElement temp = new FromListElement();
		if (node.getCorrelationName() != null) {
			temp.setAliasName(node.getCorrelationName());
		} else {
			temp.setAliasName(node.getBaseTableName());
		}
		temp.setTableName(node.getBaseTableName());
		String tableNameNo = tableName
				+ qParser.getQuery().getRepeatedRelationCount().get(tableName);
		temp.setTableNameNo(tableNameNo);
		temp.setTabs(null);
		jtn.setTableNameNo(tableNameNo);
		
		Util.updateTableOccurrences(isFromSubQuery, isWhereSubQuery, tableNameNo, qParser);
		
		if (qParser.getQuery().getCurrentIndex().get(tableName) == null)
			qParser.getQuery().putCurrentIndex(tableName, 0);
		
		return temp;
	}
}
