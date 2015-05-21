package killMutations.whereClauseNestedBlock;

import generateConstraints.GenerateCVCConstraintForNode;
import generateConstraints.GenerateCommonConstraintsForQuery;
import generateConstraints.GenerateConstraintsForConjunct;
import generateConstraints.GenerateConstraintsForHavingClause;
import generateConstraints.GenerateConstraintsForWhereClauseSubQueryBlock;
import generateConstraints.GenerateGroupByConstraints;
import generateConstraints.GenerateJoinPredicateConstraints;
import generateConstraints.RelatedToEquivalenceClassMutations;
import generateConstraints.UtilsRelatedToNode;

import java.util.*;

import parsing.Column;
import parsing.Conjunct;
import parsing.Node;
import parsing.RelationHierarchyNode;
import parsing.Table;
import testDataGen.GenerateCVC1;
import testDataGen.GenerateCVC1;
import testDataGen.QueryBlockDetails;
import testDataGen.RelatedToParameters;
import util.TagDatasets;

public class MutationsInNotExistsSubquery {

	/**
	 * Generates data to kill equivalence class mutations inside Where clause nested subquery block
	 * @param cvc
	 * @throws Exception
	 */
	
	GenerateCVC1 cvc;
	
	RelationHierarchyNode topLevelRelation;
	
	public MutationsInNotExistsSubquery(GenerateCVC1 cvc, RelationHierarchyNode relation){
		this.cvc = cvc;
		this.topLevelRelation = relation;
	}
	
	public static void genDataToKillMutantsInNotExistsSubquery(GenerateCVC1 cvc) {
		try {
		/** keep a copy of this tuple assignment values */
		HashMap<String, Integer> noOfOutputTuplesOrig = (HashMap<String, Integer>) cvc.getNoOfOutputTuples().clone();
		HashMap<String, Integer> noOfTuplesOrig = (HashMap<String, Integer>) cvc.getNoOfTuples().clone();
		HashMap<String, Integer[]> repeatedRelNextTuplePosOrig = (HashMap<String, Integer[]>)cvc.getRepeatedRelNextTuplePos().clone();

		int notExistsCount = 0;
		
		QueryBlockDetails topQbt = cvc.getOuterBlock();
		
		cvc.inititalizeForDataset();
		
		/**set the type of mutation we are trying to kill*/
		cvc.setTypeOfMutation( TagDatasets.MutationType.ORIGINAL, TagDatasets.QueryBlock.NONE );
		
		if(GenerateCVC1.tupleAssignmentForQuery(cvc) == false)
			return ;
		
		cvc.setNoOfTuples( (HashMap<String, Integer>) noOfTuplesOrig.clone() );
		cvc.setNoOfOutputTuples( (HashMap<String, Integer>) noOfOutputTuplesOrig.clone() );
		cvc.setRepeatedRelNextTuplePos( (HashMap<String, Integer[]>)repeatedRelNextTuplePosOrig.clone() );
		
		/** we have to check if there are where clause sub queries in each conjunct of outer block of query */
		for(Conjunct con: cvc.getOuterBlock().getConjuncts()){

			/**For each where clause sub query blocks of this conjunct*/
			for(Node subQCond: con.getAllSubQueryConds()){
				
				System.out.println(subQCond.getType());
				
				if (subQCond.getType().equalsIgnoreCase(Node.getNotExistsNodeType())) {
					int index = UtilsRelatedToNode.getQueryIndexOfSubQNode(subQCond);
					QueryBlockDetails qbt = topQbt.getWhereClauseSubQueries().get(index);
					MutationsInNotExistsSubquery mutationKiller = new MutationsInNotExistsSubquery(cvc, topQbt.getTopLevelRelation());
					cvc.getConstraints().add(mutationKiller.genConstraintsForNotExists(qbt, topQbt.getTopLevelRelation().getNotExistsNode(notExistsCount)));
					notExistsCount++;
				}			
			}
		}
		
		GenerateCommonConstraintsForQuery.generateDataSetForConstraints(cvc);
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	public String genConstraintsForNotExists(QueryBlockDetails qbt, RelationHierarchyNode node) throws Exception{
		String constraint = "";
		
		Vector<String> temp = new Vector<String>();
		
		if(node.getNodeType().equals("_RELATION_")){
			ArrayList<Conjunct> conjuncts = qbt.getConjuncts();
			for(Conjunct c:conjuncts){				
				 temp.add(GenerateConstraintsForConjunct.generateConstraintsNotExists(cvc, qbt, c, node.getTableName()));
			}
			
			return GenerateConstraintsForConjunct.processOrConstraintsNotExists(temp);
		}
		else if(node.getNodeType().equals("_LEFT_JOIN_")){			
			return genConstraintsForNotExists(qbt, node.getLeft());
		}
		else if(node.getNodeType().equals("_RIGHT_JOIN_")){			
			return genConstraintsForNotExists(qbt, node.getRight());
		}
		else if(node.getNodeType().equals("_JOIN_")){
			Vector<String> OrConstraints=new Vector<String>();
			for(Conjunct c: qbt.getConjuncts()){
				OrConstraints.add(GenerateConstraintsForConjunct.generateJoinConditionConstraintsForNotExists(cvc, qbt, c));
			}
			
			OrConstraints.add(genConstraintsForNotExists(qbt, node.getLeft()));
			OrConstraints.add(genConstraintsForNotExists(qbt, node.getRight()));
			
			return GenerateConstraintsForConjunct.processOrConstraintsNotExists(OrConstraints);
		}
		
		return constraint;
	}
}
