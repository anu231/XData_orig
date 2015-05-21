package killMutations.fromClauseNestedBlock;

import generateConstraints.GenerateCVCConstraintForNode;
import generateConstraints.GenerateCommonConstraintsForQuery;
import generateConstraints.GenerateConstraintsForConjunct;
import generateConstraints.GenerateConstraintsForHavingClause;
import generateConstraints.GenerateGroupByConstraints;
import generateConstraints.GenerateJoinPredicateConstraints;
import generateConstraints.RelatedToEquivalenceClassMutations;

import java.util.*;

import parsing.Column;
import parsing.Conjunct;
import parsing.Node;
import parsing.Table;
import testDataGen.GenerateCVC1;
import testDataGen.QueryBlockDetails;
import testDataGen.RelatedToParameters;
import util.TagDatasets;

/**
 * This class generates data sets to kill equivalence class mutations in the from clause nested subquery block
 * @author mahesh
 *
 */
public class EquivalenceMutationInFromSubQuery {

	/**
	 * Generates data to kill equivalence class mutations inside from clause nested subquery block
	 * @param cvc
	 * @throws Exception
	 */
	public static void generateDataForkillingEquivalenceClassMutationsInFromSubquery(GenerateCVC1 cvc) throws Exception{

		
		/** keep a copy of this tuple assignment values */
		HashMap<String, Integer> noOfOutputTuplesOrig = (HashMap<String, Integer>) cvc.getNoOfOutputTuples().clone();
		HashMap<String, Integer> noOfTuplesOrig = (HashMap<String, Integer>) cvc.getNoOfTuples().clone();
		HashMap<String, Integer[]> repeatedRelNextTuplePosOrig = (HashMap<String, Integer[]>)cvc.getRepeatedRelNextTuplePos().clone();
		
		
		/** Kill equivalence class mutations in each from clause nested block of this query*/
		for(QueryBlockDetails qbt: cvc.getOuterBlock().getFromClauseSubQueries()){

			System.out.println("\n----------------------------------");
			System.out.println("GENERATE DATA FOR Killing equivalence clause Mutations Inside From clause subquery block: "+ qbt);
			System.out.println("----------------------------------\n");

			/**Kill the equivalence clause mutations in each conjunct of this sub query query block*/
			for(Conjunct conjunct: qbt.getConjuncts()){

				System.out.println("\n----------------------------------");
				System.out.println("NEW CONJUNCT IN EC KILLING: " + conjunct);
				System.out.println("\n----------------------------------");

				/** Keep a copy of the original equivalence classes*/
				Vector<Vector<Node>> equivalenceClassesOrig = (Vector<Vector<Node>>)conjunct.getEquivalenceClasses().clone();

				System.out.println("\n----------------------------------");
				System.out.println("KILLING EC: " + equivalenceClassesOrig);
				System.out.println("\n----------------------------------");

				/** Kill each equivalence clause mutations of this conjunct*/
				for(int i=0; i<equivalenceClassesOrig.size();i++){

					/**Get the equivalence class that is to be killed*/					
					Vector<Node> ec = (Vector<Node>)equivalenceClassesOrig.get(i).clone();

					/**Update the equivalence classes...these are used during tuple assignment*/
					conjunct.setEquivalenceClasses((Vector<Vector<Node>>)equivalenceClassesOrig.clone());
					conjunct.getEquivalenceClasses().remove(ec);

					/** In this iteration we are killing equivalence  class 'ec'*/
					qbt.setEquivalenceClassesKilled( new ArrayList<Node>(ec) ); 					
					
					/** Initialize the data structures for generating the data to kill this mutation */
					cvc.inititalizeForDataset();

					/**set the type of mutation we are trying to kill*/
					cvc.setTypeOfMutation( TagDatasets.MutationType.EQUIVALENCE, TagDatasets.QueryBlock.FROM_SUBQUERY );
					
					/** get the tuple assignment for this query
					 * If no possible assignment then not possible to kill this mutation*/
					if(GenerateCVC1.tupleAssignmentForQuery(cvc) == false)
						continue;

					/** keep a copy of this tuple assignment values */
					HashMap<String, Integer> noOfOutputTuplesOrig1 = (HashMap<String, Integer>) cvc.getNoOfOutputTuples().clone();
					HashMap<String, Integer> noOfTuplesOrig1 = (HashMap<String, Integer>) cvc.getNoOfTuples().clone();
					HashMap<String, Integer[]> repeatedRelNextTuplePosOrig1 = (HashMap<String, Integer[]>)cvc.getRepeatedRelNextTuplePos().clone();


					for(int j=0;j<ec.size(); j++){
						System.out.println("----------------------------------\n");
						
						/** In this iteration we are killing equivalence  class 'ec'*/
						qbt.setEquivalenceClassesKilled( new ArrayList<Node>(ec) ); 
						
						cvc.setConstraints( new ArrayList<String>());
						cvc.setStringConstraints( new ArrayList<String>());
						cvc.setCVCStr("");
						
						/** Add constraints related to parameters*/
						cvc.getConstraints().add(RelatedToParameters.addDatatypeForParameters(cvc, qbt));
						
						cvc.setResultsetTableColumns1( new HashMap<Table,Vector<String>>() );
						
						
						cvc.setNoOfTuples( (HashMap<String, Integer>) noOfTuplesOrig1.clone() );
						cvc.setNoOfOutputTuples( (HashMap<String, Integer>) noOfOutputTuplesOrig1.clone() );
						cvc.setRepeatedRelNextTuplePos( (HashMap<String, Integer[]>)repeatedRelNextTuplePosOrig1.clone() );
						
						String CVCStr = "";
						
						
						Node eceNulled = ec.get(j);
						
						
						CVCStr += "%DataSet Generated By Nulling: "+ ((Node)eceNulled).toString() + "\n";
						
						
						
						
						/** Add constraints for all the From clause nested subquery blocks except this sub query block */
						for(QueryBlockDetails qb: cvc.getOuterBlock().getFromClauseSubQueries()){
							if(!(qb.equals(qbt))){
								cvc.getConstraints().add("\n%---------------------------------\n% FROM CLAUSE SUBQUERY\n%---------------------------------\n");
								
								cvc.getConstraints().add( QueryBlockDetails.getConstraintsForQueryBlock(cvc, qb) );
								
								cvc.getConstraints().add("\n%---------------------------------\n% END OF FROM CLAUSE SUBQUERY\n%---------------------------------\n");
							}
						}

						
						
						if(  RelatedToEquivalenceClassMutations.getConstraintsForNulledColumns(cvc, qbt, ec, eceNulled) == false)
							continue;
								
												
						/** Generate positive constraints for all the conditions of this sub query block conjunct */
						cvc.getConstraints().add( GenerateConstraintsForConjunct.getConstraintsForConjuct(cvc, qbt, conjunct) );
						
						
						/** Add negative conditions for all other conjuncts of this sub query block*/
						for(Conjunct inner: qbt.getConjuncts()){
							if(inner != conjunct){
								cvc.getConstraints().add( GenerateConstraintsForConjunct.generateNegativeConstraintsConjunct(cvc, qbt, inner) );	
							}
						}
						
						
						/** get group by constraints */
						cvc.getConstraints().add("\n%---------------------------------\n%GROUP BY CLAUSE CONSTRAINTS FOR SUBQUERY BLOCK\n%---------------------------------\n");
						cvc.getConstraints().add( GenerateGroupByConstraints.getGroupByConstraints( cvc, qbt) );
						
						
						/** Generate havingClause constraints */
						cvc.getConstraints().add("\n%---------------------------------\n%HAVING CLAUSE CONSTRAINTS FOR SUBQUERY BLOCK\n%---------------------------------\n");
						for(int l=0; l< qbt.getNoOfGroups(); l++)
							for(int k=0; k < qbt.getAggConstraints().size();k++){
								cvc.getConstraints().add(GenerateConstraintsForHavingClause.getHavingClauseConstraints(cvc, qbt, qbt.getAggConstraints().get(k), qbt.getFinalCount(), l) );
							}
						cvc.getConstraints().add("\n%---------------------------------\n%END OF HAVING CLAUSE CONSTRAINTS FOR SUBQUERY BLOCK\n%---------------------------------\n");
						
						
												
						/** add constraints of outer query block */
						cvc.getConstraints().add( QueryBlockDetails.getConstraintsForQueryBlockExceptSubQuries(cvc, cvc.getOuterBlock()) );
						
						
						cvc.setCVCStr(CVCStr);
						
						/** Call the method for the data generation*/
						GenerateCommonConstraintsForQuery.generateDataSetForConstraints(cvc);
					}

				}
				
				/** revert back the equivalence classes of this conjunct */
				conjunct.setEquivalenceClasses((Vector<Vector<Node>>)equivalenceClassesOrig.clone());
			}
		}
		
		/** Revert back to the old assignment */
		cvc.setNoOfTuples( (HashMap<String, Integer>) noOfTuplesOrig.clone() );
//		cvc.setNoOfOutputTuples( (HashMap<String, Integer>) noOfOutputTuplesOrig.clone() );
		cvc.setRepeatedRelNextTuplePos( (HashMap<String, Integer[]>)repeatedRelNextTuplePosOrig.clone() );
	}
}
