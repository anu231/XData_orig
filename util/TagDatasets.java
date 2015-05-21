package util;

/**
 * This class contains enums to denote the type of mutation we are trying to kill
 * These are used to tag data sets that we are generating
 * @author mahesh
 *
 */


public class TagDatasets {


	/** Indicates the type of mutation we are trying to kill*/
	public enum MutationType{

		ORIGINAL("DATASET FOR GENERATING NON EMPTY RESULT "),
		
		AGG("DATASET TO KILL AGGREGATION MUTATIONS "),
		
		COUNT("DATASET TO KILL COUNT MUTATIONS "),
		
		HAVING("DATASET TO KILL CONSTRAINED AGGREGATION MUTATIONS"),
		
		DISTINCT("DATASET TO KILL DISTINCT MUTATIONS "),
		
		EQUIVALENCE("DATASET TO KILL JOIN MUTATIONS "),
		
		EXTRAGROUPBY("DATASET TO KILL EXTRA GROUP BY ATTRIBUTES MUTATIONS "),
		
		PARTIALGROUPBY1("DATASET TO KILL PARTIAL GROUP BY ATTRIBUTES MUTATIONS "),
		
		PARTIALGROUPBY2("DATASET TO KILL PARTIAL GROUP BY ATTRIBUTES MUTATIONS "),
		
		NONEQUIJOIN("DATASET TO KILL NON EQUI JOIN MUTATIONS "),
		
		LIKE("DATASET TO KILL LIKE MUTATIONS "),
		
		SELCTION("DATASET TO KILL SELECTION MUTATIONS "),
		
		STRING("DATASET TO KILL STRING SELECTION MUTATIONS "),
		
		UNINTENDED("DATASET TO KILL UNINTENDED JOIN MUTATIONS DUE TO COMMON NAMES "),
		
		WHERECONNECTIVE("DATASET TO KILL WHERE CLAUSE SUBQUERY MUTATIONS "),
		
		NULL("DATASET TO KILL IS NULL MUTATIONS "),
		
		MISSING_SUBQUERY("DATASET TO KILL MISSING SUBQUERY MUTATIONS " ),
		
		PATTERN("DATASET TO KILL LIKE PATTERN MUTATIONS ");
		
		private String mutationType;
		
		/**constructor*/
		MutationType( String mutationType) {
			
			this.mutationType = mutationType;
		}
		
		/** get method*/
		public String getMutationType() {

			return mutationType;
		}
		
	}

	
	/**Indicates in which query block we are killing the mutation*/
	public enum QueryBlock{
		
		OUTER_BLOCK( "" ),
		
		FROM_SUBQUERY("IN FROM CLAUSE NESTED SUB QUERY BLOCK"),
		
		WHERE_SUBQUERY("IN WHERE CLAUSE NESTED SUB QUERY BLOCK"),
		
		NONE("");
		
		private String queryBlock;

		/**constructor*/
		QueryBlock( String queryBlock){

			this.queryBlock = queryBlock;
		}

		/** get method*/
		public String getQueryBlock() {

			return queryBlock;
		}
	}
}