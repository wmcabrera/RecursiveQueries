public class RQ_CALL {

	// noopt for no optimizations
	// selectopt for where clause
	// groupbyopt for groupby clause
	// distinctopt for distinct clause
	// joinopt for join clause
	public static String queryType = "groupopt";
	public static final String dbName = "column";
	public static String O1 = "N";
	public static String O2 = "N";
	public static String O3 = "N";
	public static String O4 = "y";
	public static String intermedProjections = "N";
	public static String createIndex1 = "N";
	public static String createIndex2 = "N";
	//public static final int depth = 2;
	public static void main(String[] args) {
	//	String[] tableNames={"tree1m4"};
		String[] tableNames={ "wiki_vote4"};
                O3= args[0];
		for (String tableName : tableNames) {
			String inputFile = tableName + "_" + queryType+".txt";
			
/*			String inputArgument = "file=" + inputFile + ",O1=" + O1 + ",O2="
					+ O2 + ",O3=" + O3 + ",O4=" + O4 + "," + "database="
					+ dbName + ","
					+ "intermediateProjections="+intermedProjections+",createIndex1="+createIndex1+",createIndex2="+createIndex2;
*/		
			String inputArgument = "file=" + inputFile + ",O1=" + O1 + ",O2="
					+ O2 + ",O3=" + O3 + ",O4=" + O4 + "," + "database="
					+ dbName + ","
					+ "intermediateProjections="+intermedProjections+",createIndex1="+createIndex1+",createIndex2="+createIndex2;
		
			try {
				rq.parser(inputArgument,Integer.parseInt(tableName.substring(tableName.length()-1)));
			} catch (Exception e) {
				System.out.println("rq parser throws an exception"+e);
			}
			System.out.println(inputArgument);
		
		}
		/*intermedProjections = "Y";
		for (String tableName : tableNames) {
			String inputFile = tableName + "_" + queryType+".txt";
			
			String inputArgument = "file=" + inputFile + ",O1=" + O1 + ",O2="
					+ O2 + ",O3=" + O3 + ",O4=" + O4 + "," + "database="
					+ dbName + ","
					+ "intermediateProjections="+intermedProjections+",createIndex1="+createIndex1+",createIndex2="+createIndex2;
		
			try {
				rq.parser(inputArgument,Integer.parseInt(tableName.substring(tableName.length()-1)));		
			} catch (Exception e) {
				System.out.println("rq parser throws an exception"+e);
			}
			System.out.println(inputArgument);
		
		}*/
		/*queryType = "groupopt";
		O4="N";
		intermedProjections = "N";
		for (String tableName : tableNames) {
			String inputFile = tableName + "_" + queryType+".txt";
			
			String inputArgument = "file=" + inputFile + ",O1=" + O1 + ",O2="
					+ O2 + ",O3=" + O3 + ",O4=" + O4 + "," + "database="
					+ dbName + ","
					+ "intermediateProjections="+intermedProjections+",createIndex1="+createIndex1+",createIndex2="+createIndex2;
		
			try {
				rq.parser(inputArgument,Integer.parseInt(tableName.substring(tableName.length()-1)));
			} catch (Exception e) {
				System.out.println("rq parser throws an exception"+e);
			}
			System.out.println(inputArgument);
		
		}
		//intermedProjections = "Y";
		O4="Y";		
		for (String tableName : tableNames) {
			String inputFile = tableName + "_" + queryType+".txt";
			
			String inputArgument = "file=" + inputFile + ",O1=" + O1 + ",O2="
					+ O2 + ",O3=" + O3 + ",O4=" + O4 + "," + "database="
					+ dbName + ","
					+ "intermediateProjections="+intermedProjections+",createIndex1="+createIndex1+",createIndex2="+createIndex2;
		
			try {
				rq.parser(inputArgument,Integer.parseInt(tableName.substring(tableName.length()-1)));
			} catch (Exception e) {
				System.out.println("rq parser throws an exception"+e);
			}
			System.out.println(inputArgument);
		
		}*/
		/*queryType = "selectopt";
		O3 = "Y";
		createIndex1="N";
		for (String tableName : tableNames) {
			String inputFile = tableName + "_" + queryType+".txt";
			
			String inputArgument = "file=" + inputFile + ",O1=" + O1 + ",O2="
					+ O2 + ",O3=" + O3 + ",O4=" + O4 + "," + "database="
					+ dbName + ","
					+ "intermediateProjections="+intermedProjections+",createIndex1="+createIndex1+",createIndex2="+createIndex2;
		
			try {
				System.out.println("test1");
				rq.parser(inputArgument,depth);
			} catch (Exception e) {
				System.out.println("rq parser throws an exception"+e);
			}
			System.out.println(inputArgument);
		
		}

		createIndex1="Y";
		for (String tableName : tableNames) {
			String inputFile = tableName + "_" + queryType+".txt";
			
			String inputArgument = "file=" + inputFile + ",O1=" + O1 + ",O2="
					+ O2 + ",O3=" + O3 + ",O4=" + O4 + "," + "database="
					+ dbName + ","
					+ "intermediateProjections="+intermedProjections+",createIndex1="+createIndex1+",createIndex2="+createIndex2;
		
			try {
				System.out.println("test1");
				rq.parser(inputArgument,depth);
			} catch (Exception e) {
				System.out.println("rq parser throws an exception"+e);
			}
			System.out.println(inputArgument);
		
		}

		queryType = "groupopt";
		O3 = "N";
		O4 = "Y";
		createIndex1="N";
		for (String tableName : tableNames) {
			String inputFile = tableName + "_" + queryType+".txt";
			
			String inputArgument = "file=" + inputFile + ",O1=" + O1 + ",O2="
					+ O2 + ",O3=" + O3 + ",O4=" + O4 + "," + "database="
					+ dbName + ","
					+ "intermediateProjections="+intermedProjections+",createIndex1="+createIndex1+",createIndex2="+createIndex2;
		
			try {
				System.out.println("test1");
				rq.parser(inputArgument,depth);
			} catch (Exception e) {
				System.out.println("rq parser throws an exception"+e);
			}
			System.out.println(inputArgument);
		
		}

		createIndex1="Y";
		for (String tableName : tableNames) {
			String inputFile = tableName + "_" + queryType+".txt";
			
			String inputArgument = "file=" + inputFile + ",O1=" + O1 + ",O2="
					+ O2 + ",O3=" + O3 + ",O4=" + O4 + "," + "database="
					+ dbName + ","
					+ "intermediateProjections="+intermedProjections+",createIndex1="+createIndex1+",createIndex2="+createIndex2;
		
			try {
				System.out.println("test1");
				rq.parser(inputArgument,depth);
			} catch (Exception e) {
				System.out.println("rq parser throws an exception"+e);
			}
			System.out.println(inputArgument);
		
		}*/

	}

}
