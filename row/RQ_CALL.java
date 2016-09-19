public class RQ_CALL {

	// noopt for no optimizations
	// selectopt for where clause
	// groupbyopt for groupby clause
	// distinctopt for distinct clause
	// joinopt for join clause
	public static String queryType = "distinct";
	public static final String dbName = "row";
	public static String O1 = "y";
	public static String O2 = "N";
	public static String O3 = "N";
	public static String O4 = "N";
	public static String intermedProjections = "N";
	public static String createIndex1 = "N";
	public static String createIndex2 = "Y";
	//public static final int depth = 2;
	public static void main(String[] args) {
			
		String[] tableNames={"treecliquedup1m6"};		
		//String[] tableNames={"co100k6","treeclique100k6","cyclicclique1m6","treeclique1m6"};
		//String[] tableNames = { "tree10m", "cyclic10m", "list10m"};
		//String[] tableNames={"tree100k", "list100k","tree1m","list1m"};

		
		// file=input.txt;O1=N;O2=N;O3=Y;O4=N;database=row;intermediateProjections=y;createIndex1=n;createIndex2=n
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
		//O4 = "Y";
		/*createIndex2="Y";
		
		}*/

		/*O4 = "N";
		
		}*/

	}

}
