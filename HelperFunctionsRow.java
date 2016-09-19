import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.util.Properties;


public class HelperFunctionsRow extends HelperFunctions {
	
	private final String jdbcClassName = "org.postgresql.Driver";
	private final String connectionUrl = "url";
	private final String databaseName = "database";
	private final String username = "username";
	private final String password = "password";
	
	public HelperFunctionsRow() {
		super();
	}

	@Override
	public void getConnection(String fileName) {
		try {
			Class.forName(jdbcClassName);
		} catch (ClassNotFoundException e) {
			System.out.println("org.postgresql.Driver driver not found");
			System.exit(0);
		}
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream("java.ini"));
		} catch (Exception e) {
			System.out.println("Properties file not found");
			System.exit(0);
		}
		// Load Connection's properties from java.ini
		String strurl = properties.getProperty(connectionUrl);
		String strdatabase = properties.getProperty(databaseName);
		String struser = properties.getProperty(username);
		String strpassword = properties.getProperty(password);
		String connectionUrl = strurl + "/" + strdatabase;
		try {
			connection = DriverManager.getConnection(connectionUrl, struser,
					strpassword);
			try {
				sqloutput = new PrintWriter(new FileWriter(fileName+".sql"), true);
			} catch (IOException e) {
				System.out.println("Error creating file: "+fileName+".sql");
			}
		} catch (Exception e) {
			System.out.println("Database connection failure"+e);
			System.exit(0);
		}		
	}

	@Override
	public void createTempTable(Optimizations opt, CRV crvObj,
			SelectQuery selectQuery) {
		String createQuery = "CREATE TABLE G AS SELECT " + crvObj.i
				+ " AS i," + crvObj.j + " AS j, 1 AS p, " + crvObj.v
				+ " AS v FROM " + crvObj.baseTable;

		if(opt.createIndex2.equals("y")) {
			createQuery=createQuery+" ORDER BY i";		
		}
		executeDropQuery("G",TABLE);
		executeCreateQuery(createQuery);
		/*if(opt.createIndex1.equals("y")) {	
			String createIndex = "create index Ei on E(i)";
			executeDropQuery("Ei","index");
			updateIndexStartTime(opt, crvObj);
			executeCreateQuery(createIndex);
			updateIndexEndTime();
		}*/
	}

	@Override
	public void createTempJoinTable(Optimizations opt, CRV crvObj,
			SelectQuery selectQuery, String tableName, String selectionCondition) {
		String joinCondition = "JOIN " + selectQuery.joinRightTable[0] + " AS "
				+ selectQuery.joinRightTableAlias[0] + " ON T."
				+ selectQuery.joinLeftTableAttr[0] + "="
				+ selectQuery.joinRightTable[0] + "."
				+ selectQuery.joinRightTableAttr[0];
		
		if (selectQuery.joinRightTable[1] != null
				&& selectQuery.joinRightTableAttr[1] != null) {
			joinCondition = joinCondition + " " + "JOIN "
					+ selectQuery.joinRightTable[1] + " AS "
					+ selectQuery.joinRightTableAlias[1] + " ON T."
					+ selectQuery.joinLeftTableAttr[1] + "="
					+ selectQuery.joinRightTable[1] + "."
					+ selectQuery.joinRightTableAttr[1];
		}

		String createQuery = "CREATE TABLE " + tableName
				+ " AS SELECT T.i AS i,T.j AS j, T.p AS p, T.v AS v, "
				+ selectionCondition + " FROM T" + " " + joinCondition;
		
		executeDropQuery(tableName,TABLE);
		executeCreateQuery(createQuery);		
	}

	@Override
	public void semiNaiveRecursion(Optimizations opt, CRV crvObj,
			SelectQuery selectQuery) {
		//System.out.println("entering seminaive recursion");
		String initialTable = "G";
		String selectionCondition = "";
		if (opt.canOptimize && selectQuery.hasJoin) {
			for (String column : selectQuery.projectedColumns) {
				if (!column.startsWith(crvObj.recursiveTable)) {
					selectionCondition = selectionCondition + column + ",";
				}
			}
			if (selectionCondition.length() > 0) {
				selectionCondition = selectionCondition.substring(0,
						selectionCondition.length() - 1);
			}
			initialTable = "TEMP_JOIN";
			createTempJoinTable(opt, crvObj, selectQuery, "TEMP_JOIN",
					selectionCondition);
		}

		String createTableR1="";
		if (selectQuery.hasGroupBy) {
			createTableR1 = "CREATE TABLE RG1" + " AS SELECT 1 AS d, i AS i, j AS j, "
					+ selectQuery.pAggregation + "(p) AS p, "
					+ selectQuery.vAggregation + "(v) AS v"
					+ selectionCondition + " FROM " + initialTable;					
			if (selectQuery.optimizedWhereCondition.length() > 0) {
				createTableR1 = createTableR1 + " WHERE "
						+ selectQuery.optimizedWhereCondition;
			}
			createTableR1=createTableR1+" GROUP BY d,i,j";
			
		}else {
			createTableR1 = "CREATE TABLE RG1 AS SELECT 1 AS d, i AS i, j AS j, p AS p, v AS v"
					+ selectionCondition + " FROM " + initialTable;			
		
			if (selectQuery.optimizedWhereCondition.length() > 0) {
				createTableR1 = createTableR1 + " WHERE "
						+ selectQuery.optimizedWhereCondition;
			}
		}
		if(opt.createIndex2.equals("y")) {
			createTableR1=createTableR1+" ORDER BY j";
		}
		executeDropQuery("RG1",TABLE);
		executeCreateQuery(createTableR1);
		
		/*if(opt.createIndex2.equals("y")) {	
			String createIndex = "create index R1j on r1(j)";
			executeDropQuery("R1j","index");
			executeCreateQuery(createIndex);
		}*/
		
		//apply_Optimizations_On_Temporary_Table("RD", "RR1", opt, crvObj,
		//		selectQuery, selectionCondition);
		//apply_Optimizations_On_Temporary_Table("distinctTable", "RR1", opt,
		//		crvObj, selectQuery, selectionCondition);

		int d = 2;
		int k = (selectQuery.recursionDepth != -1 && selectQuery.recursionDepth < crvObj.recursionDepth) ? selectQuery.recursionDepth
				: crvObj.recursionDepth;
		while (d <= k) {			
			String newTableName = "RG" + String.valueOf(d);
			String oldTableName = "RG" + String.valueOf(d - 1);
			String recursiveTableQuery = null;

			if (selectQuery.hasGroupBy) {
				recursiveTableQuery = "CREATE TABLE " + newTableName
						+ " AS SELECT " + (d) + " AS d,"+ oldTableName+ ".i AS i, G.j AS j, "
						+ selectQuery.pAggregation + "("+oldTableName+".p*G.p) AS p, "
						+ selectQuery.vAggregation + "("+oldTableName+".v*G.v) AS v"
						+ selectionCondition + " FROM " + oldTableName + " JOIN G ON "+oldTableName+".j=G.i"
						+ " WHERE "+oldTableName+".i!="+oldTableName+".j";
				recursiveTableQuery = recursiveTableQuery+ " GROUP BY "+oldTableName+".d,"+oldTableName+".i,G.j";
			}else {
				recursiveTableQuery = "CREATE TABLE " + newTableName
						+ " AS SELECT " + (d) + " AS d, " + oldTableName
						+ ".i AS i, G.j AS j, " + oldTableName
						+ ".p*G.p AS p," + "" + oldTableName + ".v+G.v AS v "
						+ selectionCondition + " FROM " + oldTableName
						+ " JOIN G ON " + oldTableName + ".j=G.i WHERE "
						+ oldTableName + ".i!=" + oldTableName + ".j";
			}
			if(opt.createIndex2.equals("y")) {			
				recursiveTableQuery=recursiveTableQuery+" ORDER BY j";
			}
			executeDropQuery(newTableName,TABLE);
			executeCreateQuery(recursiveTableQuery);

			/*if(opt.createIndex2.equals("y")) {	
				String indexName = newTableName+"j";
				String createIndex = "create index "+indexName+" on "+newTableName+"(j)";
				executeDropQuery(indexName,"index");
				executeCreateQuery(createIndex);
			}*/
			
			//apply_Optimizations_On_Temporary_Table("distinctTable",
			//		newTableName, opt, crvObj, selectQuery, selectionCondition);

			String countQuery = "SELECT COUNT(*) FROM "+newTableName;
			if (executeQuery(countQuery) == 0) {
				break;
			}
			d++;
		}

		String createQuery = "CREATE TABLE "+crvObj.recursiveTable+" AS SELECT * FROM RG1";
		for(int i=2;i<d;i++) {
			createQuery=createQuery+" UNION ALL SELECT * FROM RG"+String.valueOf(i);
		}

		executeDropQuery(crvObj.recursiveTable,TABLE);
		executeCreateQuery(createQuery);		

		String alterQuery = "ALTER TABLE " + crvObj.recursiveTable;

		if (!crvObj.recursiveTableAttr[1].equalsIgnoreCase("i")) {
			String alterQueryI = alterQuery + " RENAME i TO " + crvObj.recursiveTableAttr[1];
			executeCreateQuery(alterQueryI);
		}
		if (!crvObj.recursiveTableAttr[2].equalsIgnoreCase("j")) {
			String alterQueryJ = alterQuery + " RENAME j TO " + crvObj.recursiveTableAttr[2];
			executeCreateQuery(alterQueryJ);
		}
		if (!crvObj.recursiveTableAttr[4].equalsIgnoreCase("v")) {
			String alterQueryV = alterQuery + " RENAME v TO " + crvObj.recursiveTableAttr[4];
			executeCreateQuery(alterQueryV);
		}
		if (!crvObj.recursiveTableAttr[3].equalsIgnoreCase("p")) {
			String alterQueryP = alterQuery + " RENAME p TO " + crvObj.recursiveTableAttr[3];
			executeCreateQuery(alterQueryP);
		}
		if (!crvObj.recursiveTableAttr[0].equalsIgnoreCase("d")) {
			String alterQueryD = alterQuery + " RENAME d TO " + crvObj.recursiveTableAttr[0];
			executeCreateQuery(alterQueryD);
		}

		 
		String rcountquery="SELECT COUNT(*) FROM "+crvObj.recursiveTable;
		System.out.println("Number of rows in R table: "+executeQuery(rcountquery));
		
		deleteIntermediateTables(d);
		
	}

	@Override
	public void apply_Optimizations_On_Temporary_Table(String targetTable,
			String sourceTable, Optimizations opt, CRV crvObj,
			SelectQuery selectQuery, String selectionCondition) {
		executeDropQuery(targetTable,TABLE);

		String previousTableDistinctQuery = "";
		if (selectQuery.hasGroupBy) {
			previousTableDistinctQuery = "CREATE TABLE " + targetTable
					+ " AS SELECT d AS d, i AS i, j AS j, "
					+ selectQuery.pAggregation + "(p) AS p, "
					+ selectQuery.vAggregation + "(v) AS v"
					+ selectionCondition + " FROM " + sourceTable
					+ " WHERE i!=j";
			previousTableDistinctQuery = previousTableDistinctQuery
					+ " GROUP BY i,j,d";
		} else {
			previousTableDistinctQuery = "CREATE TABLE " + targetTable
					+ " AS SELECT d AS d, i AS i, j AS j, p AS p, v AS v"
					+ selectionCondition + " FROM " + sourceTable
					+ " WHERE i!=j";

		}
		
		if(!selectQuery.optimizedWhereCondition.equals("")) {
			previousTableDistinctQuery = previousTableDistinctQuery+" AND "+selectQuery.optimizedWhereCondition;
		}

		executeCreateQuery(previousTableDistinctQuery);

		if(opt.createIndex2.equals("y") && !targetTable.equals("RD")) {	
		String createIndex = "create index distinctTablej on distinctTable(j)";
		executeDropQuery("distinctTablej","index");
		long startIndexTime = System.currentTimeMillis();
		executeCreateQuery(createIndex);
		long endIndexTime = System.currentTimeMillis();
		System.out.println("Time taken to create index on "+sourceTable+":"+(endIndexTime-startIndexTime));
		}
	}
	
	@Override
	public void updateStartTime(String dataset, Optimizations opt,int depth) {
	String query="insert into colrq_timetrack values('"+dataset+"','"+opt.O1flag+"','"+opt.O2flag+"','"+opt.O3flag       +"','"+opt.O4flag+"',"+depth+",'"+opt.createIndex2+"',"+"TIMESTAMP 'now')";
	executeCreateQuery(query);
	}

	public void updateIndexStartTime(Optimizations opt,CRV crvobj) {
	String query="insert into colrq_indextime values('"+crvobj.baseTable+"','"+opt.O1flag+"','"+opt.O2flag+"','"+opt.O3flag+"','"+opt.O4flag+"',"+crvobj.recursionDepth+",'"+opt.createIndex2+"',"+"TIMESTAMP 'now')";
	executeCreateQuery(query);
	}

	public void updateIndexEndTime() {
	String query="update colrq_indextime set endtime=timestamp 'now' where endtime is null";
	executeCreateQuery(query);
	}
}
