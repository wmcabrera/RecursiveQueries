import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.Timer;

public class HelperFunctionsColumn extends HelperFunctions {

	private final String jdbcClassName = "com.vertica.jdbc.Driver";
	private final String connectionUrl = "url";
	private final String databaseName = "database";
	private final String username = "username";
	private final String password = "password";
	private final String PROJECTION_REFRESH = "SELECT START_REFRESH()";
	public static final String TABLE = "table";
	public static final String PROJECTION = "projection";

	public HelperFunctionsColumn() {
		super();
	}

	@Override
	public void getConnection(String fileName) {
		try {
			Class.forName(jdbcClassName);
		} catch (ClassNotFoundException e) {
			System.out.println("com.vertica.jdbc.Driver driver not found");
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
			System.out.println("Database connection failure");
			System.exit(0);
		}
	}

	@Override
	public void createTempTable(Optimizations opt, CRV crvObj,
			SelectQuery selectQuery) {

		String createQuery = null;

		if (opt.intermediateProjections.equals("y")) {
			String columnList = "(i,j,p,v)";
			createQuery = "CREATE TABLE E" + columnList + " AS SELECT "
					+ crvObj.i + " AS i," + crvObj.j + " AS j, 1 AS p, "
					+ crvObj.v + " AS v FROM " + crvObj.baseTable
					+ " ORDER BY i,j";
		} else {
			createQuery = "CREATE TABLE E AS SELECT " + crvObj.i + " AS i,"
					+ crvObj.j + " AS j, 1 AS p, " + crvObj.v + " AS v FROM "
					+ crvObj.baseTable;	
		}
		
		
		executeDropQuery("E", TABLE);
		executeCreateQuery(createQuery);		
	}

	@Override
	public void createTempJoinTable(Optimizations opt, CRV crvObj,
			SelectQuery selectQuery, String tableName, String selectionCondition) {
		String joinCondition = "JOIN " + selectQuery.joinRightTable[0] + " AS "
				+ selectQuery.joinRightTableAlias[0] + " ON T."
				+ selectQuery.joinLeftTableAttr[0] + "="
				+ selectQuery.joinRightTable[0] + "."
				+ selectQuery.joinRightTableAttr[0];
	        String distinct= "";	

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
				+ " AS SELECT " + distinct + " T.i AS i,T.j AS j, T.p AS p, T.v AS v, "
				+ selectionCondition + " FROM T" + " " + joinCondition;

		executeDropQuery(tableName, TABLE);
		executeCreateQuery(createQuery);
	}

	@Override
	public void semiNaiveRecursion(Optimizations opt, CRV crvObj,
			SelectQuery selectQuery) {
		String initialTable = "E";
		String selectionCondition = "";
		String distinct = "";
                if (opt.O1flag.equals("y") ) {
                   distinct = "distinct ";
                }
                System.out.println(opt.O1flag+"d:"+ distinct);
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

		String orderByClause = "";
		if (opt.intermediateProjections.equals("y")) {
			orderByClause = " order by j,i";
		}


		String createTableR1="";
		if (selectQuery.hasGroupBy) {
			createTableR1 = "CREATE TABLE R1" + " AS SELECT 1 AS d, i AS i, j AS j, "
					+ selectQuery.pAggregation + "(p) AS p, "
					+ selectQuery.vAggregation + "(v) AS v"
					+ selectionCondition + " FROM " + initialTable;					
			if (selectQuery.optimizedWhereCondition.length() > 0) {
				createTableR1 = createTableR1 + " WHERE "
						+ selectQuery.optimizedWhereCondition;
			}
			createTableR1=createTableR1+" GROUP BY d,i,j";
			
		}else {
			createTableR1 = "CREATE TABLE R1 AS SELECT " + distinct + " 1 AS d, i AS i, j AS j, p AS p, v AS v"
					+ selectionCondition + " FROM " + initialTable;			
		
			if (selectQuery.hasWhere & opt.O3flag.equals("y") ) {
				createTableR1 = createTableR1 + " WHERE "
						+ selectQuery.whereCondition;
			}
		}
		
		createTableR1 = createTableR1+orderByClause;

		/*String createTableR1 = "CREATE TABLE R1 AS SELECT 1 AS d, i AS i, j AS j, p AS p, v AS v"
				+ selectionCondition + " FROM " + initialTable;
		if (selectQuery.optimizedWhereCondition.length() > 0) {
			createTableR1 = createTableR1 + " WHERE "
					+ selectQuery.optimizedWhereCondition;
		}
		createTableR1 = createTableR1+orderByClause;*/

		executeDropQuery("R1", TABLE);		
		executeCreateQuery(createTableR1);	
		// apply_Optimizations_On_Temporary_Table("RD", "RR1", opt, crvObj,
		// selectQuery, selectionCondition);
		// apply_Optimizations_On_Temporary_Table("distinctTable", "RR1", opt,
		// crvObj, selectQuery, selectionCondition);

		int d = 2;
		int k = (selectQuery.recursionDepth != -1 && selectQuery.recursionDepth < crvObj.recursionDepth) ? selectQuery.recursionDepth
				: crvObj.recursionDepth;
		if (opt.O1flag.equals("y")) {
		    distinct="DISTINCT ";
		}		
		while (d <= k) {
			String newTableName = "R" + String.valueOf(d);
			String oldTableName = "R" + String.valueOf(d - 1);
			String recursiveTableQuery = null;

			if (selectQuery.hasGroupBy) {
				recursiveTableQuery = "CREATE TABLE " + newTableName
						+ " AS SELECT " + distinct + " "+ d + " AS d, "+ oldTableName+ ".i AS i, E.j AS j, "
						+ selectQuery.pAggregation + "("+oldTableName+".p*E.p) AS p, "
						+ selectQuery.vAggregation + "("+oldTableName+".v*E.v) AS v"
						+ selectionCondition + " FROM " + oldTableName + " JOIN E ON "+oldTableName+".j=E.i"
						+ " WHERE "+oldTableName+".i!="+oldTableName+".j";
				recursiveTableQuery = recursiveTableQuery+ " GROUP BY "+oldTableName+".d,"+oldTableName+".i,E.j";
                        }else if (selectQuery.hasWhere & opt.O3flag.equals("y") ) { 
 	                        recursiveTableQuery = "CREATE TABLE " + newTableName
						+ " AS SELECT "+distinct + (d) + " AS d, " + oldTableName
						+ ".i AS i, E.j AS j, " + oldTableName
						+ ".p*E.p AS p," + "" + oldTableName + ".v+E.v AS v "
						+ selectionCondition + " FROM " + oldTableName
						+ " JOIN E ON " + oldTableName + ".j=E.i WHERE "
						+ oldTableName + ".i!=" + oldTableName + ".j "
                                                + "AND " +oldTableName + "."+selectQuery.whereCondition;
                        
			}else {
				recursiveTableQuery = "CREATE TABLE " + newTableName
						+ " AS SELECT "+distinct + (d) + " AS d, " + oldTableName
						+ ".i AS i, E.j AS j, " + oldTableName
						+ ".p*E.p AS p," + "" + oldTableName + ".v+E.v AS v "
						+ selectionCondition + " FROM " + oldTableName
						+ " JOIN E ON " + oldTableName + ".j=E.i WHERE "
						+ oldTableName + ".i!=" + oldTableName + ".j ";
			}
			recursiveTableQuery=recursiveTableQuery + orderByClause;			

			executeDropQuery(newTableName, TABLE);
			executeCreateQuery(recursiveTableQuery);

			String countQuery = "SELECT COUNT(*) FROM " + newTableName;
			if (executeQuery(countQuery) == 0) {
				break;
			}
			d++;
		}
		
		String createQuery = "CREATE TABLE "+crvObj.recursiveTable+" AS SELECT " + distinct  + "  * FROM R1";
		for(int i=2;i<d;i++) {
			createQuery=createQuery+" UNION ALL SELECT * FROM R"+String.valueOf(i);
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
		executeDropQuery(targetTable, TABLE);

		if (selectionCondition.length() > 0) {
			selectionCondition = "," + selectionCondition;
		}

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

		if (!selectQuery.optimizedWhereCondition.equals("")) {
			previousTableDistinctQuery = previousTableDistinctQuery + " AND "
					+ selectQuery.optimizedWhereCondition;
		}

		executeCreateQuery(previousTableDistinctQuery);

		if (opt.intermediateProjections.equals("y")
				&& !targetTable.equals("RD")) {
			String projection = "CREATE PROJECTION IF NOT EXISTS "
					+ targetTable
					+ "_PROJ"
					+ "(i ENCODING RLE,j ENCODING RLE,p ENCODING RLE,v,d ENCODING RLE) AS SELECT i,j,p,v,d FROM "
					+ targetTable + " ORDER BY j,i,p,v";
			// executeDropQuery(targetTable+"_PROJ",PROJECTION);
			executeCreateQuery(projection);
			executeQuery(PROJECTION_REFRESH);
		}
	}

	@Override
	public void updateStartTime(String dataset, Optimizations opt,int depth) {
	String query="insert into colrq_timetrack values('"+dataset+"','"+opt.O1flag+"','"+opt.O2flag+"','"+opt.O3flag+"','"+opt.O4flag+"',"+depth+",'"+opt.intermediateProjections+"',"+"TIMESTAMP 'now')";
	executeCreateQuery(query);
	}
}
