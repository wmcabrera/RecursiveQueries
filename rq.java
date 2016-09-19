/*
 * sample input call for this rq.java program
 * 
 * file=input.txt;O1=Y;O2=Y;O3=Y;O4=Y;database=column;intermediateProjections=Y;createBuckets=Y
 */


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.StringTokenizer;


public class rq {

	public static final String INVALID_QUERY = "invalidquery";
	public static final String SELECT = "select";
	public static final String FROM = "from";
	public static final String INTO = "into";
	public static final String DISTINCT = "distinct";
	public static final String WHERE = "where";
	public static final String JOIN = "join";
	public static final String GROUPBY = "group by";
	public static final String ON = "on";
	public static final String AS = "as";
	public static final String CREATE_RECURSIVE_VIEW="create recursive view";
	public static final String INSERT_INTO = "insert into ";
	public static final String ARGUMENT_SEPARATOR = ",";
	public static final String TABLE = "table";
	
	public static void parser(String args,int depth) throws Exception {
		
		HelperFunctions helper;
		
		Optimizations opt = new Optimizations();
		String[] input=args.split(ARGUMENT_SEPARATOR);		
		String fileName = input[0].substring(input[0].indexOf("=")+1);
		
		String fileNameWithoutTxt = fileName.substring(0,fileName.length()-4);
		opt.O1flag= input[1].substring(input[1].indexOf("=")+1).toLowerCase();
		opt.O2flag= input[2].substring(input[2].indexOf("=")+1).toLowerCase();
		opt.O3flag= input[3].substring(input[3].indexOf("=")+1).toLowerCase();
		opt.O4flag= input[4].substring(input[4].indexOf("=")+1).toLowerCase();
		
		opt.intermediateProjections= input[6].substring(input[6].indexOf("=")+1).toLowerCase();
		opt.createIndex1= input[7].substring(input[7].indexOf("=")+1).toLowerCase();
		opt.createIndex2= input[8].substring(input[8].indexOf("=")+1).toLowerCase();

		if(input[5].substring(input[5].indexOf("=")+1).toLowerCase().equalsIgnoreCase("column")) {
			helper=new HelperFunctionsColumn();
			helper.getConnection(fileNameWithoutTxt+"_proj"+opt.intermediateProjections+"_"+opt.O1flag+opt.O2flag+opt.O3flag+opt.O4flag);
		}else {
			helper=new HelperFunctionsRow();
			helper.getConnection(fileNameWithoutTxt+"_orderby"+opt.createIndex2+"_"+opt.O1flag+opt.O2flag+opt.O3flag+opt.O4flag);
		}
		
				
		//parse the two queries from input file
		File file = new File(fileName);
	    BufferedReader in = new BufferedReader(new FileReader(file));
	    String[] queries=new String[10];
	    String line,query="";
	    int i=0;
	    while ((line = in.readLine()) != null) {
	    	query=query+line;
	    	if(line.trim().endsWith(";")) {
	    		query=query.trim();
	    		query=query.substring(0, query.length()-1);
	    		query=formatQuery(query);
	    		
	    		if(query.startsWith(CREATE_RECURSIVE_VIEW) || query.startsWith(CREATE_RECURSIVE_VIEW.toUpperCase())) {
	    			query=query.toLowerCase();
	    			query=query.replace("/* basic query */", "");
	    			query=query.replace("/* recursive query */", "");
	    			queries[i++]=query;
	    		}else if(query.startsWith(SELECT) || query.startsWith(SELECT.toUpperCase())) {
	    			queries[i++]=format_SELECT_Query(query);
	    		}
	    		query="";
	    	}
	    }
	    in.close();	   
	    helper.updateStartTime(fileNameWithoutTxt,opt,depth);
	    apply_Optimizations(helper,queries,opt);	
	    helper.updateEndTime();
	}
	
	private static CRV parse_Recursive_Query(String query) {
		CRV crvObj = new CRV(query);
		
		String tableColumns = get_Table_Columns(query);
		crvObj.recursiveTable=tableColumns.substring(0, tableColumns.indexOf("(")).trim();
		crvObj.recursiveTableAttr=tableColumns.substring(tableColumns.indexOf("(")+1, tableColumns.indexOf(")")).trim().split(",");
		
		int beginIndex= query.indexOf(SELECT);
		int endIndex = query.indexOf("union all")-1;
		crvObj.baseStep = query.substring(beginIndex, endIndex).trim();
		
		crvObj.baseTable=getTableName(crvObj.baseStep).trim();
		crvObj.baseStepProjections=getProjections(crvObj.baseStep);
		
		beginIndex = query.indexOf(SELECT,beginIndex+1);
		endIndex = query.lastIndexOf(")");
		String recursiveStepQuery = query.substring(beginIndex, endIndex).trim();
		recursiveStepQuery = formatQuery(recursiveStepQuery);
		
		crvObj.recursiveStep=recursiveStepQuery;
		crvObj.recursiveStepProjections=getProjections(recursiveStepQuery);
		
		String joinCondition = get_Join_Condition(recursiveStepQuery);
		String[] columns = joinCondition.split("=");
		
		crvObj.joinCondition=joinCondition;
		crvObj.JoinTableLeft=columns[0].substring(0, columns[0].indexOf(".")).trim();
		crvObj.JoinTableRight=columns[1].substring(0, columns[1].indexOf(".")).trim();
		crvObj.JoinAttrLeft=columns[0].substring(columns[0].indexOf(".")+1).trim();
		crvObj.JoinAttrRight=columns[1].substring(columns[1].indexOf(".")+1).trim();
		
		String selectionCondition = getSelectionConditions(recursiveStepQuery);
		crvObj.recursionDepth = Integer.parseInt(selectionCondition.substring(selectionCondition.indexOf("<")+1));
				
		crvObj.d=crvObj.recursiveTableAttr[0];
		crvObj.i=crvObj.recursiveTableAttr[1];
		crvObj.j=crvObj.recursiveTableAttr[2];
		crvObj.p=crvObj.recursiveTableAttr[3];
		crvObj.v=crvObj.recursiveTableAttr[4];
		
		return crvObj;
	}
	
	private static String get_Join_Condition(String query) {
		int beginIndex = query.indexOf(ON)+ON.length();				
		return query.substring(beginIndex+1,query.indexOf(WHERE)).trim();
	}
	
	private static SelectQuery parse_Select_Query(String query, Optimizations opt, CRV crvObj) {
		SelectQuery selectQuery = new SelectQuery(query);
		String[] columnArray = crvObj.recursiveTableAttr;
		
		if(query.contains(DISTINCT) && opt.O1flag.equals("y")) {
			selectQuery.hasDistinct=true;
		}
		if(query.contains(WHERE) && opt.O3flag.equals("y")) {
			selectQuery.hasWhere=true;
			String optimizedWhereCondition="";
			String selectionCondition = getSelectionConditions(query);
			selectQuery.whereCondition=selectionCondition;
			System.out.println(selectionCondition);
			String[] conditions = selectionCondition.split("and");	
/*			
			for(String condition:conditions) {
				condition=condition.trim();
				if(condition.startsWith(columnArray[1])) {
					condition=condition.replaceFirst(columnArray[1], "i");
					optimizedWhereCondition = optimizedWhereCondition+condition+" and ";
				}else if(condition.startsWith(columnArray[1].toUpperCase())) {
					condition=condition.replaceFirst(columnArray[1].toUpperCase(), "i");
					optimizedWhereCondition = optimizedWhereCondition+condition+" and ";				
				}
				else if(condition.startsWith(columnArray[4])) {
					if(condition.contains("<=") || condition.contains("<")) {
						condition=condition.replaceFirst(columnArray[4], "v");
						optimizedWhereCondition = optimizedWhereCondition+condition+" and ";
					}
				}
				else if(condition.startsWith(columnArray[4].toUpperCase())) {
					if(condition.contains("<=") || condition.contains("<")) {
						condition=condition.replaceFirst(columnArray[4].toUpperCase(), "v");
						optimizedWhereCondition = optimizedWhereCondition+condition+" and ";
					}
				}
				else if((condition.startsWith(columnArray[0]) || condition.startsWith(columnArray[0].toUpperCase())) && !(condition.startsWith(columnArray[4]) || condition.startsWith(columnArray[3])|| condition.startsWith(columnArray[2]) || condition.startsWith(columnArray[1]))) {
					//we will check if where clause has another condition on d.. so that we can stop the recursive query before itself
					if(!condition.contains(">")) {
						if(condition.contains("<=")) {
							selectQuery.recursionDepth = Integer.parseInt(condition.substring(condition.indexOf("<=")+2));
						}else if(condition.contains("<")) {
							selectQuery.recursionDepth = Integer.parseInt(condition.substring(condition.indexOf("<")+1));
						}else if(condition.contains("=")) {
							selectQuery.recursionDepth = Integer.parseInt(condition.substring(condition.indexOf("=")+1));
						}
					}
				}
			}
			if(optimizedWhereCondition.length()>0) {
				optimizedWhereCondition=optimizedWhereCondition.substring(0, optimizedWhereCondition.length()-5);
				selectQuery.optimizedWhereCondition=optimizedWhereCondition;
			}	
*/			
		}
		if(query.contains(JOIN) && opt.O2flag.equals("y")) {
			selectQuery.hasJoin=true;
			
			int beginIndex = query.indexOf(FROM)+FROM.length();
			int endIndex = query.indexOf(32, beginIndex+1);
			selectQuery.joinLeftTable = query.substring(beginIndex+1, endIndex);
			
			beginIndex = endIndex+1;
			String[] joinLeftTableAttr = new String[2];
			String[] joinRightTable = new String[2];
			String[] joinRightTableAttr = new String[2];
			int counter=0;
			
			while(query.indexOf(JOIN, beginIndex)>-1) {
				int index = query.indexOf(JOIN, beginIndex)+JOIN.length();
				
				endIndex = query.indexOf(32, index+1);
				joinRightTable[counter]=query.substring(index+1, endIndex);
				
				index = query.indexOf(AS,endIndex+1)+AS.length();
				endIndex = query.indexOf(32,index+1);
				selectQuery.joinRightTableAlias[counter]=query.substring(index, endIndex).trim();
				
				index = query.indexOf(ON,endIndex+1)+ON.length();
				
				//get join condition
				endIndex = query.indexOf(32,index+1);
				if(endIndex==-1) {
					endIndex = query.length();
				}
				String condition = query.substring(index, endIndex).trim();
				selectQuery.joinCondition=condition;
				String[] joinConditionTableColumns = condition.split("=");
				
				index = joinConditionTableColumns[0].indexOf(".");
				if(joinConditionTableColumns[0].substring(0,index).equalsIgnoreCase(crvObj.recursiveTable)) {
					joinLeftTableAttr[counter]=joinConditionTableColumns[0].substring(index+1);
				}else {
					joinRightTableAttr[counter]=joinConditionTableColumns[0].substring(index+1);
				}
				index = joinConditionTableColumns[1].indexOf(".");
				if(joinConditionTableColumns[1].substring(0,index).equalsIgnoreCase(crvObj.recursiveTable)) {
					joinLeftTableAttr[counter]=joinConditionTableColumns[1].substring(index+1);
				}else {					
					joinRightTableAttr[counter]=joinConditionTableColumns[1].substring(index+1);
				}
				counter++;
				beginIndex = endIndex+1;
			}
			
			selectQuery.joinLeftTableAttr=joinLeftTableAttr;
			selectQuery.joinRightTable=joinRightTable;
			selectQuery.joinRightTableAttr=joinRightTableAttr;			
		}
		
		if(query.contains(GROUPBY) && opt.O4flag.equals("y")) {
			selectQuery.hasGroupBy=true;
			boolean ijCondition=false;
			String aggregations = getAggregations(query);
			String[] aggreationArray = aggregations.split(",");			
			for(String a: aggreationArray) {
				if(a.equalsIgnoreCase(columnArray[1])) {
					ijCondition=true;
				}else if(a.equalsIgnoreCase(columnArray[2])) {
					ijCondition=true;
				}else if(a.equalsIgnoreCase(columnArray[0])) {
				}
				else {
					ijCondition=false;
					break;
				}
			}
			
			
			if(ijCondition==true) {
				//String groupByQuery = GROUPBY+" "+columnArray[1]+","+columnArray[2];								
				String projections[]=getProjections_INTO(query);				
				for(String projection:projections) {
					if(projection.contains(columnArray[3])) {
						selectQuery.pAggregation=projection.substring(0, projection.indexOf("("));
					}else if(projection.contains(columnArray[4])) {
						selectQuery.vAggregation=projection.substring(0, projection.indexOf("("));
					}
				}
			}else {
				selectQuery.hasGroupBy=false;
			}
		}
		
		int beginIndex = query.indexOf(SELECT)+SELECT.length();
		int endIndex=-1;
		if(query.indexOf(INTO)!=-1){
			endIndex = query.indexOf(INTO);
		}else {
			endIndex = query.indexOf(FROM);
		}
		String projectedColumns =  query.substring(beginIndex+1, endIndex).trim();
		if(projectedColumns.contains(DISTINCT)) {
			projectedColumns=projectedColumns.substring(DISTINCT.length()+1);
		}
		selectQuery.projectedColumns = query.substring(beginIndex+1, endIndex).split(",");
			
		return selectQuery;
	}
	
	private static void apply_Optimizations(HelperFunctions helper,String[] queries,Optimizations opt) {
		CRV crvObj = parse_Recursive_Query(queries[0]);
		SelectQuery selectQuery = parse_Select_Query(queries[1], opt, crvObj);
		
		opt.canOptimize = is_Optimization_Valid(crvObj.recursiveTable, selectQuery.joinLeftTable);
		
		
		//create temporary table based on join condition is possible or not
		helper.createTempTable(opt,crvObj,selectQuery);
		helper.semiNaiveRecursion(opt, crvObj, selectQuery);
	
		String tableName = get_Table_Name_Select_Into(queries[1]);
		helper.executeDropQuery(tableName,TABLE);
		helper.executeCreateQuery(queries[1]);
		
	}		
	
	private static boolean is_Optimization_Valid(String recursiveTable, String joinLeftTable) {
		if(recursiveTable.equalsIgnoreCase(joinLeftTable)) {
			return true;
		}
		return false;
	}		
	
	private static String get_Table_Columns(String query) {
		int beginIndex = query.indexOf(CREATE_RECURSIVE_VIEW)+CREATE_RECURSIVE_VIEW.length();
		int endIndex = query.indexOf("as")-1;
		String tableColumns = query.substring(beginIndex+1, endIndex);
		return tableColumns.trim();
	}	
	
	private static String getAggregations(String query) {
		if(!query.contains(GROUPBY))
			return null;
		
		int beginIndex = query.indexOf(GROUPBY)+GROUPBY.length();
		int endIndex = query.indexOf(" ", beginIndex+1);
		if(endIndex==-1)
			endIndex = query.length();
		return query.substring(beginIndex, endIndex).trim();
	}
	
	
	private static String[] getProjections(String query) {
		int nextIndex = query.indexOf(SELECT)+SELECT.length();
		
		if(query.charAt(nextIndex)!=' ') {
			return null;
		}
		
		//now get index of FROM
		int fromIndex = query.indexOf(FROM)-1;
		if(query.charAt(fromIndex)!=' ') {
			return null;
		}
		
		
		String columnNames = query.substring(nextIndex+1, fromIndex);
		String columnArray[] = columnNames.split(",");
		
		for(int i=0;i<columnArray.length;i++) {
			columnArray[i]=columnArray[i].trim();
		}
		
		return columnArray;
	}
	private static String[] getProjections_INTO(String query) {
		int nextIndex = query.indexOf(SELECT)+SELECT.length();
		
		if(query.charAt(nextIndex)!=' ') {
			return null;
		}
		
		int fromIndex = query.indexOf(INTO)-1;
		if(query.charAt(fromIndex)!=' ') {
			return null;
		}
		
		
		String columnNames = query.substring(nextIndex+1, fromIndex);
		String columnArray[] = columnNames.split(",");
		
		for(int i=0;i<columnArray.length;i++) {
			columnArray[i]=columnArray[i].trim();
		}
		
		return columnArray;
	}
	
	private static String getTableName(String query) {
		int tableIndexStart = query.indexOf(FROM)+FROM.length();
		int tableIndexEnd = query.indexOf(" ", tableIndexStart+1);
		if(tableIndexEnd==-1) {
			tableIndexEnd=query.length();
		}
		return query.substring(tableIndexStart+1,tableIndexEnd);
		
	}
	
	private static String get_Table_Name_Select_Into(String query) {
		int tableIndexStart = query.indexOf(INTO)+INTO.length();
		int tableIndexEnd = query.indexOf(FROM);		
		return query.substring(tableIndexStart+1,tableIndexEnd).trim();
		
	}
	
	private static String getSelectionConditions(String query) {
		if(!query.contains(WHERE))
			return null;
		
		int beginIndex = query.indexOf(WHERE)+WHERE.length();
	
		int endIndex;
		if(query.contains(GROUPBY))
			endIndex=query.indexOf(GROUPBY);
		else
			endIndex=query.length();
		
		String selectionCondition = query.substring(beginIndex, endIndex).trim();
		return selectionCondition;
	}
	
	private static String formatQuery(String query) {
		
		//remove multiple spaces in the query
		StringTokenizer stringTokenizer = new StringTokenizer(query, " ");
	    StringBuffer stringBuffer = new StringBuffer();
	    while(stringTokenizer.hasMoreElements()){
	        stringBuffer.append(stringTokenizer.nextElement()).append(" ");
	    }
		query=stringBuffer.toString().trim();
		
		String[] operatorList = {"<","<>","=",">",">=","<=",",","+"};
		for(String operator:operatorList) {
			int beginIndex=0;
			while(true) {
				int index = query.indexOf(operator, beginIndex)-1;
				if(index<0)
					break;				
				if(query.charAt(index)==' ') {
					query=query.substring(0,index)+query.substring(index+1);
				}
				int topIndex = query.indexOf(operator,beginIndex)+operator.length();
				if(query.charAt(topIndex)==' ') {
					query=query.substring(0,topIndex)+query.substring(topIndex+1);
				}
				beginIndex=index+2;
			}		
		}
		return query;
	}
	
	private static String format_SELECT_Query(String query) {
		if(query.contains(SELECT.toUpperCase())) {
			query=query.replace(SELECT.toUpperCase(), SELECT.toLowerCase());
		}
		if(query.contains(FROM.toUpperCase())) {
			query=query.replace(FROM.toUpperCase(), FROM.toLowerCase());
		}
		if(query.contains(WHERE.toUpperCase())) {
			query=query.replace(WHERE.toUpperCase(), WHERE.toLowerCase());
		}
		if(query.contains(INTO.toUpperCase())) {
			query=query.replace(INTO.toUpperCase(), INTO.toLowerCase());
		}
		String AND_SPACE = " and ";
		if(query.contains(AND_SPACE.toUpperCase())) {
			query=query.replace(AND_SPACE.toUpperCase(), AND_SPACE.toLowerCase());
		}
		if(query.contains(GROUPBY.toUpperCase())) {
			query=query.replace(GROUPBY.toUpperCase(), GROUPBY.toLowerCase());
		}
		if(query.contains(JOIN.toUpperCase())) {
			query=query.replace(JOIN.toUpperCase(), JOIN.toLowerCase());
		}
		if(query.contains(ON.toUpperCase())) {
			query=query.replace(ON.toUpperCase(), ON.toLowerCase());
		}
		if(query.contains(AS.toUpperCase())) {
			query=query.replace(AS.toUpperCase(), AS.toLowerCase());
		}
		return query;
	}
}
