
public class Optimizations {
	public String O1flag;
	public String O2flag;
	public String O3flag;
	public String O4flag;
	public boolean canOptimize=false;
	public String intermediateProjections;
	public String createIndex1;
	public String createIndex2;
	Optimizations() {
		O1flag="n";
		O2flag="n";
		O3flag="n";
		O4flag="n";
		intermediateProjections="n";
		createIndex1="n";
		createIndex2="n";
	}
	
}

class CRV {
	String query;
	String baseTable;
	String recursiveTable;
	String[] recursiveTableAttr;
	String baseStep;
	String[] baseStepProjections;
	String[] recursiveStepProjections;
	String recursiveStep;
	String joinCondition;
	String JoinAttrLeft;
    String JoinAttrRight;
    String JoinTableLeft;
    String JoinTableRight;
	String i,j,v,p,d;
	int recursionDepth;
	
	CRV(String query) {
		this.query=query;
	}
}

class SelectQuery {
	String query;
	boolean hasDistinct=false;
	boolean hasJoin=false;
	boolean hasWhere=false;
	boolean hasGroupBy=false;
	
	String whereCondition="";
	String optimizedWhereCondition="";
	String[] projectedColumns;
	String pAggregation=null;
	String vAggregation=null;
	
	String joinCondition=null;
	String joinLeftTable=null;
	String[] joinLeftTableAttr = new String[2];
	String[] joinRightTable = new String[2];
	String[] joinRightTableAttr = new String[2];
	String [] joinRightTableAlias=new String[2];
	
	int recursionDepth;
	
	public SelectQuery(String query) {
		this.query=query;
		this.recursionDepth=-1;
	}
}