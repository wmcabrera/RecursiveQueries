import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class HelperFunctions {
	protected Connection connection;
	protected PrintWriter sqloutput;
	public static final String TABLE = "table";
	
	public HelperFunctions() {
		connection = null;
	}

	public abstract void getConnection(String fileName);

	public abstract void createTempTable(Optimizations opt, CRV crvObj,
			SelectQuery selectQuery); 
	
	public abstract void createTempJoinTable(Optimizations opt, CRV crvObj,
			SelectQuery selectQuery, String tableName, String selectionCondition);
	
	public int executeQuery(String query) {
		try {
			Statement statement = connection.createStatement();
			sqloutput.println(query);
			sqloutput.println();
			ResultSet rs = statement.executeQuery(query);
			rs.next();
			return rs.getInt(1);
			
			
		} catch (Exception e) {
			System.out.println(e);
			System.out.println("Error while executing query " + query);
		}
		return -1;
	}
	public void executeRefresh() {
		try {
			Statement statement = connection.createStatement();
			sqloutput.println("SELECT START_REFRESH()");
			sqloutput.println();
			ResultSet rs = statement.executeQuery("SELECT START_REFRESH()");
		} catch (Exception e) {
			System.out.println(e);
			System.out.println("Error while executing refresh query ");
		}
	}

	public void executeCreateQuery(String query) {
		try {
			Statement statement = connection.createStatement();
			sqloutput.println(query);
			sqloutput.println();
			statement.executeUpdate(query);
			
		} catch (Exception e) {
			System.out.println("Error while executing query " + query);
			System.out.println(e);
			System.exit(0);
		}
	}

	public void executeDropQuery(String tableName,String type) {
		try {
			Statement statement = connection.createStatement();
			String dropIfExistsQuery = "DROP "+type+" IF EXISTS " + tableName+" CASCADE";
			sqloutput.println(dropIfExistsQuery);
			sqloutput.println();
			statement.executeUpdate(dropIfExistsQuery);			
			
		} catch (Exception e) {
			System.out.println("Error while executing drop query ");
			System.out.println(e);
			System.exit(0);
		}
	}

	public abstract void semiNaiveRecursion(Optimizations opt, CRV crvObj,
			SelectQuery selectQuery);

	public abstract void apply_Optimizations_On_Temporary_Table(String targetTable,
			String sourceTable, Optimizations opt, CRV crvObj,
			SelectQuery selectQuery, String selectionCondition);
	
	public void deleteIntermediateTables(int d) {
		executeDropQuery("E",TABLE);
		for (int i = 1; i < d; i++) {
			String tableName = "R" + String.valueOf(i);
			executeDropQuery(tableName,TABLE);
		}
	}


	public abstract void updateStartTime(String dataset,Optimizations opt,int depth) ;
	
	public void updateEndTime() {
		String query="update colrq_timetrack set endtime=timestamp 'now' where endtime is null";
		executeCreateQuery(query);
	}
}
