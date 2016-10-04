import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Random;


public class TreeClique {
	
	static int cost=1;
	static long counter=0;
	public static void main(String[] args) throws Exception {
		FileWriter fileWriter = new FileWriter("E:\\datasets\\clique\\tree\\TreeClique-10mEdge.csv");
        PrintWriter printWriter = new PrintWriter(fileWriter);
        
		//denoted as M
		int number_of_cliques=15;
		//denoted as K
		int clique_size=5;
		
		createCliques(number_of_cliques,clique_size,printWriter);
		
		MTreeClique(number_of_cliques,clique_size,printWriter);
		
		System.out.println("Total number of edges : "+counter);
		
		printWriter.flush();
        printWriter.close();
        fileWriter.close();
		
	}
	
	private static void MTreeClique(int M,int K,PrintWriter printWriter) {
		Random rn=new Random();
				
		int edge=0;
		int parent=1;
		int child=2;
		while(edge<M-1) {
			int start=(parent-1)*K+1;
			int end=(child-1)*K+1;
			int random1=start+Math.abs((rn.nextInt()%K));
			int random2=end+Math.abs((rn.nextInt()%K));
			printWriter.println(random1+","+random2+","+cost);
			counter++;
			child++;
			if(child%2==0)
				parent++;
			edge++;			
		}			
	}
	
	private static void createCliques(int M,int K,PrintWriter printWriter) {
		int i=1;
		
		for(int j=1;j<=M;j++) {
			//start and end
			int start=i;
			int end=i+K-1;
			for(int p=start;p<=end;p++) {
				for(int q=p+1;q<=end;q++) {
					printWriter.println(p+","+q+","+cost);
					printWriter.println(q+","+p+","+cost);					
					counter+=2;
				}
			}
			i=end+1;
		}
	}

}
