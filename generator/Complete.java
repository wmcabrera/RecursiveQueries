import java.io.FileWriter;
import java.io.PrintWriter;


public class Complete_CSV {

        public static void main(String[] args) throws Exception {
                int number_Of_vertices =1000;
                int cost=1;
                FileWriter fileWriter = new FileWriter("E:\\datasets\\complete\\CompleteDataSet-1MEdge.csv");
        PrintWriter printWriter = new PrintWriter(fileWriter);
       
        for(int i=1;i<=number_Of_vertices;i++) {
                for(int j=1;j<=number_Of_vertices;j++) {
                        if(i!=j) {
                                printWriter.println(i+","+j+","+cost);
                        }
                }
        }
        
        printWriter.flush();
        printWriter.close();
        fileWriter.close();

        }
}
