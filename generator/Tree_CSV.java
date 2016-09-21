import java.io.FileWriter;
import java.io.PrintWriter;

public class Tree_CSV {

        public static void main(String[] args) throws Exception {
                FileWriter fileWriter = new FileWriter("tree10m.csv");
        PrintWriter printWriter = new PrintWriter(fileWriter);
        int cost=1;
        int start_Vertex=1;
        int end_Vertex=1;
        int number_Of_Edges=0;
        while(true) {
                int i=start_Vertex;
                int j=end_Vertex+1;
                while(i<=end_Vertex) {
                        printWriter.println(i+","+j+","+cost);
                        printWriter.println(i+","+(j+1)+","+cost);
                        i=i+1;
                        j=j+2;
                        number_Of_Edges+=2;
                        if(number_Of_Edges>10000000) {
                                break;
                        }
                }
                start_Vertex=end_Vertex+1;
                end_Vertex=j-1;
                if(number_Of_Edges>10000000) {
                        break;
                }
        }
        
        printWriter.flush();
        printWriter.close();
        fileWriter.close();
        
        }
}
