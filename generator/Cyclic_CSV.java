public class Cyclic_CSV {

        public static void main(String[] args) throws Exception {
                int number_Of_Vertices=100000;
                int cost=1;
                FileWriter fileWriter = new FileWriter("E:\\datasets\\cyclic\\CyclicDataSet-200kEdge.csv");
        PrintWriter printWriter = new PrintWriter(fileWriter);
        int j,k;
        for(int i=1;i<=number_Of_Vertices;i++) {
        
                while((j=(new Random()).nextInt(number_Of_Vertices)+1)==i);
                printWriter.println(i+","+j+","+cost);
        
                while((k=(new Random()).nextInt(number_Of_Vertices)+1)==i || k==j);
                printWriter.println(i+","+k+","+cost);
        
        }
        
        printWriter.flush();
        printWriter.close();
        fileWriter.close();
        }
}
