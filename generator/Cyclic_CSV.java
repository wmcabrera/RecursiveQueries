public class Cyclic_CSV {

         public static void main(String[] args) throws Exception {
                int number_Of_Vertices=1000;
                int cost=1;
                FileWriter fileWriter = new FileWriter("E:\\datasets\\List\\ListDataSet-1kEdge.csv");
        PrintWriter printWriter = new PrintWriter(fileWriter);
       
                for(int i=2;i<=number_Of_Vertices;i++) {
                        printWriter.println((i-1)+","+i+","+cost);
                }

                printWriter.flush();
                printWriter.close();
                fileWriter.close();
        }
}
