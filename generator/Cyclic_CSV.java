public class Cyclic_CSV {

         public static void main(String[] args) throws Exception {
                
                int N = args[N];
                int number_Of_Vertices=N;
                int cost=1;
                FileWriter fileWriter = new FileWriter( "Cyclic"+ N+".csv");
        PrintWriter printWriter = new PrintWriter(fileWriter);
       
                for(int i=2;i<=number_Of_Vertices;i++) {
                        printWriter.println((i-1)+","+i+","+cost);
                }
                printWriter.println(N+","+"1," + cost);
                printWriter.flush();
                printWriter.close();
                fileWriter.close();
        }
}
