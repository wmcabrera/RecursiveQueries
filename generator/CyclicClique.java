import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Random;


public class CompleteClique {

        static int cost=1;
        static long counter=0;
        public static void main(String[] args) throws Exception {

                String str="abc2";
                System.out.println(Integer.parseInt(str.substring(str.length()-1)));

                FileWriter fileWriter = new FileWriter("E:\\datasets\\clique\\complete\\CompleteClique-10mEdge.csv");
        PrintWriter printWriter = new PrintWriter(fileWriter);
        
                //denoted as M
                int number_of_cliques=3150;
                //denoted as K
                int clique_size=8;

                createCliques(number_of_cliques,clique_size,printWriter);

                MCompleteClique(number_of_cliques,clique_size,printWriter);

                System.out.println("Total number of edges : "+counter);

                printWriter.flush();
        printWriter.close();
        fileWriter.close();

        }

        private static void MCompleteClique(int M,int K,PrintWriter printWriter) {
                Random rn=new Random();

                for(int i=1;i<=M;i++) {
                        for(int j=1;j<=M;j++) {
                                if(i!=j) {
                                        int start =(i-1)*K+1;
                                        int end=(j-1)*K+1;

                                        int random1=start+Math.abs((rn.nextInt()%K));
                                        int random2=end+Math.abs((rn.nextInt()%K));
                                        printWriter.println(random1+","+random2+","+cost);
                                        counter++;
                                }
                        }
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
