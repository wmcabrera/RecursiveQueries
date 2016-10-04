import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Random;
import java.lang.Integer;


public class TreeClique {

        static int cost=1;
        static long counter=0;
        public static void main(String[] args) throws Exception {

                String str="abc2";
                int N = Integer.parseInt(args[0] );
                int clique_size = Integer.parseInt(args[1] );
                System.out.println(clique_size);


                //denoted as K
                int number_of_cliques=  N/clique_size;
                int sizeE = number_of_cliques*clique_size*( clique_size -1) + number_of_cliques-1;

                FileWriter fileWriter = new FileWriter("treeclique"+sizeE+".csv");
                PrintWriter printWriter = new PrintWriter(fileWriter);

                createCliques(number_of_cliques,clique_size,printWriter);

                MTreeClique(number_of_cliques,clique_size,printWriter);

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


        private static void MTreeClique(int number_of_cliques,int K,PrintWriter printWriter) {
                Random rn=new Random();

        /*
                while(edge<M-1) {
                        int start=(parent-1)*K+1;
                        int end=(child-1)*K+1;
                        int random1=start+Math.abs((rn.nextInt()%K));
                        int random2=end+Math.abs((rn.nextInt()%K));
                        counter++;
                        child++;
                        if(child%2==0)
                                parent++;
                        edge++;
                }
        */
                int cntEdges  =0;
                int a=1;
                while  ( cntEdges< number_of_cliques-1) {
                   int i = K*a;
                   int j1 = 2*K*a;  cntEdges++;
                   int j2=  j1+5; cntEdges++;
                   printWriter.println(i+","+j1+","+cost);
                   printWriter.println(i+","+j2+","+cost);
                   a++;
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
