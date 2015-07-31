import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.DecimalFormat;


public class KMeansFileGenerator {
	int numberOfAttributes=10;
	static int n=1000; // Number of nodes
	DecimalFormat decfor = new DecimalFormat("##.##");
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		KMeansFileGenerator k = new KMeansFileGenerator();
		k.generateDatapointsFile();
		k.generateCentroidFile();
	}
	
	//Random number generator
	double random(){
		double r = (Math.random()*10); 
		r= Double.parseDouble(decfor.format(r));
		return r;
	}
	
	public void generateDatapointsFile(){
		String line,dataPointsAttributes;
		String s="";
		System.out.println("Kmeans File Generator: ");
		System.out.println("Enter number of nodes in the graph\nDefault Size is 1000 (Press return to continue)");
		try{
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			s= br.readLine();
			if(!s.isEmpty())
				n=Integer.parseInt(s);
			String path = System.getProperty("user.dir");
			path = path.replace("jars", "");
			PrintWriter pr = new PrintWriter(path+"/datapoints.txt","UTF-8");
			for(int i = 0; i<n; i++){
				line ="P";
				dataPointsAttributes ="";
				line+=(i+1)+"\t";
				for(int j=0; j<numberOfAttributes; j++ ){
					dataPointsAttributes+=random()+",";
				}
				line+=dataPointsAttributes.substring(0, (dataPointsAttributes.length()-1));
				System.out.println(line);
				pr.println(line);
			}
			pr.close(); // Closing output file
		}//Try
		catch(FileNotFoundException fe){
			System.out.println("File not found.");
		}
		catch(IOException ie){
			System.out.println("IO Exception");
		}
	}//end of method
	
	
	public void generateCentroidFile(){
		int n=4; // Number of centroids
		try{
			String path = System.getProperty("user.dir");
			path = path.replace("jars", "");
			BufferedReader br = new BufferedReader(new FileReader(path+"/datapoints.txt"));
			PrintWriter pr = new PrintWriter(path+"/centroid_1.txt","UTF-8");

			long temp=0;
			for(int i = 0; i<n; i++){
				temp= Math.round(Math.random() * (n-temp));
				for(long h=0; h<temp-1;h++){
					br.readLine();
				}
//				String centString="C"+br.readLine().substring(1);
				String[] tempString= br.readLine().split("\t");
				String centString= "C"+(i+1)+"\t"+tempString[1];
				pr.println(centString);
				System.out.println(centString);
			}
			pr.close(); // Closing output file
			br.close();
		}//Try
		catch(FileNotFoundException fe){
			System.out.println("File not found.");
		}
		catch(IOException ie){
			System.out.println("IO Exception");
		}
	}//end of method
	
}