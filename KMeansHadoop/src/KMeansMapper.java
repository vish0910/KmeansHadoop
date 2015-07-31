import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class KMeansMapper extends Mapper<Text, Text, Text,Text>{
	static int numberOfCentroids = 4;
	static int numberOfAttributes = 10; // 10 required for assignment
	Datapoint[] centroid = new Datapoint[numberOfCentroids];
	FileSystem fs;
	BufferedReader br = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		super.setup(context);
		Configuration conf = context.getConfiguration();
		
		createCentroid(conf.get("iterationCount"));		

	}
	
	// Creating centroid from the file
	public void createCentroid(String s) throws IOException{
		
		FileSystem fs = FileSystem.get(new Configuration());
		String line="";
		Path inputFilePath= new Path("centroid/centroid_"+s+".txt");


		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputFilePath)));

		for(int x=0;x<numberOfCentroids;x++)
		{
			line=br.readLine();
			String[] centroidString = line.split("\t");
			centroid[x]= new Datapoint(centroidString[0],centroidString[1]);
		}
		br.close();
	}
	
	// Calculate euclidian distance
	double calculateEuclideanDistance(Datapoint cent, Datapoint dp){
		double dist=Double.MAX_VALUE;
		
		double sumOfSquare=0;
		for(int i= 0; i<numberOfAttributes;i++){
			sumOfSquare+=Math.pow((cent.getVal(i)-dp.getVal(i)),2);
		}
		dist=Math.sqrt(sumOfSquare);
		return dist;
	}
	
	@Override
  	//Process every node that is gray and explore its neighbors
  	public void map(Text key, Text value, Context context) throws IOException, InterruptedException, ArrayIndexOutOfBoundsException{
		Datapoint datapoint;
  		String dataPointID=key.toString();
  		String datapointAttributes = value.toString();
  		//Comparing each datapoint to every centroids
  		datapoint = new Datapoint(dataPointID, datapointAttributes);
  		double minDis=Double.MAX_VALUE;
  		int cluster = -1; //-1 should be the default value//So that it throws ArrayIndexOutOfBound Exception
  		for(int j = 0 ; j < numberOfCentroids ;j++){
  			double dist=calculateEuclideanDistance(centroid[j], datapoint);
  			if(minDis>dist){
  				minDis=dist;
  				cluster=j;
  			}
  		}
  		context.write(new Text(centroid[cluster].getId()),new Text(datapoint.toString()) );
  	}// End of map()	
	
  }// End of Mapper   
