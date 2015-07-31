import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class KMeansHadoop extends Configured implements Tool{
	static int numberOfCentroids = 4;
	static int numberOfAttributes=10;	

	public static boolean shouldContinue = true;
	public static int iterationCount = 0;
	
	@Override
	public int run(String[] args) throws Exception {
		
    	
		//Exit if the paths not provided.
		if(args.length!=3){
			System.out.println("Invalid Parameters");
			return -1;
		}
				
        //Creating a Job
        Configuration conf = getConf(); // THIS IS THE CORRECTWAY

      conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
        
      while(shouldContinue){
	    	String ip,op;
	    	conf.set("iterationCount", (iterationCount+1)+""); //Always before intitializing job
	        Job job = new Job(conf, "K Means");
	      
	        //Driver Class
	        job.setJarByClass(KMeansHadoop.class);
	        
	        //Setting Mapper, Reducer and Partitioner Classes
	        job.setMapperClass(KMeansMapper.class);
	        job.setReducerClass(KMeansReducer.class);

	        job.setPartitionerClass(KMeansPartitioner.class);
  
	        //Setting Mapper Key and Value output type
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
	        //Setting final Key and Value Type
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        
	        job.setInputFormatClass(KeyValueTextInputFormat.class);
	        
	        //Set number of Reducers
	        job.setNumReduceTasks(4);
	
	        //Creating file-paths

			ip=args[0];	

	    	op = args[1]+(iterationCount+1);	
	        
	        //Setting the paths
	        FileInputFormat.setInputPaths(job, new Path(ip));
	        FileOutputFormat.setOutputPath(job, new Path(op));
	        
	        //Running the job.
	        job.waitForCompletion(true);
	        
	        //Getting the value of counter.

	        iterationCount++;
	        
	        int i =shouldNextIteration(conf);
	        System.out.println(""+i);
	        
	        
      }
        return 0;
    }
	
	
	int shouldNextIteration(Configuration conf) throws IOException{
		
		//Merging output from reducer
		FileSystem fs = FileSystem.get(conf);
		Path reducerOutput = new Path("kmeansOutput/out1"); // FOR JAR
	    Path centroidOld = new Path("centroid/centroid_"+(iterationCount)+".txt");
	    Path centroidNew = new Path("centroid/centroid_"+(iterationCount+1)+".txt");
	    
	    FileUtil.copyMerge(fs,reducerOutput,fs,centroidNew,false,conf,null);
	    
	    
	    //Reading new centroid file
	    String line;
	    Datapoint[] newCentroid = new Datapoint[numberOfCentroids];
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroidNew)));
		
		for(int x=0;x<numberOfCentroids;x++)
		{
				line= br.readLine();
				String[] centroidString = line.split("\t");
				newCentroid[x]= new Datapoint(centroidString[0],centroidString[1]);
				
				System.out.println(centroidString[0] + "===VSD NEW===="+ centroidString[1]);
		}
		br.close();
		
		
		
		//Reading old centroid
		line="";
		Datapoint[] oldCentroid = new Datapoint[numberOfCentroids];
		br = new BufferedReader(new InputStreamReader(fs.open(centroidOld)));
		for(int x=0;x<numberOfCentroids;x++)
		{
			line=br.readLine();
			String[] centroidString = line.split("\t");
			oldCentroid[x]= new Datapoint(centroidString[0],centroidString[1]);
			System.out.println(centroidString[0] + "===VSD OLD===="+ centroidString[1]);
		}
		br.close();
		
	
		//Comparing the change from previous centroid
		for(int i = 0; i<numberOfCentroids;i++){
	
				
					for(int k = 0;k<numberOfAttributes;k++){
						if(0.02<Math.abs(newCentroid[i].getVal(k)-oldCentroid[i].getVal(k))){
							shouldContinue = true;
							System.out.println("Should continue: "+ shouldContinue);
							return 1;
						}
						else{
							shouldContinue = false;
							System.out.println("Should continue: "+ shouldContinue);
						}
					}
		}
		return 0;
	}


    public static void main(String[] args) throws Exception {
    	
        int res = ToolRunner.run(new Configuration(), new KMeansHadoop(), args);
        System.exit(res);
    }

}
