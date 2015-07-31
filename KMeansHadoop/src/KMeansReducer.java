import java.io.IOException;
import java.text.DecimalFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<Text,Text,Text,Text> {
	int numberOfCentroids = 4;
	int numberOfAttributes = 10; // 10 required for assignment
	double[] sumOfAttributes = new double[numberOfAttributes];
	FileSystem fs;
	DecimalFormat decfor = new DecimalFormat("##.##");
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{		
		int dpCount=0;
		for(int g=0;g<numberOfAttributes;g++){
			sumOfAttributes[g]=0;
		}
		
		//Summing the attributes of datapoints assigned to same cluster
		for (Text value: values){	
	   		Datapoint dp = new Datapoint(key.toString(),value.toString());
	   		for(int j = 0; j<numberOfAttributes;j++){
	   			sumOfAttributes[j]+=dp.getVal(j);
	   		}
	   		dpCount++;
		}
		String s = "";
		//Getting the average
		for(int j = 0;j<numberOfAttributes;j++){
			sumOfAttributes[j]=(sumOfAttributes[j]/dpCount);
		}
		
		//Compiling the string of attributes for the centroid
		for(int j=0;j<numberOfAttributes;j++){
			s+=decfor.format(sumOfAttributes[j])+",";
		}
		s=s.substring(0, s.length()-1);
		
		context.write(key,new Text(s));
	}
}//End of Reducer
