import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KMeansPartitioner extends Partitioner<Text,Text>{

	@Override
	public int getPartition(Text key, Text value, int noOfReducers) {
		// TODO Auto-generated method stub
		String s = key.toString();
		int i= Character.getNumericValue(s.charAt(1));
		return (i)/noOfReducers;
	}

}
