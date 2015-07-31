
public class Datapoint {

	private String id;
	private int numberOfAttributes = 10;
	private double[] attributes = new double[numberOfAttributes];
	
	Datapoint(String id, String att){
		this.id=id;
		String[] attri = att.split(",");
		for(int i=0;i< attri.length;i++){
			attributes[i]=Double.parseDouble(attri[i]);
		}
			
	}//End of Constructor
	
	public String toString(){
		//Just attributes
		String s="";
		for(int i=0;i<numberOfAttributes;i++){
			s+=attributes[i]+",";
		}
		s=s.substring(0, s.length()-1);
		return s;
	}
	
	public double getVal(int index){
		return attributes[index];
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public double[] getAttributes() {
		return attributes;
	}

	public void setAttributes(double[] attributes) {
		this.attributes = attributes;
	}
}//End of Class
