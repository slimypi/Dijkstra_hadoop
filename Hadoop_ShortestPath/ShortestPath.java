

/*by SLIMANE AGOURAM
ENSIIE 3A/ILC 2A*/

import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

public class ShortestPath {
  //this class represents the mapper
  public static class ShortestPathMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    
     /* * 
   * representing graph: Adjacency list
   * 
   * A map task receives
   * Key: node n
   * Value: D (distance from start either -1 for infinite, or 0 for the starting node), points-to (list of nodes reachable from n)
   * The reduce task gathers possible distances to a given p and selects the minimum one
   */
      
    public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
      //parse the line in the input file
      String parts[] = value.toString().split("_"); //we split our entry line in two pieces : (Node + initial_cost) and (Neighbor nodes)
      String neigbor_nodes[] = parts[1].split(","); // we extract neighbor nodes for the current node
      String secondaryParts[] = parts[0].split("\t"); // we split then our first slice  into Node and initial_cost
      int cost = Integer.parseInt(secondaryParts[1]); //we fetch the initial cost
      String myKey = secondaryParts[0];
      if(cost != -1){ //if the current node is not the begining node then fetch neighbor_nodes and intermediar_costs 
	      for(int i=0; i<neigbor_nodes.length; i++){
	      		if(!neigbor_nodes[i].equals(" ")) // a test to distinguish nodes with no neighbors and not wput them in the context
	      			context.write(new Text(neigbor_nodes[i]), new Text((CostsMatrix.Dist(myKey.charAt(0),neigbor_nodes[i].charAt(0))+cost) + "_" + " "));
	      		
	      	}
	}
	context.write(new Text(myKey), new Text(cost + "_" + parts[1]));
    }
  }
  //This class is basically the matrix of our intermediar costs for the entry graph.

  public static class CostsMatrix
  {
  	private static int distance[][]={{0,3,4,1,-1,-1,-1},{-1,0,-1,-1,2,-1,-1},{-1,-1,0,-1,-1,-1,5},{-1,-1,-1,0,-1,1,-1},{-1,-1,-1,-1,0,-1,3},{-1,-1,3,-1,-1,0,-1},{-1,-1,-1,-1,-1,-1,0}};
  	public static int Dist(char a,char b){
  		int Inta = a - 'A';
  		int Intb = b - 'A';
  		return distance[Inta][Intb];
  	}
  }
  
  //Reducer for our ShortestPath Algorithm
  public static class ShortestPathReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
        //Key is node n
        //Value is D, Points-To
        //For every point (or key), look at everything it points to.

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
        
        List<String> v = new ArrayList<String>();
	//we put the nodes in a list so we can count their occurences 
        for (Text val : values) {
    	    v.add(val+"");
        }
        //if the key has only one value then no comparison needed
        if(v.size() == 1){
        	context.write(key,new Text(v.get(0)));
        }
        //if the count is more than 1 it means that we have new values to compare them and keep the minimum of them
        else{
		int costMin = -1;
		String neigbor_nodes = " ";
	      	for (int i=0;i<v.size();i++) {
	    	    String parts[] = v.get(i).toString().split("_");
	      	    String neighbor = parts[1];
	      	    int cost = Integer.parseInt(parts[0]);
	      	    //if the value of cost is equal to -1 it means that we have and infinite value
	      	    if(cost!= -1 && ((costMin != -1 && cost <costMin)|| (costMin==-1))){
	      	    	costMin = cost;
	      	    }
	      	    if(!neighbor.equals(" "))
	      	    	neigbor_nodes = neighbor;
		}
		context.write(key, new Text(costMin+"_"+neigbor_nodes));
        }
      	
    	
    }
  }
  //The next Method is used to see if two files match. if yes, then the method returns a 1 flag (int), if not then it returns 0 flag (int), and returns -1 
  // if  an error has occured.
  //This method will be used dureing the execution of multiple passes of Map/Reduce and will help decide when to stop the lagorithm:
    //if the output file of the current iteration matches the one from the iteration before, then we surely havve reached the final solution.
  public static int FileMatcher(String pathFile1, String pathFile2)
  {
  	BufferedReader reader1 = null; //initialise our buffers
  	BufferedReader reader2 = null;
  	try{
  		reader1 = new BufferedReader(new FileReader(pathFile1)); //trying to load file 1
  		reader2 = new BufferedReader(new FileReader(pathFile2));	// trying to load file 2
  	}
  	catch(FileNotFoundException e){
  		System.out.println("Error while trying to open : " + pathFile1 + " " + pathFile2 + " Exception : " + e);
  		return -1;
  	}
  	String line1="",line2=""; //We loaded the files, now what we have to do is browse into each one line by line.Before that we initialize our strings  
  	try{
  		line1=reader1.readLine(); //we try to read the line from the first file
  		line2=reader2.readLine(); //we try to read the line from second file
	  	while(line1 != null && line2!=null){ 
	  		if(!line1.equals(line2)) //if they do not match, then why go further? just return a non_matching flag
	  			return 0;
	  		line1=reader1.readLine();//if they match, we go through the next lines perfomring the same thing till the End of file
  			line2=reader2.readLine();
	  	}
	  	
	}
	catch(Exception e){
		System.out.println(e);
		return -1;
	}
  	if(line1==null && line2==null) //we have reached the end of file, it means we went through all the lines succesfully, Great, return 1
	  		return 1;
	else{
			
			return 0;
	}
  }



  public static void main(String[] args) throws Exception {
    String subFolderDirectory = "/part-r-00000"; //subfolderDirectory where to go look for the output of previous pass
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: ShortestPath <in> <out>");
      System.exit(2);
    }
    int i=0;
    do{ //the loop that launches the map/reduce passes .. we do at least one passe that's why we chose {do while} instead of {while do}.
	    i++;
	    Job Phase = new Job(conf, "ShortestPathPhaseNbr"+i);
	    Phase.setJarByClass(ShortestPath.class);
	    Phase.setMapperClass(ShortestPathMapper.class);
	    Phase.setReducerClass(ShortestPathReducer.class);
	    Phase.setOutputKeyClass(Text.class);
	    Phase.setOutputValueClass(Text.class);
	    if(i==1)	
	    	FileInputFormat.addInputPath(Phase, new Path(otherArgs[0]));
    	    else
    	    	FileInputFormat.addInputPath(Phase, new Path(otherArgs[1]+(i-1)+ subFolderDirectory));
	    //here we're using a temporary output folder that will containt the first output that will be used later in the second phase 
	    FileOutputFormat.setOutputPath(Phase, new Path(otherArgs[1]+i));
	    if(!Phase.waitForCompletion(true))
	    	System.exit(1);
	    
    }while((i==1)?true:(FileMatcher(otherArgs[1]+(i-1)+ subFolderDirectory,otherArgs[1]+i + subFolderDirectory)==0)); //compare current output with pervious 
    //output, if it matches it means we stop we have reached the final solution.
    
    System.exit(0);
   }
}
