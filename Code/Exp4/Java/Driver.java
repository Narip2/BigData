package exp4;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

public class Driver {
	//How many clusters
	public static int k = 2;
	public static int count = 0;
	//Contain x and y
	public static double[][] points = new double[155][2];
	//Contain centers
	public static double[][] centers = new double[k][2];
	public static int same(double[][] a, double[][] b) 
	{
		int flag = 0;
		for(int i = 0; i < k; i++)
		{
			if((a[i][0] != b[i][0])||(a[i][1] != b[i][1]))
				flag = 1;
		}
		if(flag == 1)return 1; //not the same
		return 0;
	}
	public static void main(String args[]) throws IOException, InterruptedException {
		double max_x = 0;
		double max_y = 0;
		//Read file to get initial points messages
		String path = "/home/narip/Atom/Big_Data/iris_data";
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line;
		while((line = br.readLine()) != null) {
			//x
			points[count][0] = Double.parseDouble(line.split(" ")[0].toString());
			//y
			points[count][1] = Double.parseDouble(line.split(" ")[1].toString());
			count++;
		}
		double min_x = points[0][0];
		double min_y = points[0][1];
		for(int i = 0; i < count; i++) {
			min_x = Math.min(min_x,points[i][0]);
			max_x = Math.max(max_x, points[i][0]);
			min_y = Math.min(min_y, points[i][1]);
			max_y = Math.max(max_y, points[i][1]);
		}
		//random centers
		for(int i = 0; i < k; i++) {
			centers[i][0] = min_x + Math.random()*(max_x - min_x);
			centers[i][1] = min_y + Math.random()*(max_y - min_y);
		}
		String forItr[] = {"",""};
		forItr[0] = path;
		int time = 0;
		double[][] old_centers = new double[k][2];
//		for(int i = 0; i < 10; i++) {
//			forItr[1] = "output" + time;
//			time++;
//			Iterator.main(forItr);
//			for(int j = 0; j < k; j++) {
//				System.out.println(centers[j][0]+" "+centers[j][1]);
//			}
//			System.out.println();
//		}
		do {
			for(int i = 0; i < k; i++) {
				old_centers[i][0] = centers[i][0];
				old_centers[i][1] = centers[i][1];
			}
			forItr[1] = "output" + time;
			time++;
			Iterator.main(forItr);
			for(int j = 0; j < k; j++) {
				System.out.println(centers[j][0]+" "+centers[j][1]);
			}
			System.out.println();
		}while(same(old_centers,centers) == 1);
		
		for(int i = 0; i < k; i++)
		{
			System.out.println(centers[i][0]);
			System.out.println(centers[i][1]);
		}
	}
}
