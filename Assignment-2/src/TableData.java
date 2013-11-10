import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Random;


public class TableData {

	public static void main(String[] args) throws FileNotFoundException {
		StringBuilder data = new StringBuilder(); 
		data.append(createEmployees());
		createTsvFile(data.toString(),"employees.tsv");
		data = new StringBuilder(); 
		data.append(createStudents());
		createTsvFile(data.toString(),"students.tsv");
		data = new StringBuilder(); 
		data.append(createTechDepts());
		createTsvFile(data.toString(),"techdepts.tsv");
	}
	
	private static String createEmployees(){
		Random random = new Random();
		int rnd = 0;
		StringBuilder employees = new StringBuilder();
		for (int i=1; i<=100000; i++){
			rnd = random.nextInt(10)+1;
			employees.append(Integer.toString(i)+"\t");
			employees.append("name"+Integer.toString(i)+"\t");
			if(i<=10000){
				employees.append("manager"+Integer.toString(rnd)+"\t");
				employees.append("techdept"+Integer.toString(rnd)+"\t");
			}else{
				employees.append("manager"+Integer.toString(i-9990)+"\t");
				employees.append("dept"+Integer.toString(i-10000)+"\t");
			}			
			employees.append(Integer.toString(i)+"\t");
			employees.append(Integer.toString(i)+"\n");
		}
		return employees.toString();
	}
	
	private static String createStudents(){
		StringBuilder students = new StringBuilder();
		for (int i=1; i<=100000; i++){
			students.append(Integer.toString(i)+"\t");
			students.append("name"+Integer.toString(i)+"\t");
			students.append("course"+Integer.toString(i)+"\t");
			students.append(Integer.toString(i)+"\n");
		}
		return students.toString();
	}
	
	private static String createTechDepts(){
		StringBuilder techDepts = new StringBuilder();
		for (int i=1; i<=10; i++){
			techDepts.append("techdept"+Integer.toString(i)+"\t");
			techDepts.append("manager"+Integer.toString(i)+"\t");
			techDepts.append("loacation"+Integer.toString(i)+"\t");
			techDepts.append(Integer.toString(i)+"\n");
		}
		return techDepts.toString();
	}
	
	private static void createTsvFile(String employees,String filename) throws FileNotFoundException{
		PrintWriter out = new PrintWriter(filename);
		out.print(employees);
		out.close();
	}
}
