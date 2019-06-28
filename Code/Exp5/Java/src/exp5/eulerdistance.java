package exp5;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import exp2.Job2;

import org.eclipse.swt.widgets.Button;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
public class eulerdistance {
	public static int recommend_number = 10;
	//当前的movie name
	String current_moviename = "";
	//总共有9742部电影
	final int totalmovies = 9742;
	protected Shell shell;
	public static int[] movielist = new int[recommend_number];
	public static Double[] movie_score = new Double[recommend_number];
	//my own functions
	public int random_movie()
	{
		Random random = new Random(System.currentTimeMillis());
		String filepath = "/home/narip/Code/Python/Big_Data/ml-latest-small/movies.csv";
		int movieid = 0;
		try {
			CsvReader csvreader = new CsvReader(filepath);
			//Read headers, and let get() to know the headers
			csvreader.readHeaders();
			int randomnumber = random.nextInt(totalmovies);
			for(int i = 0; i < randomnumber + 1; i++) {
				csvreader.readRecord();
			}
			movieid = Integer.parseInt(csvreader.get("movieId"));
			current_moviename = csvreader.get("title"); 
			csvreader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return movieid;
	}
	//The function to run mapreduce 
	public void mapreduce_driver() {
		String[] input1 = {"/home/narip/Code/Python/Big_Data/mapin.csv","/home/narip/Code/Python/Big_Data/Output/mapreduce_output"};
		String[] input2 = {"/home/narip/Code/Python/Big_Data/mapin.csv","/home/narip/Code/Python/Big_Data/Output/Inverse_index_output"};
		String[] input3 = {"/home/narip/Code/Python/Big_Data/Output/Inverse_index_output/part-r-00000","/home/narip/Code/Python/Big_Data/Output/sort_output"};
//		check_mapreduce.main(input1);
		mapreduce.main(input1);
		Inverse_index.main(input2);
		sort.main(input3);
	}
	public void button_things() throws IOException {
		recommend.loadpath = "/home/narip/Code/Python/Big_Data/Output/sort_output/part-r-00000";
		recommend window = new recommend();
		shell.dispose();
		window.open();
	}
	/**
	 * Launch the application.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			eulerdistance window = new eulerdistance();
			window.open();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Open the window.
	 */
	public void open() {
		menu.shell_num = 2;
		Display display = Display.getDefault();
		createContents();
		shell.open();
		shell.layout();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
	}

	/**
	 * Create contents of the window.
	 */
	protected void createContents() {
		int[] movieid = new int[recommend_number];
		shell = new Shell();
		shell.setSize(595, 341);
		shell.setText("请为"+recommend_number+"部电影打分");
		Button btnNewButton = new Button(shell, SWT.NONE);
		btnNewButton.setBounds(0, 0, 290, 29);
		movieid[0] = random_movie();
		btnNewButton.setText(current_moviename);
		btnNewButton.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				movielist[menu.movie_number] = movieid[0];
				score window = new score();
				window.open();
				movie_score[menu.movie_number++] = score.score;
				if(menu.movie_number == recommend_number) {
					mapreduce_driver();
					try {
						button_things();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
		});
		
		Button btnNewButton_1 = new Button(shell, SWT.NONE);
		btnNewButton_1.setBounds(296, 0, 294, 29);
		movieid[1] = random_movie();
		btnNewButton_1.setText(current_moviename);
		btnNewButton_1.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				movielist[menu.movie_number] = movieid[1];
				score window = new score();
				window.open();
				movie_score[menu.movie_number++] = score.score;
				if(menu.movie_number == recommend_number) {
					mapreduce_driver();
					try {
						button_things();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
		});
		
		Button button = new Button(shell, SWT.NONE);
		movieid[2] = random_movie();
		button.setText(current_moviename);
		button.setBounds(0, 49, 290, 29);
		button.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				movielist[menu.movie_number] = movieid[2];
				score window = new score();
				window.open();
				movie_score[menu.movie_number++] = score.score;
				if(menu.movie_number == recommend_number) {
					mapreduce_driver();
					try {
						button_things();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
		});
		
		Button button_1 = new Button(shell, SWT.NONE);
		movieid[3] = random_movie();
		button_1.setText(current_moviename);
		button_1.setBounds(296, 49, 294, 29);
		button_1.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				movielist[menu.movie_number] = movieid[3];
				score window = new score();
				window.open();
				movie_score[menu.movie_number++] = score.score;
				if(menu.movie_number == recommend_number) {
					mapreduce_driver();
					try {
						button_things();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
		});
		
		Button button_2 = new Button(shell, SWT.NONE);
		movieid[4] = random_movie();
		button_2.setText(current_moviename);
		button_2.setBounds(0, 104, 290, 29);
		button_2.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				movielist[menu.movie_number] = movieid[4];
				score window = new score();
				window.open();
				movie_score[menu.movie_number++] = score.score;
				if(menu.movie_number == recommend_number) {
					mapreduce_driver();
					try {
						button_things();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
		});
		
		Button button_3 = new Button(shell, SWT.NONE);
		movieid[5] = random_movie();
		button_3.setText(current_moviename);
		button_3.setBounds(296, 104, 294, 29);
		button_3.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				movielist[menu.movie_number] = movieid[5];
				score window = new score();
				window.open();
				movie_score[menu.movie_number++] = score.score;
				if(menu.movie_number == recommend_number) {
					mapreduce_driver();
					try {
						button_things();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
		});
		
		Button button_4 = new Button(shell, SWT.NONE);
		movieid[6] = random_movie();
		button_4.setText(current_moviename);
		button_4.setBounds(0, 155, 290, 29);
		button_4.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				movielist[menu.movie_number] = movieid[6];
				score window = new score();
				window.open();
				movie_score[menu.movie_number++] = score.score;
				if(menu.movie_number == recommend_number) {
					mapreduce_driver();
					try {
						button_things();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
		});
		
		Button button_5 = new Button(shell, SWT.NONE);
		movieid[7] = random_movie();
		button_5.setText(current_moviename);
		button_5.setBounds(296, 155, 294, 29);
		button_5.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				movielist[menu.movie_number] = movieid[7];
				score window = new score();
				window.open();
				movie_score[menu.movie_number++] = score.score;
				if(menu.movie_number == recommend_number) {
					mapreduce_driver();
					try {
						button_things();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
		});
		
		Button btnNewButton_2 = new Button(shell, SWT.NONE);
		btnNewButton_2.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				eulerdistance window = new eulerdistance();
				shell.dispose();
				window.open();
			}
		});
		btnNewButton_2.setBounds(240, 259, 93, 29);
		btnNewButton_2.setText("换一批");

	}

}
