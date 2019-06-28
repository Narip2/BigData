package exp5;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

import com.csvreader.CsvReader;
import org.eclipse.swt.widgets.Button;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;

public class test {
	//total Movie numbers
	final int totalmovies = 9742;
	//list to hold the movies are chosen
	static int recommend_number = 10;
	static int[] movielist = new int[recommend_number];
	String current_moviename = "";
	protected Shell shell;
	//Generate random_movies
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
	
	public void exe_mapreduce() {
		String[] input1 = {"/home/narip/Code/Python/Big_Data/mapin.csv","/home/narip/Code/Python/Big_Data/Output/mostcommon_output"};
		String[] input2 = {"/home/narip/Code/Python/Big_Data/mapin.csv","/home/narip/Code/Python/Big_Data/Output/sort2_output"};
		most_common.main(input1);
		sort2.main(input2);
	}
	
	public void button_things() throws IOException {
		recommend.loadpath = "/home/narip/Code/Python/Big_Data/Output/sort2_output/part-r-00000";
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
			test window = new test();
			window.open();
			String filepath = "/home/narip/Code/Python/Big_Data/ml-latest-small/movies.csv";
			CsvReader csvreader = new CsvReader(filepath);
			csvreader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Open the window.
	 */
	public void open() {
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
		shell = new Shell();
		shell.setSize(595, 341);
		shell.setText("选择10个喜欢的电影");
		int[] movieid = new int[8];
		Button btnNewButton = new Button(shell, SWT.NONE);
		btnNewButton.setBounds(0, 28, 280, 29);
		movieid[0] = random_movie();
		btnNewButton.setText(current_moviename);
		btnNewButton.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				System.out.println(movieid[0]);
				movielist[menu.movie_number++] = movieid[0];
				if(menu.movie_number == recommend_number) {
					exe_mapreduce();
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
		btnNewButton_1.setBounds(0, 82, 280, 29);
		movieid[1] = random_movie();
		btnNewButton_1.setText(current_moviename);
		btnNewButton_1.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				System.out.println(movieid[1]);
				movielist[menu.movie_number++] = movieid[1];
				if(menu.movie_number == recommend_number) {
					exe_mapreduce();
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
		btnNewButton_2.setBounds(0, 179, 280, 29);
		movieid[2] = random_movie();
		btnNewButton_2.setText(current_moviename);
		btnNewButton_2.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				System.out.println(movieid[2]);
				movielist[menu.movie_number++] = movieid[2];
				if(menu.movie_number == recommend_number) {
					exe_mapreduce();
					try {
						button_things();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
		});
		
		Button btnNewButton_3 = new Button(shell, SWT.NONE);
		btnNewButton_3.setBounds(298, 82, 291, 29);
		movieid[3] = random_movie();
		btnNewButton_3.setText(current_moviename);
		btnNewButton_3.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				System.out.println(movieid[3]);
				movielist[menu.movie_number++] = movieid[3];
				if(menu.movie_number == recommend_number) {
					exe_mapreduce();
					try {
						button_things();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
		});
		
		Button btnNewButton_4 = new Button(shell, SWT.NONE);
		btnNewButton_4.setBounds(0, 133, 280, 29);
		movieid[4] = random_movie();
		btnNewButton_4.setText(current_moviename);
		btnNewButton_4.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				System.out.println(movieid[4]);
				movielist[menu.movie_number++] = movieid[4];
				if(menu.movie_number == recommend_number) {
					exe_mapreduce();
					try {
						button_things();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
		});
		
		Button btnNewButton_5 = new Button(shell, SWT.NONE);
		btnNewButton_5.setBounds(298, 28, 291, 29);
		movieid[5] = random_movie();
		btnNewButton_5.setText(current_moviename);
		btnNewButton_5.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				System.out.println(movieid[5]);
				movielist[menu.movie_number++] = movieid[5];
				if(menu.movie_number == recommend_number) {
					exe_mapreduce();
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
		movieid[6] = random_movie();
		button.setText(current_moviename);
		button.setBounds(300, 133, 289, 29);
		button.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				System.out.println(movieid[6]);
				movielist[menu.movie_number++] = movieid[6];
				if(menu.movie_number == recommend_number) {
					exe_mapreduce();
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
		movieid[7] = random_movie();
		button_1.setText(current_moviename);
		button_1.setBounds(298, 179, 291, 29);
		button_1.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				System.out.println(movieid[7]);
				movielist[menu.movie_number++] = movieid[7];
				if(menu.movie_number == recommend_number) {
					exe_mapreduce();
					try {
						button_things();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
		});
		
		Button btnNewButton_12 = new Button(shell, SWT.NONE);
		btnNewButton_12.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				shell.dispose();
				test window = new test();
				window.open();
			}
		});
		btnNewButton_12.setBounds(237, 237, 93, 29);
		btnNewButton_12.setText("换一批");
	}
}
