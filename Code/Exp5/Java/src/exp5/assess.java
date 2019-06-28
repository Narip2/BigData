package exp5;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.custom.StyledText;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.eclipse.jface.text.TextViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Spinner;

import com.csvreader.CsvReader;

import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;

public class assess {
	public static int userid;
	public static int[] movielist;
	public static Double[] movie_score;
	protected Shell shell;

	/**
	 * Launch the application.
	 * @param args
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public void Init() {
		menu.shell_num = 1;
		userid = 0;
		movielist = new int[10];
		movie_score = new Double[10];
	}
	public void Initialize() throws NumberFormatException, IOException {
		int count = 0;
		String filepath = "/home/narip/Code/Python/Big_Data/ml-latest-small/ratings.csv";
		CsvReader csvreader = new CsvReader(filepath);
		csvreader.readHeaders();
		while(csvreader.readRecord()) {
			if(Integer.parseInt(csvreader.get("userId")) == userid) {
				if(count < 10) {
					movielist[count] = Integer.parseInt(csvreader.get("movieId"));
					movie_score[count] = Double.parseDouble(csvreader.get("rating")); 
					count++;	
				} else { return; }
			}
		}
	}
	public void exec_mapreduce() {
		String[] input1 = {"/home/narip/Code/Python/Big_Data/mapin.csv","/home/narip/Code/Python/Big_Data/assess_output/mapreduce"};
		String[] input2 = {"/home/narip/Code/Python/Big_Data/mapin.csv","/home/narip/Code/Python/Big_Data/assess_output/Inverse_index"};
		String[] input3 = {"/home/narip/Code/Python/Big_Data/assess_output/Inverse_index/part-r-00000","/home/narip/Code/Python/Big_Data/mapin.csv","/home/narip/Code/Python/Big_Data/assess_output/assess_result"};
		mapreduce.main(input1);
		Inverse_index.main(input2);
		assess_mapreduce.main(input3);
	}
	public static void main(String[] args) {
		try {
			assess window = new assess();
			window.open();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Open the window.
	 */
	public void open() {
		Init();
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
		shell.setSize(450, 300);
		shell.setText("SWT Application");
		
		TextViewer textViewer = new TextViewer(shell, SWT.BORDER);
		StyledText styledText = textViewer.getTextWidget();
		styledText.setText("Choose an userid:");
		styledText.setEditable(false);
		styledText.setBounds(25, 130, 124, 42);
		
		Spinner spinner = new Spinner(shell, SWT.BORDER);
		spinner.setMinimum(1);
		spinner.setMaximum(610);
		spinner.setBounds(155, 130, 110, 42);
		
		Button btnNewButton = new Button(shell, SWT.NONE);
		btnNewButton.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				userid = Integer.parseInt(spinner.getText());
				try {
					Initialize();
				} catch (NumberFormatException | IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				exec_mapreduce();
				assess_result window = new assess_result();
				shell.dispose();
				window.open();
			}
		});
		btnNewButton.setBounds(300, 140, 93, 29);
		btnNewButton.setText("开始评估");

	}
}
