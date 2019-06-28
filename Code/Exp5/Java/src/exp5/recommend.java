package exp5;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Text;

import com.csvreader.CsvReader;

import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.custom.StyledText;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.eclipse.jface.text.TextViewer;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;

public class recommend {
	static String loadpath = "";
	protected Shell shell;

	/**
	 * Launch the application.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			recommend window = new recommend();
			window.open();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Open the window.
	 * @throws IOException 
	 */
	public void open() throws IOException {
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
	public String id2title(String movieid) throws IOException {
		String filepath = "/home/narip/Code/Python/Big_Data/ml-latest-small/movies.csv";
		CsvReader reader = new CsvReader(filepath);
		reader.readHeaders();
		while(reader.readRecord()) {
			if(reader.get("movieId").equals(movieid)) {
				return reader.get("title");
			}
		}
		return "wrong";
	}
	/**
	 * Create contents of the window.
	 * @throws IOException 
	 */
	protected void createContents() throws IOException {
		shell = new Shell();
		shell.setSize(450, 317);
		shell.setText("recommendation");
		
		TextViewer textViewer = new TextViewer(shell, SWT.BORDER);
		textViewer.setEditable(false);
		StyledText styledText = textViewer.getTextWidget();
		FileInputStream inputStream = new FileInputStream(loadpath);
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
		String line = "";
		StringBuilder show = new StringBuilder();
		String[] str;
		for(int i = 0; i < 10; i++) {
			line = bufferedReader.readLine();
			str = line.split("\t");
			show.append(id2title(str[1]));
			show.append("\n");
		}
		styledText.setText(show.toString());
		styledText.setBounds(10, 10, 424, 237);
		
		Button btnNewButton = new Button(shell, SWT.NONE);
		btnNewButton.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				menu window = new menu();
				shell.dispose();
				window.open();
			}
		});
		btnNewButton.setBounds(180, 259, 93, 29);
		btnNewButton.setText("Main menu");
	}
}
