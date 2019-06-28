package exp5;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.custom.StyledText;

import java.io.File;

import org.eclipse.jface.text.TextViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;

public class assess_result {

	protected Shell shell;

	public static void delete(File file) {
		if(!file.exists()) return;
		if(file.isFile() || file.list()==null) {
			file.delete();
		}else {
			File[] files = file.listFiles();
			for(File a:files) {
				delete(a);					
			}
			file.delete();
		}
		
	}
	/**
	 * Launch the application.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			assess_result window = new assess_result();
			window.open();
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
		shell.setSize(450, 300);
		shell.setText("SWT Application");
		
		TextViewer textViewer = new TextViewer(shell, SWT.BORDER);
		StyledText styledText = textViewer.getTextWidget();
		styledText.setText(assess_mapreduce.result + "");
		styledText.setBounds(101, 81, 171, 52);
		
		Button btnNewButton = new Button(shell, SWT.NONE);
		btnNewButton.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				File file = new File("/home/narip/Code/Python/Big_Data/assess_output");
				delete(file);
				assess window = new assess();
				shell.dispose();
				window.open();
			}
		});
		btnNewButton.setBounds(140, 193, 93, 29);
		btnNewButton.setText("Back");

	}
}
