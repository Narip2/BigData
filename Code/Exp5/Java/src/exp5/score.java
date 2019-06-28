package exp5;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Spinner;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;

public class score {
	public static double score = 0;
	protected Shell shell;

	/**
	 * 
	 * Launch the application.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			score window = new score();
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
		
		Spinner spinner = new Spinner(shell, SWT.BORDER);
		spinner.setBounds(152, 77, 110, 42);
		spinner.setMaximum(50);
		spinner.setDigits(1);
		spinner.setIncrement(5);
		
		Button btnNewButton = new Button(shell, SWT.NONE);
		btnNewButton.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				score = Double.parseDouble(spinner.getText());
				System.out.println(score);
				shell.dispose();
			}
		});
		btnNewButton.setBounds(169, 181, 93, 29);
		btnNewButton.setText("确定");
	}
}
