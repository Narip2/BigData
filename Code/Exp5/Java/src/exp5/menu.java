package exp5;

import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;

public class menu {
	protected Shell shell;
	public static int shell_num;
	//movie_number is the top of the array movielist and movie_score
	public static int movie_number; 
	/**
	 * Launch the application.
	 * @param args
	 */
	public void Init() {
		movie_number = 0;
		shell_num = 0;
	}
	public static void main(String[] args) {
		try {
			menu window = new menu();
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
		
		Button btnNewButton = new Button(shell, SWT.NONE);
		btnNewButton.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				test window = new test();
				shell.dispose();
				window.open();
			}
		});
		btnNewButton.setBounds(172, 34, 93, 29);
		btnNewButton.setText("推荐算法1");
		
		Button btnNewButton_1 = new Button(shell, SWT.NONE);
		btnNewButton_1.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				eulerdistance window = new eulerdistance();
				shell.dispose();
				window.open();
			}
		});
		btnNewButton_1.setBounds(172, 112, 93, 29);
		btnNewButton_1.setText("推荐算法2");
		
		Button btnNewButton_2 = new Button(shell, SWT.NONE);
		btnNewButton_2.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseDown(MouseEvent e) {
				assess window = new assess();
				shell.dispose();
				window.open();
			}
		});
		btnNewButton_2.setBounds(172, 203, 93, 29);
		btnNewButton_2.setText("推荐算法3");

	}
}
