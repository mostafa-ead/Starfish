package edu.duke.starfish.visualizer.model;

import javax.swing.table.DefaultTableModel;

/**
 * A simple, read-only table model
 * 
 * @author hero
 */
public class ReadOnlyTableModel extends DefaultTableModel {

	private static final long serialVersionUID = 59543654844814500L;

	public ReadOnlyTableModel() {
		super();
	}

	public ReadOnlyTableModel(Object[] columnNames, int rowCount) {
		super(columnNames, rowCount);
	}

	public boolean isCellEditable(int row, int col) {
		return false;
	}

}
