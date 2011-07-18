package edu.duke.starfish.visualizer.model.settings;

import javax.swing.table.AbstractTableModel;

import edu.duke.starfish.profile.utils.Constants;

/**
 * @author Fei Dong (dongfei@cs.duke.edu)
 * 
 *         TableModel
 * 
 *         Pass data to build the model, helper for setting JTable in
 *         SpecificationView.fx
 */
public class ConfigTableModel extends AbstractTableModel {

	private static final long serialVersionUID = 4457677824945742061L;
	private boolean readOnly;
	private boolean mapOnly;
	private String[] columnNames;
	private Object[][] data;

	/*
	 * Constructor, serve for the Config Table
	 * 
	 * @param col1 used for set the data[][]
	 * 
	 * @param col2 same as above
	 * 
	 * @param readOnly whether the table is read only or not
	 */
	public ConfigTableModel(Object[] col1, Object[] col2, boolean readOnly,
			boolean mapOnly) {
		this.readOnly = readOnly;
		this.mapOnly = mapOnly;

		// init columnNames
		columnNames = new String[2];
		columnNames[0] = "Field";
		columnNames[1] = "Value";

		// init data
		data = new Object[col1.length][2];
		for (int i = 0; i < col1.length && i < col2.length; i++) {
			data[i][0] = col1[i];
			data[i][1] = col2[i];
		}
	}

	public int getColumnCount() {
		return columnNames.length;
	}

	public int getRowCount() {
		return data.length;
	}

	public String getColumnName(int col) {
		return columnNames[col];
	}

	public Object getValueAt(int row, int col) {
		return data[row][col];
	}

	/*
	 * Don't need to implement this method unless your table's editable.
	 */
	public boolean isCellEditable(int row, int col) {
		// Note that the data/cell address is constant,
		// no matter where the cell appears onscreen.
		if (readOnly)
			return false;
		
		if (mapOnly && data[row][0].equals(Constants.MR_RED_TASKS))
			return false;
		
		return (col != 0);
	}

	/*
	 * Don't need to implement this method unless your table's data can change.
	 */
	public void setValueAt(Object value, int row, int col) {

		try {
			if (data[row][col] instanceof Integer)
				data[row][col] = Integer.parseInt(value.toString());
			else if (data[row][col] instanceof Float)
				data[row][col] = Float.parseFloat(value.toString());
			else if (data[row][col] instanceof Double)
				data[row][col] = Double.parseDouble(value.toString());
			else if (data[row][col] instanceof String)
				data[row][col] = value.toString();
			else if (data[row][col] instanceof Boolean)
				data[row][col] = Boolean.parseBoolean(value.toString());
			fireTableCellUpdated(row, col);
		} catch (NumberFormatException e) {
		}
	}
}
