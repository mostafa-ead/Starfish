package edu.duke.starfish.visualizer.model.settings;

import javax.swing.table.AbstractTableModel;
import edu.duke.starfish.whatif.data.MapInputSpecs;
import java.util.List;

/**
 * @author Fei Dong (dongfei@cs.duke.edu)
 * 
 *         TableModel
 * 
 *         Pass data to build the model, helper for setting JTable in
 *         SpecificationView.fx
 */
public class SpecsTableModel extends AbstractTableModel {

	private static final long serialVersionUID = -7440715725583246779L;
	private boolean readOnly;
	private String[] columnNames;
	private Object[][] data;

	/*
	 * Construct, serve for the Input Spec Table
	 * 
	 * @param specs used for set the data[][]
	 * 
	 * @param isWhatif specify the whatIf, it is useful when define which grid
	 * is editable
	 */
	public SpecsTableModel(List<MapInputSpecs> specs, boolean readOnly) {
		this.readOnly = readOnly;
		// init columnNames
		columnNames = new String[4];
		columnNames[0] = "Path Index";
		columnNames[1] = "Count";
		columnNames[2] = "Size (MB)";
		columnNames[3] = "Compressed";

		// init data
		data = new Object[specs.size()][4];
		for (int i = 0; i < specs.size(); i++) {
			data[i][0] = specs.get(i).getInputIndex();
			data[i][1] = specs.get(i).getNumSplits();
			data[i][2] = specs.get(i).getSize() / (1024 * 1024);
			data[i][3] = specs.get(i).isCompressed();
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
	 * JTable uses this method to determine the default renderer/ editor for
	 * each cell. If we didn't implement this method, then the last column would
	 * contain text ("true"/"false"), rather than a check box.
	 */
	public Class<?> getColumnClass(int c) {
		return getValueAt(0, c).getClass();
	}

	/*
	 * Don't need to implement this method unless your table's editable.
	 */
	public boolean isCellEditable(int row, int col) {
		// Note that the data/cell address is constant,
		// no matter where the cell appears onscreen.
		if (readOnly)
			return false;
		return true;
	}

	/*
	 * Don't need to implement this method unless your table's data can change.
	 */
	public void setValueAt(Object value, int row, int col) {
		data[row][col] = value;
		fireTableCellUpdated(row, col);
	}
}
