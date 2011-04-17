package edu.duke.starfish.visualizer.view.custom;

import javafx.scene.CustomNode;
import javafx.ext.swing.SwingComponent;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.event.ListSelectionEvent;
import javafx.scene.paint.Color;
import javafx.scene.shape.Rectangle;
import javafx.scene.Node;

import edu.duke.starfish.visualizer.view.custom.Job;
import edu.duke.starfish.visualizer.model.ReadOnlyTableModel;



/**
 * @author Fei Dong (dongfei@cs.duke.edu)
 * 
 * JobTable.fx
 * 
 * create job table based on JTable, invoked at Main.fx
 */
public class TableColumns {
	var content : String[];
}

public class TableCell {
	var text : String;
}

public class TableRow {
	var cells : TableCell[];
}

public class TableComponent extends SwingComponent {
	var table : JTable;
	var model : ReadOnlyTableModel;
	var selectionNum : Integer;
	var columns : TableColumns on replace{
		model = new ReadOnlyTableModel(columns.content, 0);
		table.setModel(model);
	};
	
	var rows : TableRow[] on replace oldValue[lo..hi] = newVals {
		for(index in [hi..lo step -1]){
			model.removeRow(index);
		}

		for(row in newVals){
			model.addRow(for(cell in row.cells) cell.text);
		}
	};

	override function createJComponent() {
		table = new JTable(new ReadOnlyTableModel());
		model = table.getModel() as ReadOnlyTableModel;
		table.setAutoResizeMode(JTable.AUTO_RESIZE_SUBSEQUENT_COLUMNS);

		table.getSelectionModel().addListSelectionListener(javax.swing.event.ListSelectionListener {
			override function valueChanged(e : javax.swing.event.ListSelectionEvent) {
				selectionNum = table.getSelectedRow();
			}
		});
		return new JScrollPane(table);
	}
}

public class JobTable extends CustomNode{
	public var jobs : Job[];
	public var select : Integer;
	public var width : Integer;
	public var height : Integer;
	
	public var table : JTable;

	override function create(): Node {
		var group = javafx.scene.Group{
			content: [
			TableComponent {
				width : bind width
				height : bind height
				columns : TableColumns {
					content : ["Job Name", "ExecID", "User", "Start Time", "End Time", "Status"]
				}
				rows: bind for(j in jobs) TableRow {
					cells: [
						TableCell {
							text : bind j.name
						},
						TableCell {
							text : bind j.execId
						},
						TableCell {
							text : bind j.user
						},
						TableCell {
							text : bind j.startTime
						},
						TableCell {
							text : bind j.endTime
						},
						TableCell {
							text : bind j.status
						}
					] // end of cells
				} // end of rows
				selectionNum: bind select with inverse
			} // end of TableComponent
			
			Rectangle {
				width : bind width
				height : bind height
				fill : Color.LIGHTBLUE
				opacity : 0.05
			}
			]
		} // end of Group()
		
		table = (group.content[0] as TableComponent).table;
		
		return group;
		
	} // end of create()
} // end of class