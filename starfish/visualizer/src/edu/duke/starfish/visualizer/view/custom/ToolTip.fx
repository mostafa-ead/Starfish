package edu.duke.starfish.visualizer.view.custom;

import javafx.scene.CustomNode;
import javafx.scene.Node;
import javafx.scene.Group;
import javafx.scene.shape.Rectangle;
import javafx.scene.text.Text;
import javafx.scene.text.Font;
import javafx.scene.paint.Color;
import javafx.util.Math;


/**
 * A ToolTip object used to display a small descriptive text (usually for mouse hovers)
 * 
 * @author hero
*/
public class ToolTip extends CustomNode {
    
    public var textWrappingWidth : Number = 220;
    var text : Text;
    var rect : Rectangle;
    
    // Create the tool tip
    public override function create(): Node {
        
    	 rect = Rectangle {
				fill: Color.IVORY
				arcHeight: 15
				arcWidth: 15
				stroke: Color.SILVER
				strokeWidth: 1
				opacity: 0.95
				visible: false;
    	 }
    	 
    	 text = Text {
    	     font: Font {size : 12}
    	     content: ""
    	     wrappingWidth: textWrappingWidth
    	     visible: false;
    	 }
        	        	 
		return Group { content : [ rect, text ] };
    }
    
    // Show the tool tip
    public function show(content: String, 
    					 x: Number, y: Number, 
    					 transX: Number, transY: Number,
    					 textWidth: Number, textHeight: Number): Void {
        
	    // Set the properties of the text field
	    text.content = content;
	    text.x = x + 10;
	    text.y = y + 20;
	    text.translateX = transX;
	    text.translateY = transY;
	    
	    // NOTE: When we migrate to JavaFX 1.3, replace
	    // textWidth/textHeight with 'text.layoutBounds.width/height'
	    	    
	    // Set the properties of the rectangle
	    rect.x = x;
	    rect.y = y;
	    rect.width = textWidth + 20;
	    rect.height = textHeight + 20;
	    rect.translateX = transX;
	    rect.translateY = transY;
	    			    
	    if (rect.x + rect.width > scene.width - 10) {
	        rect.x = rect.x - rect.width;
	        text.x = text.x - rect.width;
	    }
	    
	    // Make the toolTip visible
        text.visible = true;
        rect.visible = true;
    }

	// Hide the tool tip
    public function hide(): Void {
        text.visible = false;
        rect.visible = false;
    }
    
}

