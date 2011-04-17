package edu.duke.starfish.visualizer.view.custom;

import javafx.scene.CustomNode;
import javafx.scene.Node;
import javafx.scene.Group;
import javafx.scene.shape.Rectangle;
import javafx.scene.text.Text;
import javafx.scene.text.Font;
import javafx.scene.control.Button;
import javafx.scene.paint.Color;
import javafx.scene.text.FontWeight;


/**
 * A simple custom node to create a bordered logical group with a title as part
 * of the border at the top
 * 
 * @author hero
*/
public class BorderGroup extends CustomNode {
    public var groupX : Number;
    public var groupY : Number;
    public var groupWidth : Number;
    public var groupHeight : Number;
    public var groupTitle : String;
    public var textWidth : Number; // Temp until migrate to JavaFX 1.3
    
    public override function create(): Node {
        
        var border = Rectangle {
			    x: bind groupX
				y: bind groupY
			    width: bind groupWidth
				height: bind groupHeight
				fill: Color.WHITE
				arcHeight: 20
				arcWidth: 20
				stroke: Color.SILVER
				strokeWidth: 2
       	 };
       	 
       	 var title = Text {
	      	     x: bind groupX + 30
        	     y: bind groupY + 5
        	     font: Font.font(Font.DEFAULT.family, FontWeight.BOLD, 14)
        	     content: groupTitle
   	   	 };
   	   	 
   	   	 // NOTE: When we migrate to JavaFX 1.3, replace width
   	   	 // and height with 'title.boundsInLocal.width/height'
   	   	 
       	 var whiteRect = Rectangle {
    		    x: bind groupX + 24
    			y: bind groupY - 8
    		    width: textWidth + 10
    			height: 17.6
				fill: Color.WHITE
    	 };
	        	 
		return Group { content : [ border, whiteRect, title ] };
    }
}

