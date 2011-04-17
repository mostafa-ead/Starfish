package edu.duke.starfish.visualizer.view;

/**
 * @author dongfei
 */
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.MouseEvent;

// Custom button created using Images
public class ImageButton extends ImageView {

    public var normalImage: Image on replace {
        if(normalImage != null) { image = normalImage; }
    }
    public var selectImage: Image;

    public override var onMouseEntered = function(e:MouseEvent) {
        if(selectImage != null) { image = selectImage; }
    }

    public override var onMouseExited = function(e:MouseEvent) {
        if(normalImage != null) { image = normalImage; }
    }
}