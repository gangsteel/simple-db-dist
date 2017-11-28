package global;

import simpledb.*;

import java.util.Iterator;

/**
 * Created by aditisri on 11/27/17.
 */
public class Utils {
    public Utils(){

    }

    public static Tuple stringToTuple(TupleDesc td, String strTup){
        String[] tupArr = strTup.split(" ");
        Tuple t = new Tuple(td);
        for (int i = 0; i < td.numFields(); i++){
            if(td.getFieldType(i) == Type.INT_TYPE){
                t.setField(i, new IntField(Integer.parseInt(tupArr[i])));
            }
            else if(td.getFieldType(i) == Type.STRING_TYPE){
                t.setField(i, new StringField(tupArr[i], Type.STRING_TYPE.getLen()));

            }
        }
        return t;
    }

}
