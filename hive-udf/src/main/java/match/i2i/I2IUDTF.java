package match.i2i;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

@Description(
        name = "explode_name",
        value = "_FUNC_(col) - The parameter is a column name."
                + " The return value is two strings.",
        extended = "Example:\n"
                + " > SELECT _FUNC_(col) FROM src;"
                + " > SELECT _FUNC_(col) AS (name, surname) FROM src;"
                + " > SELECT adTable.name,adTable.surname"
                + " > FROM src LATERAL VIEW _FUNC_(col) adTable AS name, surname;"
)
public class I2IUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs)
            throws UDFArgumentException {

        if (argOIs.length != 1) {
            throw new UDFArgumentException("ExplodeStringUDTF takes exactly one argument.");
        }
        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) argOIs[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0, "ExplodeStringUDTF takes a string as a parameter.");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("item_id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("item_neighbors");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        // TODO Auto-generated method stub
        if (args[0] != null && args[0].toString().length() > 0) {
            String[] items = args[0].toString().split(",");
            if (items.length > 1) {
                for (int index = 0; index < items.length; index++) {
                    StringBuilder stringBuilder = new StringBuilder();
                    for (int j = 0; j < items.length; j++) {
                        if (index != j) {
                            stringBuilder.append(",").append(items[j]);
                        }
                    }
                    Object[] out = new Object[2];
                    out[0] = items[index];
                    out[1] = stringBuilder.substring(1).toString();
                    forward(out);
                }
            }
        }
    }

    @Override
    public void close() throws HiveException {
        // TODO Auto-generated method stub
    }

}
