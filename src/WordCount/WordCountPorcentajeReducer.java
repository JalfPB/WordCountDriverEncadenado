// WordCountPorcentajeReducer.java
package WordCount;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountPorcentajeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable porcentajeFinal = new DoubleWritable();

    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sumaPorcentajes = 0.0;

        for (DoubleWritable porcentaje : values) {
            sumaPorcentajes += porcentaje.get();
        }

        porcentajeFinal.set(sumaPorcentajes);
        context.write(key, porcentajeFinal);
    }
}
