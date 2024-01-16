// WordCountPorcentajeMapper.java
package WordCount;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountPorcentajeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final DoubleWritable porcentaje = new DoubleWritable();
    private final Text palabra = new Text();

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // Dividir la línea en índice y contenido
        String[] parts = value.toString().split("\t", 2);

        if (parts.length == 2) {
            long numPalabras = context.getConfiguration().getLong("num_palabras", 1);

            // Dividir el contenido en palabras
            String[] palabras = parts[1].trim().split("\\s+");

            for (String palabraActual : palabras) {
                // Calcular el porcentaje por palabra
                double porcentajePalabra = (double) 1 / numPalabras * 100.0; // Cada palabra tiene un conteo de 1
                porcentaje.set(porcentajePalabra);

                // Emitir la palabra y el porcentaje
                palabra.set(palabraActual);
                context.write(palabra, porcentaje);
            }
        }
    }
}
