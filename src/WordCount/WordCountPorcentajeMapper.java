package WordCount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountPorcentajeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final Text palabra = new Text();
    private final DoubleWritable porcentaje = new DoubleWritable();
    private final Map<String, Long> palabraContador = new HashMap<>();

    protected void setup(Context context) throws IOException, InterruptedException {
        // Obtener el contador de palabras del contexto
        long numPalabras = context.getCounter("WordCount", "totalPalabras").getValue();

        // Configurar el contador de palabras en la configuración para que esté disponible en todos los nodos
        context.getConfiguration().setLong("num_palabras", numPalabras);
    }

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t", 2);

        if (parts.length == 2) {
            String[] palabras = parts[1].trim().split("\\s+");

            // Calcular el número total de palabras en este documento
            long totalPalabrasDocumento = Long.parseLong(parts[0]);

            // Actualizar el contador de palabras
            context.getCounter("WordCount", "totalPalabrasDocumento").increment(totalPalabrasDocumento);

            // Calcular el porcentaje por palabra usando la frecuencia de la palabra y el número total de palabras
            for (String palabraActual : palabras) {
                // Actualizar el contador de la palabra
                palabraContador.put(palabraActual, palabraContador.getOrDefault(palabraActual, 0L) + 1);

                // Emitir la palabra y el porcentaje
                palabra.set(palabraActual);
                double porcentajePalabra = ((double) palabraContador.get(palabraActual) / totalPalabrasDocumento) * 100.0;
                porcentaje.set(porcentajePalabra);
                context.write(palabra, porcentaje);
            }
        }
    }
}





