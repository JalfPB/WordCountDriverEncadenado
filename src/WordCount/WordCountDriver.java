package WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: jobs <in> [<in>...] <out>");
            System.exit(2);
        }

        // Primer trabajo: Contar palabras
        Job job = Job.getInstance(getConf());
        job.setJarByClass(getClass());

        // ... (configuración del primer trabajo)

        for (int i = 0; i < args.length - 1; ++i)
            FileInputFormat.addInputPath(job, new Path(args[i]));

        Path tmp = new Path(args[args.length - 1] + "-tmp");
        FileOutputFormat.setOutputPath(job, tmp);

        if (!job.waitForCompletion(true))
            return 1;

        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1] + "-output"));

        // Obtener el número total de palabras del primer trabajo
        long numPalabras = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_OUTPUT_RECORDS").getValue();
        // Segundo trabajo: Calcular porcentajes
        job = Job.getInstance(getConf());
        job.setJarByClass(getClass());

        // ... (configuración del segundo trabajo)

        FileInputFormat.addInputPath(job, tmp);
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1] + "-output"));

        job.setMapperClass(WordCountPorcentajeMapper.class);
        job.setReducerClass(WordCountPorcentajeReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Configurar el número total de palabras para el segundo trabajo
        Configuration conf = job.getConfiguration();
        conf.setLong("num_palabras", numPalabras);

        FileInputFormat.addInputPath(job, tmp);
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1] + "-output"));

        if (!job.waitForCompletion(true))
            return 2;

        // Tercer trabajo: Opcional - Agregar más trabajos aquí si es necesario

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int resultado = ToolRunner.run(new WordCountDriver(), args);
        System.exit(resultado);
    }
}
