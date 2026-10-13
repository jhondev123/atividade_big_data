package netflix;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Soma parcial das palavras ainda no mapper, para trafegar menos dados no shuffle. */
public class SomaCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable soma = new IntWritable();

    @Override
    protected void reduce(Text chave, Iterable<IntWritable> valores, Context context)
            throws IOException, InterruptedException {
        // Titulos passam direto: o dataset pode ter titulos repetidos e eles nao podem ser somados.
        if (chave.toString().startsWith(DescricaoMapper.PREFIXO_TITULO)) {
            for (IntWritable valor : valores) {
                context.write(chave, valor);
            }
            return;
        }

        int total = 0;
        for (IntWritable valor : valores) {
            total += valor.get();
        }
        soma.set(total);
        context.write(chave, soma);
    }
}
