package netflix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NetflixAnalysis extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: hadoop jar netflix-analysis.jar <entrada> <saida>");
            return 2;
        }

        Job job = Job.getInstance(getConf(), "Netflix - analise das descricoes");
        job.setJarByClass(NetflixAnalysis.class);

        // define qual a classe que le as linhas e transforma em um registro
        job.setInputFormatClass(CsvInputFormat.class);

        // faz o mapeamento para quando é um titulo para quando é uma descrição
        job.setMapperClass(DescricaoMapper.class);

        // faz um agrupamento das palavras que são iguais somando as mesmas para reduzir o número da palavras com a msm chave
        job.setCombinerClass(SomaCombiner.class);

        // classe que faz o processamento dos registros já mapeados e enchutos, contabilizando titulos, palavras e afins ...
        job.setReducerClass(EstatisticasReducer.class);
        // quantidades de tasks usadas para processar os dados, usamos 1 pois os dados precisam ser processados juntos para as contabilizações totais
        // caso fossem mais tasks teriamos que mudar a abordagem da contabilização dos titulos e palavras pois dentro do job não se enxergariam
        job.setNumReduceTasks(1); 

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        MultipleOutputs.addNamedOutput(job, EstatisticasReducer.SAIDA_RESUMO,
                TextOutputFormat.class, Text.class, Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new NetflixAnalysis(), args));
    }
}
