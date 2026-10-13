package netflix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Roda com um unico reducer (as respostas sao globais).
 * Grava "palavra -> frequencia" em part-r-00000 e, no cleanup, as respostas em resumo-r-00000.
 */
public class EstatisticasReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    static final String SAIDA_RESUMO = "resumo";
    private static final int TOP_N = 5;

    private final Map<String, Integer> frequencias = new HashMap<>();
    private final List<String> maisLongas = new ArrayList<>();  // títulos com a maior descrição
    private final List<String> maisCurtas = new ArrayList<>();  // títulos com a menor descrição
    private int maiorQtd = Integer.MIN_VALUE;   // recorde de "mais palavras", começa no menor int possível
    private int menorQtd = Integer.MAX_VALUE;   // recorde de "menos palavras", começa no maior int possível
    private long registros = 0;                 // quantos títulos passaram
    private long totalPalavras = 0;             // todas as palavras de todas as descrições, contando repetidas

    private MultipleOutputs<Text, IntWritable> resumo;

    @Override
    protected void setup(Context context) {
        resumo = new MultipleOutputs<>(context);
    }
    // para cada registro
    @Override
    protected void reduce(Text chave, Iterable<IntWritable> valores, Context context)
            throws IOException, InterruptedException {
        String texto = chave.toString();
        // vê se é titulo
        if (texto.startsWith(DescricaoMapper.PREFIXO_TITULO)) {
            String titulo = texto.substring(DescricaoMapper.PREFIXO_TITULO.length());
            for (IntWritable valor : valores) {
                registrarTitulo(titulo, valor.get());
            }
            return;
        }

        int soma = 0;
        for (IntWritable valor : valores) {
            soma += valor.get();
        }
        frequencias.put(texto, soma);
        totalPalavras += soma;
        context.write(chave, new IntWritable(soma));
    }
    // método que verifica se o titulo tem a maior ou a menor descrição
    private void registrarTitulo(String titulo, int qtd) {
        registros++;
        if (qtd > maiorQtd) {
            maiorQtd = qtd;
            maisLongas.clear();
        }
        if (qtd == maiorQtd) {
            maisLongas.add(titulo);
        }
        if (qtd < menorQtd) {
            menorQtd = qtd;
            maisCurtas.clear();
        }
        if (qtd == menorQtd) {
            maisCurtas.add(titulo);
        }
    }

    // roda no final de fazer o reduce de todos os registros, escreve os resultados no arquivo de saída
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        escrever("registros_analisados", String.valueOf(registros));

        for (String titulo : maisLongas) {
            escrever("descricao_mais_longa", maiorQtd + "\t" + titulo);
        }
        for (String titulo : maisCurtas) {
            escrever("descricao_mais_curta", menorQtd + "\t" + titulo);
        }

        // Palavras com a mesma frequencia sao desempatadas em ordem alfabetica.
        Comparator<Map.Entry<String, Integer>> porFrequencia = Map.Entry.comparingByValue();
        Comparator<Map.Entry<String, Integer>> alfabetica = Map.Entry.comparingByKey();
        List<Map.Entry<String, Integer>> lista = new ArrayList<>(frequencias.entrySet());

        lista.sort(porFrequencia.reversed().thenComparing(alfabetica));
        escreverTop("top5_mais_frequentes", lista);

        lista.sort(porFrequencia.thenComparing(alfabetica));
        escreverTop("top5_menos_frequentes", lista);

        if (!lista.isEmpty()) {
            int frequenciaMinima = lista.get(0).getValue();
            long empatadas = lista.stream().filter(e -> e.getValue() == frequenciaMinima).count();
            escrever("palavras_com_frequencia_minima", frequenciaMinima + "\t" + empatadas);
        }

        escrever("total_palavras", String.valueOf(totalPalavras));
        escrever("palavras_distintas", String.valueOf(frequencias.size()));

        resumo.close();
    }

    private void escreverTop(String nome, List<Map.Entry<String, Integer>> lista)
            throws IOException, InterruptedException {
        for (int i = 0; i < Math.min(TOP_N, lista.size()); i++) {
            Map.Entry<String, Integer> e = lista.get(i);
            escrever(nome, (i + 1) + "\t" + e.getKey() + "\t" + e.getValue());
        }
    }

    private void escrever(String chave, String valor) throws IOException, InterruptedException {
        resumo.write(SAIDA_RESUMO, new Text(chave), new Text(valor));
    }
}
