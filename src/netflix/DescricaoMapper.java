package netflix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Para cada registro do csv transforma:
 *   Palavra da descrição em: (palavra, 1)
 *   Uma vez por título ("#TITULO#Titulo (Tipo)", qtdPalavras)
 */
public class DescricaoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /** Palavras normalizadas so tem [a-z0-9], entao nunca colidem com este prefixo. */
    static final String PREFIXO_TITULO = "#TITULO#";

    enum Contadores { REGISTROS, SEM_DESCRICAO, REGISTROS_INVALIDOS, STOPWORDS_IGNORADAS }

    private static final IntWritable UM = new IntWritable(1);
    private final Text chave = new Text();
    private final IntWritable quantidade = new IntWritable();

    private Set<String> stopwords;
    private int colTipo, colTitulo, colDescricao, minimoCampos;

    @Override
    protected void setup(Context context) throws IOException {
        stopwords = Utils.carregarStopwords();
    }

    // para cada registro, chama o map que pega a descrição apartir da coluna descrição e conta as palavras (jhonattan, 1).
    // e também contabiliza os titulos adicionando o prefixo
    @Override
    protected void map(LongWritable posicao, Text registro, Context context)
            throws IOException, InterruptedException {
        List<String> campos = Utils.lerCampos(registro.toString());

        if (posicao.get() == 0) {
            lerCabecalho(campos);
            return;
        }

        if (campos == null || campos.size() < minimoCampos) {
            context.getCounter(Contadores.REGISTROS_INVALIDOS).increment(1);
            return;
        }

        String descricao = campos.get(colDescricao);
        if (descricao.trim().isEmpty()) {
            context.getCounter(Contadores.SEM_DESCRICAO).increment(1);
            return;
        }
        context.getCounter(Contadores.REGISTROS).increment(1);

        int palavrasNaDescricao = 0;
        for (String palavra : Utils.normalizar(descricao).split("\\s+")) {
            if (palavra.isEmpty()) {
                continue;
            }
            if (stopwords.contains(palavra)) {
                context.getCounter(Contadores.STOPWORDS_IGNORADAS).increment(1);
                continue;
            }
            chave.set(palavra);
            context.write(chave, UM);
            palavrasNaDescricao++;
        }

        String titulo = campos.get(colTitulo).replace('\n', ' ');
        chave.set(PREFIXO_TITULO + titulo + " (" + campos.get(colTipo) + ")");
        quantidade.set(palavrasNaDescricao);
        context.write(chave, quantidade);
    }

    /** Descobre a posicao das colunas pelo nome, entao a ordem delas no CSV nao importa. */
    private void lerCabecalho(List<String> campos) throws IOException {
        List<String> nomes = new ArrayList<>();
        if (campos != null) {
            for (String campo : campos) {
                nomes.add(campo.replace("\uFEFF", "").trim().toLowerCase(Locale.ROOT)); // remove BOM
            }
        }

        colTipo = nomes.indexOf("type");
        colTitulo = nomes.indexOf("title");
        colDescricao = nomes.indexOf("description");
        if (colTipo < 0 || colTitulo < 0 || colDescricao < 0) {
            throw new IOException("Cabecalho sem as colunas type, title e description: " + nomes);
        }
        minimoCampos = Math.max(colTipo, Math.max(colTitulo, colDescricao)) + 1;
    }
}
