#!/usr/bin/env bash
# Trabalho 1: compila o job, gera o .jar, roda no Hadoop (modo local) e mostra as respostas.
# Uso: ./rodar.sh   (precisa do Java e do Hadoop instalados: veja o instalar.sh)
set -euo pipefail
cd "$(dirname "$0")"

echo "[1/3] Compilando o codigo e gerando o netflix-analysis.jar..."
rm -rf build && mkdir build
javac -encoding UTF-8 -cp "$(hadoop classpath)" -d build src/netflix/*.java          # as bibliotecas do Hadoop entram no classpath
jar cfe netflix-analysis.jar netflix.NetflixAnalysis -C build . -C src stopwords.txt

echo "[2/3] Rodando o job no Hadoop (log completo em hadoop.log)..."
rm -rf saida                                                        # o Hadoop exige que a pasta de saida nao exista
if ! hadoop jar netflix-analysis.jar netflix_titles.csv saida > hadoop.log 2>&1; then
    echo "ERRO: o job falhou. Ultimas linhas do log:"; tail -n 30 hadoop.log; exit 1
fi
grep -oE 'Job job_[a-z0-9_]+ completed successfully' hadoop.log | sed 's/^/      /'

echo "[3/3] Respostas"
STOPWORDS=$(grep -o 'STOPWORDS_IGNORADAS=[0-9]*' hadoop.log | tail -n 1 | cut -d= -f2 || true)
FORA_DO_PADRAO=$(awk -F'\t' '$1 !~ /^[a-z0-9]+$/' saida/part-r-00000 | wc -l | tr -d ' ')

awk -F'\t' -v stopwords="$STOPWORDS" -v fora="$FORA_DO_PADRAO" '
    $1 == "registros_analisados"           { registros = $2 }
    $1 == "descricao_mais_longa"           { longa = longa "     " $3 " - " $2 " palavras\n" }
    $1 == "descricao_mais_curta"           { curta = curta "     " $3 " - " $2 " palavras\n" }
    $1 == "top5_mais_frequentes"           { mais  = mais  "     " $2 ". " $3 " (" $4 ")\n" }
    $1 == "top5_menos_frequentes"          { menos = menos "     " $2 ". " $3 " (" $4 ")\n" }
    $1 == "palavras_com_frequencia_minima" { freq_min = $2; qtd_min = $3 }
    $1 == "total_palavras"                 { total = $2 }
    $1 == "palavras_distintas"             { distintas = $2 }
    END {
        print  ""
        print  "=============================== RESPOSTAS ==============================="
        print  "Titulos analisados: " registros
        print  ""
        print  "1. Normalizacao (minusculas, sem pontuacao e caracteres especiais):"
        print  "     " distintas " palavras distintas, " fora " fora do padrao [a-z0-9] (lista em saida/part-r-00000)"
        printf "2. Titulo com a descricao mais longa:\n%s", longa
        printf "3. Titulo com a descricao mais curta:\n%s", curta
        printf "4. Top 5 palavras mais repetidas:\n%s", mais
        printf "5. Top 5 palavras menos repetidas:\n%s", menos
        print  "     (" qtd_min " palavras aparecem so " freq_min " vez; o empate e desfeito em ordem alfabetica)"
        print  "6. Total de palavras em todas as descricoes: " total
        print  "7. Stopwords ignoradas em todas as contagens acima: " stopwords " ocorrencias descartadas"
        print  "========================================================================="
    }' saida/resumo-r-00000
