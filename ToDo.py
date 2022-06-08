########################### MAIS PRIORITARIO  #####################

#ARQUITETURA - Desenhar arquitetura base dados falar com luis - MAIS TARDE
#METODOS DE ANALISE & CLASSIFICACAO + SOFISTICADOS
#IGUALMENTE AVANÇAR AS OUTRAS REDES SOCIAIS FACEBOOK + LINKEDIN (INSTAGRAM, TIKTOK, MAIS NO FUTURO)

############################# DONE ################################

#1. Traduçao para ingles de tweets para sentiment analysis para o metodo de polaridade funcionar
#2. Export files to Excel
#3. Acrescentar "bloco" + "time_block" como chaves às tabela, para se saber em relaçao a qual empresa "base" cada competitor se refere
#4. Retirar o uso da funçao de variaçoes de nomes de empresa, pois estava apenas a duplicar tweets no Excel extraido
#5. Acrescentar argumento de desde há quanto tempo queremos os tweets. Possível downside de reduzir o numero de tweets que
#   de facto sao extraidos, pois a versao atual ja nao tem argumento "since", e portanto os tweets considerados "recent" que
#   nao preenchem o requisito temporal da funcao contribuem para o limite de processamento de tweets. Usar tempos em  timezones iguais
#6. Funcao com o dicionario de linguagens disponiveis para filtro no tweepy (consulta apenas ou futura integracao no sistema)
#7. Ajuste aos tempos, until para o dia depois, pois o Tweepy o until é até ao dia anterior
#8. Integracao de timezones
#9. Na contagem removi apostrofos, caps lock
#10. Tweets feitos pela empresa em questao (filtro usado é o screen name, portanto assume-se que sao iguais)
#11. Tweets feitos pela competicao sobre a empresa (por enquanto tenho o erro unauthorized, ver melhor)
#12. contadores de tempo: API tempo de resposta por funçao, feito (segundos do inicio ao fim da funcao)

#################################### TO DO 

### 2. Tratar abreviaturas, será que o API trata? Verificar
### 3. Algoritmo Roberta para sinonimos
### 4. Influx DB - Associaçao de palavras

### gitlabs
### linkedin












