# Key Ingestion Concepts (Principais Conceitos de Ingestão):

## Referencias: 
- [Optimizing Apache Spark on Databricks](https://customer-academy.databricks.com//lms/index.php?r=course/deeplink&course_id=122&hash=6befa750504994ad9157bbde023a3740fbe13562&generated_by=92982)
- [Spark UI Simulator](https://www.databricks.training/spark-ui-simulator/)

## Qual otimizacao é aplicavel a quase todos os trabalhos do spark?
- Todo conjunto de dados é afetado por distorcoes que fazem com que algumas % das tarefas demorem mais do que outras?
- Todos sao afetados por problemas de memoria, como erros OOM e derramamento (Spill) durante as trocas?
- Todo desenvolvedor trabalha com jobs de streaming estruturado?
- Todo trabalho "incorretamente" emprega cache?
- Qual é a única coisa que cada trabalho do Spark faz?
    - A unica otimizacao aplicavel a quase todos os trabalhos do Spark é reduzir a quantidade de tempo que o Spark gasta lendo dados. Com isso, o planejamento de como os dados sao ingeridos geralmente mitiga muitos dos outros problemas.

## 1. Ingestion Basics (Noções básicas de processamento):

### 1.1 Entenda como o Apache Spark ingere dados pode:
- Nos ajudar a entender o que está acontecendo, por tras;
- Tornando o debug e o ajuste mais fáceis;

### 1.2 Particao Spark:
- O primeiro grande conceito a esclarecer é o da particao spark. Isso NÃO é o mesmo que particionamento de disco (às vezes chamado de particionamento Hive). Embora ambos representem "particionamento" no sentido mais verdadeiro da palavra.
- Um é um particionamento de daods em repouso (Disco);
- O outro é o particionamento de dados em transito (RAM);
- E não há uma correlacao direta entre os dois;
- No entanto, o proprietario pode afetar o outro de maneiras inesperadas;

### 1.3 Ingestion - From Dist to Spark:
- O tamanho inicial da partição é regido por quatro fatores principais:
1. O número de núcleos em seu cluster (ou melhor, é o paralelismo padrão);
2. O tamanho estimado do conjunto de dados;
3. O valor de configuracao no "spark.sql.files.maxPartitionBytes";
    - Por padrao o valor é 128 MB;
    - Controla o tamanho máximo de uma particao;
4. O valor de configuracao no "spark.sql.files.openCostInBytes";
    - Por padrao o valor é 4 MB;
    - Os MBs "extras" de sobrecarga preenchidos para o tamanho de cada arquivo;

### 1.4 Ingestion - The Algorithm:
- Experimento 1241;
- Note a funcao "predictNumPartitions(files)" no passo "A-2";
- Compare o passo "C" ao passo "I" e note:
    - O tempo total de execucao de cada comando;
    - O numero total de tasks/partitions;
    - O numero previsto de particoes;
    - No "Spark UI", em "Summary Metrics", o "Input Size/Records";
- Resumo: O melhor caso foi o "Step E". No "Step G", houve uma diferenca entre a quantidade de tasks atuais e previstas;
- Há um aumento de desempenho marginal à medida que maxPartitionBytes aumenta:
    - Presumivelmente de uma redução em IO e tarefas mais eficientes;
- Um ganho de desempenho linear não visto como maxPartitionBytes é aumentado:
    - Possivelmente devido a mais pressão de memória e GC extra;
    - Mas o ganho de desempenho parece estabilizar;
- Existe um limite no qual não podemos forçar partições maiores durante a ingestão, mas você provavelmente não quer ir tão longe de qualquer maneira;
    * AVISO: Os ganhos de desempenho vistos aqui não são nada comparados ao aumento do paralelismo aumentando o número de núcleos;
- Por que queremos ajustar o maxPartitionBytes?
    - Voce tem um trabalho perfeito e nao ha mais nada para ajustar;
    - Para evitar repartition/coalesce em particoes menores e maiores;
    - Para evitar a enésima iteracao em um job:
    - No exeperimento 1241, o passo L mostra um exemplo como alguem pode ajustar automaticamente o maxPartitionBytes;
- E quanto a spark.sql.files.openCostInBytes?
    - Os 4 MB padrão deste valor são adicionados ao tamanho de cada arquivo para estimativa;
    - O conselho geral dos docs é superestimar esse valor;
    - ** O custo estimado para abrir um arquivo, medido pelo número de bytes, pode ser verificado simultaneamente. Isso é usado ao colocar vários arquivos em uma partição. É melhor superestimar, pois as partições com arquivos pequenos serão mais rápidas que as partições com arquivos maiores (que são agendadas primeiro)... 
- Mas quando você está lidando com os problemas clássicos de "arquivos minúsculos"...
    - Seus arquivos de 1 MB são estimados em 5 MB (por exemplo);
    - Lembre-se, esses arquivos devem ter entre 128 MB e 1024 MB;
    - Com um arquivo de 1 MB preenchido para 5 MB, suas partições Spark são 5x maiores;
    - Quanto menores os arquivos ingeridos, pior se torna o problema;
            
## 2. Predicate Pushdowns

### 2.1 Introduction to Predicate Pushdowns
- Reduzir a ingestao de dados é uma das melhores estrategias para mitigar problemas de desempenho no Apache Spark;
- O "predicate pushdown" é a base para a maioria das estrategias de reducao;
- Um "predicate" é simplesmente uma condicao aplicada aos registros lidos por um trabalho do Spark;
- Mais especificamente, está empregando esquemas discretos, transformacoes filter(), where(), select() e drop() que sao entao enviados para o armazenamento de dados subjacentes;
- Como é aplicado o "predicate pushdown"?
    - Como é 100% dependente da fonte de daos subjacentes;
    - Para um banco de dados relacional como o PostgreSQL, isso envolve uma instrucao SELECT e uma condicao WHERE;
    - Neste cenario, apenas registros e colunas qualificados sao retornados ao Apache Spark pelo servidor PostgreSQL;
- Quando o "predicate pushdown" é aplicado?
    - É aplicado o mais cedo possivel;
    - Se aplicado cedo o suficiente (por exemplo, como a primeira transformação), os    registros podem ser excluídos e nunca puxados para o executor;
    - Se aplicado tardiamente, todos os registros podem ser puxados para o executor, após o que os registros não correspondentes são descartados;
    - Um dos "bugs" mais comuns resulta da introdução de transformações entre a leitura inicial e a transformação de filtragem;

### 2.2 Predicate Pushdown with PostgreSQL
- Experimento 8342
- Compare o passo B e C que consultam uma tabela com 1 milhao de registros;
- Observe a diferença de código de uma linha entre cada comando
- Observe a duração da execução de ambos os comandos.
- Na interface do usuário do Spark, detalhes da consulta:
    - Observe quantos registros foram retornados pelo Scan JDBNRelation
    - No Plano Físico, observe os valores passados para o PushedFilters.
    
### 2.3 Crippling the Predicate (Paralisando o predicado)
- Um problema comum de desempenho é a paralisação do predicado;
- Isso geralmente acontece ao refatorar o código existente, onde o contexto completo da consulta do Spark pode ser perdido;
- Em quase todos os casos, é o resultado ou a introdução de operações entre a leitura e o filtro com efeitos colaterais indesejados;
- Eles podem ser reconhecidos antes e depois da alteração do código, mas uma revisão da consulta deve se tornar uma prática padrão;
- Experimento 8342
    - Compare os passos D, B e C;
    - Em termos de desempenho, com qual etapa B vs C a etapa D mais se parece? B
    - O que mudou no código? B para C filtro. C para D cache;
    - O que mudou nos Detalhes da Consulta? 
    - O que mudou no Plano Físico? Mais etapas para o passo D;

### 2.4 Columnar Reads with Parquet
- Os filtros não são o único "predicado" que pode ser pressionado;
- A seleção da coluna também pode ser empurrada para baixo:
    - Com um banco de dados como o PostgreSql, isso é feito com uma instrução SELECT;
    - Para arquivos, exigimos um formato de arquivo colunar;
- O que constitui um "Formato de arquivo colunar?"
    - Definição do Livro Didático: Os dados são armazenados por coluna, não por linha;
    - Os exemplos incluem Delta, Parquet e ORC;
- Comparado aos formatos de arquivo baseado em linha que armazenam dados por linha:
    - Os exemplos incluem CSV, TSV, JSON e AVRO;
- Ao ler apenas colunas específicas, o Spark pode reduzir o IO geral;
- Considere o esquema do exemplo anterior:
    - nome: STRING, cor: STRING, cidade: STRING, idade: INTEGER;
- E a seguinte Consulta SQL:
    - SELECIONE nome, idade DE qualquer coisa;
- Apenas as colunas de qualificação (nome e idade) são retornadas;
- Compare isso com JSON e CSV em que toda a linha deve ser lida no executor antes que as colunas não utilizadas possam ser descartadas;
- Experimento 4112;
- Compare os passos B, C e D;    
    - Note o tempo de duracao de execucao de cada comando;
    - Note a diferenca de codigo de cada comando;
    - No Spark UI, no Query Details, note:
    - O "Scan parquet" bloqueia, "scan time total";
    - O "Scan parquet" bloqueia, "filesystem read data size";
    - O "WholeStageCodegen(1)" bloqueia, "rows output";
- O que é diferente em cada um das consultas do "Physical Plan"?
    - Os passos C e D sao identicos, e mais performaticos que o passo B

## 3. Disk Partitioning

### 3.1 Introduction to Disk Partitioning
- Nao confundir com uma particao spark que se aplica a particoes de dados na memoria;
- O particionamento de disco se aplica a particoes de dados em disco;
- O particionamento de disco usa uma estrutura de diretorio especial para agregar registros semelhantes;
- Como as partições de disco se relacionam com as colunas:
    - O arquivo parcial não retém necessariamente colunas particionadas;
    - O spark pode enviar um predicado para o scanner de arquivo;
    - Somente os diretorios que correspondem ao predicado sao lidos;
    - O Spark infere tipos e nome de colunas da estrutura de diretorios, reduzindo ainda mais o numero de bytes lidos do disco;
    - Funciona para Delta, Parquet, ORC, CSV, JSON e muitos mais;
    - Consulte a descoberta de particao para obter mais informacoes e a definicao de configuracao "spark.sql.sources.partitionColumnTypeInferece.enabled";

### 3.2 Predicate Pushdown with Disk Partitioning
- Experimento 2934;
- Compare os passos B e C onde a data é filtrada pela "city_id";
- No o total de tempo de execucao de cada comando;
- No SparkUi, Query Details:
    - Note o numero de arquivos lidos;
    - Note o tamanho total de dados lidos do filesystem;
    - Note o tempo de leitura do filesystem;
    - A execucao no passo C é bem mais performatica pelo filtro na particao, há filtro no plano de execucao;
- ATENCAO
    - O excesso de partitionamento pode levar a arquivos minusculos;
    - Considere um arquivo partical de 1024 MB agora particionado por hora;
    - Isso produzirá 24 arquivos, cada um com aproximadamente 42 MB (1024 / 24);
    - O exceso de partionamento pode resultar em varredura excessiva de diretorios, criando uma estrutura altamente aninhada (lembre-se do experimento 8973);
    - Lembre-se Anuncie as colunas particionadas para que elas sejam utilizadas;
    - Um dos erros mais comuns que os consumidores cometem é criar seus proprios indices em vez de usar os indices otimizados fornecidos;
    - RUIM: someDF.filter(year($"trx_date") === 1975); 
    - BOM: someDF.filter($"p_trx_year") === 1975);

## 4. Z-Ordening

### 4.1 Introduction to Z-Ordering
- Reduz drasticamente o tempo de varredura para consultas altamente seletivas;
- Suporta operadores de comparação binária como StartsWith, Like, In <list>, bem como operações AND, OR e NOT e outros;
- O particionamento de disco visa grandes segmentos de dados (por exemplo, particionado por ano);
- Z-Ordering é ideal para consultas do tipo agulha no palheiro (needle-in-the-haystack);
- Z-Ordenar uma tabela é tão simples quanto adicionar ZORDER BY para OPTIMIZE:
    - "OPTIMIZE tableA ZORDER BY (colunas1, colunas2)";
- Nota: Este recurso requer Delta em Databricks;   
- Exemplo:
    - Considere o Dataset com duas colunas, X e Y tendo 64 combinacoes de 0,0 até 7,7:

    ![...](https://github.com/Flaviohnb/Databricks/blob/main/img_ref/zOrder_example1.png?raw=true)

    - Quando organizado em disco, cada arquivo parcial de parquet contém quatro registros cada. 
    - A classificação "linear" por N colunas favorecerá fortemente a primeira coluna especificada (neste caso X, e não Y);
    - "Z-Order", os pontos atribuídos ao mesmo arquivo também estão próximos entre si ao longo de cada uma das N dimensões;
    - O salto de dados ajuda a identificar quais arquivos de peças podem e não podem ser ignorados. Considere esta consulta: "SELECT * FROM pontos WHERE x = 2 OR y = 3";

    ![...](https://github.com/Flaviohnb/Databricks/blob/main/img_ref/zOrder_example3.png?raw=true)

    - Linear: 9 arquivos verificados no total, 21 falsos positivos;
    - Z-order: 7 arquivos verificados no total, 13 falsos positivos;
    ![...](https://github.com/Flaviohnb/Databricks/blob/main/img_ref/zOrder_example4.png?raw=true)

### 4.2 Predicate Pushdown with Z-Ordered Data
- Experimento 2934. Observe o passo D. 
    - Note o tempo total de execucao para cada comando.
    - No Spark Ui, Query Details:
    - Note o numero de arquivos lidos:
        - B = 100; C = 1; D =33;
    - Note o tamanho total de dados do filesystem lido:
        - B = 102 GB; C = 285 MB; D = 10.3 GB;
    - Note o tempo total de leitura do filesystem:
        - B = ~7 min; ~10 seg; ~4 min;
    - Compare o passo B e C, a diferenca entre "Physical Plan".
- Casos de uso:
    - No último exemplo, usamos um índice ordenado em Z para filtrar por city_id (com baixa cardinalidade);
    - Mas a ordem Z é mais eficaz com índices que apresentam alta cardinalidade;
    - Isso seria equivalente a pesquisar um banco de dados relacional por chave primária ou outro id único;

### 4.3 Z-Ordered, Haystack Query in Action
- E se quiséssemos encontrar um registro de um conjunto de dados Terabyte?
- Experimento 1337 que consulta um conjunto de dados de 1 Terabyte para um único ID?
    - Contraste Step B (não otimizado) para Step C (indexado);
    - Anote o tempo total de execução de cada comando:
        - B = 16.93 min, C = 2.52 min;
    - Anote o número de arquivos lidos:
        - B = 1024, C = 360;  
    - Observe o tamanho total dos dados de leitura do sistema de arquivos:
        - B = 1016.9 GB, C = 117 GB;
    - Observe o tempo de leitura do sistema de arquivos:
        - B = 7.8 h, C = 1.1 h;
- Para mas informacoes Z-Order Clustering:
    - [Z Ordering multi dimensional clustering](docs.databricks.com/delta/optimizations/file-mgmt.html#z-ordering-multi-dimensional-clustering)
    - [Z Order by columns](docs.databricks.com/delta/quick-start.html#zorder-by-columns)
    - [Processing petabytes of data with databricks delta](databricks.com/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html)

## 5. Bucketing

## 5.1 Introduction to Bucketing
- 