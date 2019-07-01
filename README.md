Nasa Kennnedy
===========

Perguntas primeira parte:
---------------

Qual o objetivo do comando cache​ ​em Spark?
---------------

Ensinar o RDD que apos a excução de um comando ele deve guardar o conteúdo lido em memoria;

```
Links:
https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-rdd-caching.html
https://spark.rstudio.com/guides/caching/
https://stackoverflow.com/questions/28981359/why-do-we-need-to-call-cache-or-persist-on-a-rdd
```

O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?
---------------

Porque o Spark faz o processamento em memoria e MapReduce usa dico e esse processo é infinitamente mais demorado porisao I/O. na documentação e artigos sobre Spark afirmam ser 100 vezes mais rapido hadoop

Qual é a função do SparkContext​?
---------------

Ponto de entrada principal para a funcionalidade Spark. Um SparkContext representa a conexão a um cluster do Spark e pode ser usado para criar RDDs, acumuladores e variáveis ​​de difusão nesse cluster.

```
Links:
https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/SparkContext.html
https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-SparkContext.html
```

Explique com suas palavras o que é Resilient​ ​Distributed​ ​Datasets​ (RDD).
---------------

RDD é a mecanica de distribuir os dados pelos nós do spark de uma maneira segura por ser resiliente tendo mais de uma copia de um bloco de dados em outros nós(caso um nó falhe o mesmo bloco está garantindo em outro) e assim garantindo q todo a transformação/processamento seja concluido.

GroupByKey​ ​é menos eficiente que reduceByKey​ ​em grandes dataset. Por quê?
---------------

GroupByKey​ espalha os dados nas partições para agrupar no final enquanto o reduceByKey agrupa os dados já no final do processo;

https://www.linkedin.com/pulse/groupbykey-vs-reducebykey-neeraj-sen/


Explique o que o código Scala abaixo faz.
---------------
```
val textFile = sc.textFile("hdfs://...")                // Sinaliza qual dataset ira carregar
val counts = textFile.flatMap(line => line.split(" "))  // splita as palavras das linas em arrays e consolidando todos em uma lista
.map(word => (word, 1))                                 // adiciona o peso 1 para cada palavra
.reduceByKey(_ + _)                                     // por ultimo agrupa todas as palavras iguais trazendo o total
counts.saveAsTextFile("hdfs://...")                     // salvando em um novo Datasets
```

Requirements
-------------------

 * Java => 8.0
 * Maven3
 * docker do spark
```
sudo docker run -ti -p 4040:4040 epahomov/docker-spark /spark/bin/spark-shell
```


Perguntas Segunda parte:
---------------

Notas:
- tentei baixar o segundo arquivo, mas não estava disponivel então trabalhei com o arquivo "Jul 01 to Jul 31, ASCII format, 20.7 MB gzip compressed"
- Usei Java 8 com spark-core_2.12
- um docker spark standalone

Respostas:
---------------

- Número de hosts únicos: 5697;

- O total de erros 404: 10845;

- Os 5 URLs que mais causaram erro 404:
  - hoohoo.ncsa.uiuc.edu:251 errors
  - jbiagioni.npt.nuwc.navy.mil:131 errors
  - piweba3y.prodigy.com:110 errors
  - piweba1y.prodigy.com:92 errors
  - phaelon.ksc.nasa.gov:64 errors

 - O total de erros 404 por dia:  
    - date: 01/Jul/1995 total 404: 316
    - date: 02/Jul/1995 total 404: 291
    - date: 03/Jul/1995 total 404: 474
    - date: 04/Jul/1995 total 404: 359
    - date: 05/Jul/1995 total 404: 497
    - date: 06/Jul/1995 total 404: 640
    - date: 07/Jul/1995 total 404: 570
    - date: 08/Jul/1995 total 404: 302
    - date: 09/Jul/1995 total 404: 348
    - date: 10/Jul/1995 total 404: 398
    - date: 11/Jul/1995 total 404: 471
    - date: 12/Jul/1995 total 404: 471
    - date: 13/Jul/1995 total 404: 532
    - date: 14/Jul/1995 total 404: 413
    - date: 15/Jul/1995 total 404: 254
    - date: 16/Jul/1995 total 404: 257
    - date: 17/Jul/1995 total 404: 406
    - date: 18/Jul/1995 total 404: 465
    - date: 19/Jul/1995 total 404: 639
    - date: 20/Jul/1995 total 404: 428
    - date: 21/Jul/1995 total 404: 334
    - date: 22/Jul/1995 total 404: 192
    - date: 23/Jul/1995 total 404: 233
    - date: 24/Jul/1995 total 404: 328
    - date: 25/Jul/1995 total 404: 461
    - date: 26/Jul/1995 total 404: 336
    - date: 27/Jul/1995 total 404: 336
    - date: 28/Jul/1995 total 404: 94

 - O total de bytes: 38692395676
