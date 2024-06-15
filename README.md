# lisarbjc

## Objetivo

Case engenharia de dados. Pipeline de injestão de dados de uma loja fictícia chamada aqui de Loja Simulada.

## Problema

O sócio fundador da Loja Simulada (uma loja de bicicleta) expos as seguintes necessidades:

1. Acompanhar em tempo 'real' as seguintes informações do dia atual: totais de preço, desconto e valor, em um grafico de pizza permitindo assim que ele acompanhe e até intervenha junto ao time comercial.

2. Disponibilizar os dados em uma modelagem que permita ser analisada por ferramentas de BI/OLAP.

## Considerações - Loja Simulada

1. Possui um sistema de vendas (transacional com vendasd geradas aleatoriamente);

2. Detém, de todos os seus clientes e funcionários (titulares dos dados), o concentimento para compartilhamento e/ou tratamento de seus dados com parceiros para fins de análises no contexto da Loja Simulada;

3. A schema do banco de dados foi baseado no curso **Formação Engenharia de Dados: Domine Big Data!** [Formação Engenharia de Dados: Domine Big Data!](https://www.udemy.com/course/engenheiro-de-dados) do instrutor *Fernando Amaral* [Fernando Amaral](https://www.udemy.com/course/engenheiro-de-dados/#instructor-1)

4. Além do conteúdo parcialmente aproveitado do curso citado no item 3, alguns dados foram gerados aleatoreamente como: cpf, telefone, origem racial afim de compor a experiência do case em questão atravéz das aplicações em python contidas neste projeto (pyCPFgen.py, pyCELgen.py e pyOrigemRacial.py). 
Estes podem ser encontrados em */lab/utils*

## Arquitetura da solução

A arquitetura foi pensada considerando a arquitetura Lambda.

Para o acompanhamento em tempo real (near real time para ser mais exato), o responsável pelo sistema transacional da Loja Simulada ficou com a responsabilidade de fornecer para um tópico do Apacke kafka [Apacke kafka](https://kafka.apache.org/), os dados de cada tranzação assim que esta esteja concluída. O SparkStream  [SparkStream](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) (Structured Streaming)  [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) ouve este mesmo tópico, realiza a sumarização das informações e encaminha para um segundo tópico. Uma API - desenvolvidad em NodeJs [NodeJs](https://nodejs.org/en/) - consome estes dados neste segundo tópico e, conectada via WebSocket (WS) [WebSocket](https://developer.mozilla.org/pt-BR/docs/Web/API/WebSockets_API) à um aaplicação customizada - também desenvolvida em NodeJs [NodeJs](https://nodejs.org/en/) - para dashboard, atualiza o dashboard.

Para a disponibilização dos dados em um formato adequado para análise por ferramentas apropriadas de BI/OLAP, foi considerado a arquiteura medalhão, contendo as camadas bronze, siver e gold. Fazendo a coleta dos dados no banco de dados transacional do sistema Loja Simulada periodicamente em horário apropriado - trazendo menor impacto possível para a operação da loja (transacional) - com o Apache Spark [Apache Spark](https://spark.apache.org/), e disponibilizado em uma camada bronze (somente dados novos e/ou alterados). Tembém periodicamente, os dados são coletados da camada bronze utilizando o Apache Spark [Apache Spark](https://spark.apache.org/), tratados de acordo com a LGPD (Lei Geral de Proteção de Dados) [LGPD](https://www.planalto.gov.br/ccivil_03/_ato2015-2018/2018/lei/l13709.htm) e salvos na camada silver. E não diferente dos demais, estes são coletados da camada silver pelo Apache Spark [Apache Spark](https://spark.apache.org/), tratados de acordo com uma modelagem dimensional estrela e disponibilizados na camada ouro.

Os acessos a cada camada são dados aos usuários de níveis 1 a 3, sendo bronze, silver e gold respectivamente.

O armazenamento dos dados foi feito considerando o arquivo Parquet [Parquet](https://parquet.apache.org/).

As tecnologias foram conteinerizadas individualmente com a utilização do Docker [Docker](https://www.docker.com/) - permitindo assim serem distribuídas em servidores/máquinas distintas - porém neste *case*, rodando em um servidor OnPremises.

As aplicações contam com coletores/expositores de métricas para auxiliar na observabilidade.

## Arquitetura técnica

*lab/evidencias/Arquitetura_tecnica*

### Streaming

O sistema transacional da Loja Simulada é compostO por uma aplicação em NodeJs [NodeJs](https://nodejs.org/en/) e
uma instância de banco de dados MySQL [MySQL](https://www.mysql.com/). As *vendas*, assim que efetivadas, são
encaminhadas para o Apacke kafka [Apacke kafka](https://kafka.apache.org/) no tópico vendas-deshboard-bronze (t1). O SparkStream  [SparkStream](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
coleta deste tópico, faz a sumarização e encaminha para o tópico vendas-deshboard-gold
(t2). O Merge é feito por uma API em NodeJs [NodeJs](https://nodejs.org/en/) que coleta no tópico t2 e através de uma
conexão WebSocket (WS) [WebSocket](https://developer.mozilla.org/pt-BR/docs/Web/API/WebSockets_API), encaminha para um deshboard desenvolvido também em NodeJs [NodeJs](https://nodejs.org/en/)
com o FrameWork ReactJs [ReactJs](https://react.dev/) e uma lib para gráficos (ChartJs).

### Batch

O processo batch *bronze* realiza a coleta no banco transacional da Loja Simulada,
trazendo todos os dados de todas as tabelas, analisa e considera somente os registros
novos e/ou atualizados, adicionando aos registros atual da camada bronze (praticamente
um append), alem de adicionar uma chave substitutiva (surrogate key - SK) e uma data de referencia de carga, e salva no formato Parquet [Parquet](https://parquet.apache.org/).
O processo batch *silver* realiza a leitura do arquivo Parquet [Parquet](https://parquet.apache.org/) da camada bronze, realiza a
limpeza dos dados, aplica a anonimização e salva em formato Parquet [Parquet](https://parquet.apache.org/).
O processo batch *gold* realiza a leitura do arquivo Parquet [Parquet](https://parquet.apache.org/) da camada silver, realiza a
sumarização, aplicando a modelagem dimensional estrela, considerando aadição da dimensão tempo e salva em formato
Parquet [Parquet](https://parquet.apache.org/), podendo então ser consumido por alguma ferramenta de BI/OLAP.

## Evidências

A imagem */lab/evidencias/loja-a-api* apresenta os logs da loja (venda realizada) e da api depois de no tópico t1 para então encaminhar ao dashboard.

A imagem */lab/evidencias/dashboard* apresenta o dashboard assim atualizado.

A imagem */lab/evidencias/relacional* apresenta o modelo relacional da origem.

A imagem */lab/evidencias/ingestao-base-bronze* apresenta os logs resultantes da ingestão base bronze.

A imagem */lab/evidencias/ingestao-base-silver* apresenta os logs resultantes da ingestão base silver.

A imagem */lab/evidencias/ingestao-base-gold* apresenta os logs resultantes da ingestão base gold.

A imagem */lab/evidencias/analise-bronze* apresenta a diferença entre o schema da base origem e o schema bronze, no qual são adicionados os campos *data_carga* e *surrogate_key*.

A imagem */lab/evidencias/analise-silver* apresenta a diferença no schema e a aplicação de criptografia, anonimização, considerando a LGPD (Lei Geral de Proteção de Dados) [LGPD](https://www.planalto.gov.br/ccivil_03/_ato2015-2018/2018/lei/l13709.htm).

A imagem */lab/evidencias/dimensional* apresenta o modelo dimensional gold.

## Arquivos fonte

    .
    ├── lab                                             # Pasta principal do projeto (laboratorio)
    │   ├── apps                                        # aplicações
    │   |   ├── api-dashboard                           # aplicação api-dashboard
    │   |   |   ├── services                            # serviços
    │   |   |   |   ├── kafkajs.js                      # serviço kafka
    │   |   |   |   └── wsServer.js                     # serviço webSocket
    │   |   |   ├── app.js                              # aplicação - entrypoint
    │   |   |   ├── dockerfile                          # geração da imagem
    │   |   |   ├── package-lock.json                   # package-lock
    │   |   |   └── package.json                        # package - entrypoint npm
    │   |   ├── dashboard                               # aplicação dashboard
    │   |   |   ├── public                              # arquivos publicos
    │   |   |   |   ├── favicon.ico                     # favicon
    │   |   |   |   ├── index.html                      # index
    │   |   |   |   ├── manifest.json                   # manifest
    │   |   |   |   └── robots.txt                      # robots
    │   |   |   ├── src                                 # source
    │   |   |   |   └── componentes                     # componentes
    │   |   |   |       └── FaturamentoDesconto.jsx     # componente faturamento vs desconto
    │   |   |   ├── App.css                             # stilo
    │   |   |   ├── App.jsx                             # conponente app
    │   |   |   ├── index.js                            # index
    │   |   |   ├── dockerfile                          # geração da imagem
    │   |   |   ├── package-lock.json                   # package-lock
    │   |   |   └── package.json                        # package - entrypoint npm
    │   |   ├── kafka                                   # volumes/*
    │   |   ├── loja_simulada                           # aplicação loja simulada
    │   |   |   ├── src                                 # source
    │   |   |   |   ├── cadastro.js                     # cadastro
    │   |   |   |   ├── db.js                           # db
    │   |   |   |   ├── gerador.js                      # gerador
    │   |   |   |   ├── kafka.js                        # kafka
    │   |   |   |   ├── routes.js                       # routes
    │   |   |   |   └── validar.js                      # validar
    │   |   |   ├── app.js                              # aplicação - entrypoint
    │   |   |   ├── docker-compose.yml                  # docker compose
    │   |   |   ├── dockerfile                          # dockerfile
    │   |   |   ├── package-lock.json                   # package-lock
    │   |   |   └── package.json                        # package - entrypoint npm
    │   |   ├── mysql                                   # mysql
    |   |   |   ├── data                                # volume
    |   |   |   ├── queries                             # queries
    │   |   |   |   ├── 1.CreateTable.sql               # query
    │   |   |   |   ├── 2.InsertClientes.sql            # query
    │   |   |   |   ├── 3.IsertIntoProdutos.sql         # query
    │   |   |   |   └── 4.InsertIntoVendedores.sql      # query
    │   |   |   ├── docker-compose.yml                  # docker compose
    │   |   |   └── schema.sql                          # schema
    |   |   ├── observabilidade                         # aplicações de observabilidade
    │   |   |   ├── docker-compose.yml                  # docker compose
    │   |   |   └── prometheus.yml                      # prometheus config
    |   |   ├── spark                                   # spark
    │   |   |   └── docker-compose.yml                  # docker compose
    │   |   └──── docker-compose.yml                    # docker compose
    |   ├── evidencias                                  # evidencias
    │   |   ├── analise-bronze.png                      # imagem
    │   |   ├── analise-silver.png                      # imagem
    │   |   ├── dashboard.png                           # imagem
    │   |   ├── dimensional.png                         # imagem
    │   |   ├── full.excalidraw                         # bkp excalidraw
    │   |   ├── ingestao-base-bronze.png                # imagem
    │   |   ├── ingestao-base-silver.png                # imagem
    │   |   ├── ingestao-base-gold.png                  # imagem
    │   |   ├── loja-a-api.png                          # imagem
    │   |   └── relacional.png                          # imagem
    |   ├── lake                                        # evidencias
    │   |   ├── BRONZE                                  # dados bronze
    │   |   ├── GOLD                                    # dados gold
    │   |   ├── root                                    # dados root
    │   |   ├── SILVER                                  # dados silver
    │   |   ├── user1                                   # dados user1
    │   |   ├── user2                                   # dados user2
    │   |   └── user3                                   # dados user3
    |   └── utils                                       # uteis
    │       ├── pyCELgen.py                             # dados bronze
    │       ├── pyCPFgen.py                             # dados bronze
    │       ├── pyOigemRacial.py                        # dados bronze
    │       └── pyToLow.py                              # dados bronze
    │── .gitignore                                      # gitignore
    │── build.sh                                        # bash para gerar algumas imagens docker
    │── instalar-dependencias.sh                        # bash para auxíliar com as dependências
    │── usuarios-acl.sh                                 # bash para criação de usuarios e ACL
    └── README.md                                       # README

## Instalacao 

Faça o clone do projeto na pasta home *(~/)* de um usuário com privilégios administrativos (root)
```
$ git clone https://github.com/ClovisReis21/lisarbjc.git

$ cd lisarbjc
```

### pre requisitos

Sertifique-se de que as dependências abaixo estejam instaladas

* Docker -> engine community version 26.1.4 - **instale você mesmo**
* Linux Ubuntu 24.04.4 LTS
* Java -> openjdk-11-jre-headless   *
* mysql-client-8.0  *
* python3-pip   *
* jupyter   *

**O arquivo bash** *instalar-dependencias.sh* **pode auxiliar na instalação de algumas das dependencias acima (marcadas com *)- **porem, é importante se certificar se ele fez um bom trabalho antes de seguir adiante**
```
$ sudo sh instalar-dependencias.sh
```

### build das imagens docker
Execulte o arquito build.sh para criar as imagens dos apps *api-dashboard, dashboard e loja_simulada.
```
$ sudo sh build.sh
```

### usuarios e ACL
Certifique-se de que a estrutura de pastas e arquivos esta como apresentado acima, isso irá impactar na ACL diretamente.
Estando as pastas user1, user2 e user3 de forma correta, execulte o arquivo *usuarios-acl*.
```
$ sudo sh usuarios-acl.sh
```

### prometheus
Configure o arquivo */etc/docker/daemon.json* com o ip do docker conforme abaixo
encontre o ip:
* run ip addr show docker0 to get inet ip (docker0)

Atualize ou crie o arquivo */etc/docker/daemon.json*:
```
sudo gedit /etc/docker/daemon.json
{
    "metrics-addr" : "<IP>:9323",
    "experimental" : true
}
```

Abra o arquivo prometheus.yml e configure o *targets* do job_name: *docker* para o ip interno do Docker.
```
  job_name: docker
    static_configs:
      targets: ['<IP>:9323']
```

Agora reinicie o serviço:
```
sudo run systemctl daemon-reload
sudo run systemctl restart docker
```

### iniciando o projeto
Tendo dudo dado certo até aqui, é hora de subir o projeto.
Navegue até a pasta apps
```
$ cd lab/apps
```

Execulte o comando docker compose up **atente-se entre as variações deste comando**, algumas versões do docker funcionam com o comando *docker-compose up* - confira na documentação Docker [Docker](https://www.docker.com/).
```
$ sudo docker compose up
```
Aguarde até que os containers subam...


### utilizando
abra outro terminal e navegue para a pasta lake lisarbjc/lab/lake/*root*
```
$ cd ~/lisarbjc/lab/lake/root
```
execulte o comando 'jupyter notebook' para utiliza-lo no seu navegador de preferência.
```
$ jupter notebook
```

Caso o seu navegador não abra automaticamente, copie o lind apresentado no terminal e cole na barra de endereços do navegador
```
http://localhost:8888/?token=758e8bdfe08e1...............1ed5df1943f
```

Lá você vai encontrar o arquivo *CASE-dashboard.ipynb* responsavel pela merge do streaming.
Abra este pelo jupyter notebook e execulte todas as células.

Dando tudo certo até aqui, você já pode acessar o dashboard pelo link *localhost:5000*.

A aplicação realiza geração aleatória de *vendas* com uma fequencia de 3 vendas por minuto, mas isso pode ser alterado conforme se queira sempre respeitando os limites de infraestrutura na qual o projeto estiver rodando.
Para realizar a alteração da quantidade de vendas por minuto, utilize a URL *http://localhost:30001/update/<vendasPorMinuto>* com o verbo PUT.

Caso queira utilizar o comando curl para isto:
```
$ curl -X PUT http://localhost:30001/update/<vendasPorMinuto>
```
Onde <vendasPorMinuto> é a quantidade de vendas por minuto que quer que sejam geradas.

Os demais arquivos na pasta root, *CASE-ingestao-bronze.ipynb*, *CASE-ingestao-silver.ipynb* e *CASE-ingestao-gold.ipynb* podem ser utilizados para ingestão conforme sugerido por seus nomes.

### acesso ao mysql - transacional
Caso queira acessar o banco de dados, utilize o exemplo abaixo:
```
$ sudo mysql -h127.0.0.1 -P3306 -uroot -proot
```

### observabilidade
O grafana está disponível no link *http://localhost:3000/login*.

Importante saber o ip interno do prometheus para, então adicionar no grafana
Para isto digite o comando *docker network inspect apps_loja*
```
$ sudo docker network inspect apps_loja
```
O resultado é similar ao abaixo:
```
{
    "ConfigOnly": false,
    "Containers": {
        "225b92764a46b971a6fb4dddeb22b304daebbeb9b9c6fcf78e03a5172507dab4": {
            "Name": "zookeeper",
            "EndpointID": "b097bcca2b76de4c67ff52eb17cb26fcbf90f0cbea5bddf0e61f0015c6ecd091",
            "MacAddress": "02:42:ac:12:00:03",
            "IPv4Address": "172.18.0.3/16",
            "IPv6Address": ""
        },
        "27ef7e433b9bb76b3fae1fe2b4e70d6a0212fd390456b1ba97459cc0a82d7676": {
            "Name": "kafka",
            "EndpointID": "39e4ef5f66e93defe920e17ad9065a0aab5f1d38acf835c599155d364469ac38",
            "MacAddress": "02:42:ac:12:00:04",
            "IPv4Address": "172.18.0.4/16",
            "IPv6Address": ""
        },
        "42d32cc877cb16103f2d0b00f738ac7b937e830b2b6640f587510f434f6739a9": {
            "Name": "api_dashboard",
            "EndpointID": "2eead56d6a2ec710178b6de15c403f6a6a153eacbd7c150ed416bff343d78210",
            "MacAddress": "02:42:ac:12:00:07",
            "IPv4Address": "172.18.0.7/16",
            "IPv6Address": ""
        },
        "44d2bc45c657f7b8a950c8a8f52b5b5f36e7a81d9094ea20c23d13bba7534a2c": {
            "Name": "dashboard",
            "EndpointID": "7140e0c02f0ee68772a2da77ce658292ddad1951f831f93fd9bb403d674d8633",
            "MacAddress": "02:42:ac:12:00:08",
            "IPv4Address": "172.18.0.8/16",
            "IPv6Address": ""
        },
        "89462e050e9d75698459004a75345af702c299b4acee735819590de5bc18a3d7": {
            "Name": "grafana",
            "EndpointID": "d75bfbb96d8234c4bcd48b9a196bc85718ad5c1e639cf85e8a078b7bb94ba0a4",
            "MacAddress": "02:42:ac:12:00:0a",
            "IPv4Address": "172.18.0.10/16",
            "IPv6Address": ""
        },
        "96f61d38b5e92e80977ad0e1038888e142f27f9c119486bed205e532372a735b": {
            "Name": "apps-kafdrop-1",
            "EndpointID": "5f8046fe8276315cacd3de8ed60afe18a4ee6934fe341848455b402d33025233",
            "MacAddress": "02:42:ac:12:00:06",
            "IPv4Address": "172.18.0.6/16",
            "IPv6Address": ""
        },
        "c4db85e558960078a46550f7bbde2817fe2fd4dd4ca85ea2a5f19023d642e2b5": {
            "Name": "mysql",
            "EndpointID": "ef2007b942948deb3bf116987e94384bf5e5febb79ef22ef963f0133d84b4d37",
            "MacAddress": "02:42:ac:12:00:02",
            "IPv4Address": "172.18.0.2/16",
            "IPv6Address": ""
        },
        "c80697e37d62bfca29f7ec2e58e7b61d65f24dede36ca6d671ab2f67d49fe2cc": {
            "Name": "loja_simulada",
            "EndpointID": "e718b8c1917f8fe1c46afdb289f093f768caeb98f5baf930e79fbf51715d3116",
            "MacAddress": "02:42:ac:12:00:05",
            "IPv4Address": "172.18.0.5/16",
            "IPv6Address": ""
        },
        "cb6cce5962839f4281504744195636549dd21ebd4d00ca531f9ce77000f1a01a": {
            "Name": "prometheus",
            "EndpointID": "accef1e27a66742c7666be58f7c00f92e0d93174069ce49b856f957936082f1b",
            "MacAddress": "02:42:ac:12:00:09",
            "IPv4Address": "172.18.0.9/16",
            "IPv6Address": ""
        }
    },
    "Options": {},
    "Labels": {
        "com.docker.compose.network": "loja",
        "com.docker.compose.project": "apps",
        "com.docker.compose.version": "2.27.1"
    }
}
```
localize a propriedade **IPv4Address** do container prometheus
```
"cb6cce5962839f4281504744195636549dd21ebd4d00ca531f9ce77000f1a01a": {
    "Name": "prometheus",
    "EndpointID": "accef1e27a66742c7666be58f7c00f92e0d93174069ce49b856f957936082f1b",
    "MacAddress": "02:42:ac:12:00:09",
    "IPv4Address": "172.18.0.9/16",
    "IPv6Address": ""
}
``` 
Utilize o ip encontrado no grafana *172.18.0.9*

Lá é possível observar algumas métricas disponíveis.

### Recomendações
* VSCode
* Postiman

