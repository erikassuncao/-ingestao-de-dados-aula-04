### Aula 04 - Ingestão de Dados - PECE/USP

#### Inclusão da plataforma AWS EMR na solução para processar as rotinas Python criadas na Aula 02.

<img width="916" alt="Captura de Tela 2022-08-08 às 14 05 03" src="https://user-images.githubusercontent.com/9318332/183473783-280c4155-34ee-4742-af33-3730b796e194.png">

1) Criamos o bucket e carregamos o CSV na pasta RAW

2) Criamos o banco de dados Mysql no RDS

3) Criamos o cluster para execução dos steps;

4) Implementamos os steps individuais em Python que tratam os dados e salvam os dimensionamentos e fatos em “Trusted”.

5) Criamos o catalogo utilizando o AWS Glue
    
## Referência
 - [Ranking de Instituições por Índice de Reclamações](https://dados.gov.br/dataset/ranking-de-instituicoes-por-indice-de-reclamacoes)
 - [ Tarifas Bancárias - por Segmento e por Instituição](https://dados.gov.br/dataset/tarifas-bancarias-por-segmento-e-por-instituicao)
 - [Amazon EMR](https://aws.amazon.com/pt/emr/)
 - [AWS Glue](https://aws.amazon.com/pt/glue/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc)
## Autores

- [@erikassuncao](https://www.github.com/erikassuncao)
- Jorge Alexandre Pires de Oliveira
- Paulo Henrique de Souza Pereira Prazeres
- Willian Alves Barboza
