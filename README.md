# Projeto análise de vendas

Utilizei a API FAKESTORE nesse projeto:
https://fakestoreapi.com/

Esse projeto foi feito para praticar as seguintes habilidades:

- PYTHON
- SQL
- PANDAS
- REQUEST
- PYODBC
- JSON
- POWER BI

## Introdução
![bro](https://github.com/xleofarias/DB_STORE/assets/113566725/0c11c685-7a98-4038-b609-9ed6a73a8ea0)



## Python

Utilizei algumas bibliotecas em python:
  
  ## 1- PYODBC:
  ```
    # Informações do banco
    DRIVER = config('DRIVER')
    DB_USER =  config('DB_USER')
    DB_PASSWORD = config('DB_PASSWORD')
    DB_NAME = config('DB_NAME')

    # Conexão do banco
    conexao = pyodbc.connect(f'DRIVER={DRIVER}; DSN={DB_NAME}; UID={DB_USER}; PWD={DB_PASSWORD}')

    # Ativando meu banco para query
    cursor = conexao.cursor()
```

## 2- DECOUPLE
Para deixar as informações do banco ocultas no código utilizei outra biblioteca chamada decouple, caso tenha interesse irei deixar o link da documentação abaixo:

  https://pypi.org/project/python-decouple/


## 3- REQUEST

Após isso comecei a fazer as requisições da API para dentro da minha maquina, fiz da seguinte maneira:

```
# Requisição para coleta os produtos
requisicao = requests.get("https://fakestoreapi.com/products/").text
```

## 4- JSON e PANDAS

```
# Criando string em json
conversao_string_json = json.loads(requisicao)

# Criar DataFrame a partir do dicionário
df = pd.DataFrame(conversao_string_json)

# Salvar apenas os dados do DataFrame em um arquivo JSON
guarda = df.to_json("produtos.json", orient="records" , indent=4)

```
Dessa forma possibilitando trabalhar com o json em python sem problemas. Fiz algumas manipulações para deixar o arquivo json da forma que queria:

```
# Para abrir o campo json
with open("produtos.json") as file:
    # Carregar seu conteúdo e torná-lo um novo dicionário
    dados = json.load(file)
# Excluir o par chave-valor de cada pedido e separando a coluna rating para duas colunas rate e count
    for dado in dados:
        dado["rate"] = dado["rating"]["rate"]
        dado["count"] = dado["rating"]["count"]
        del dado["rating"]
        del dado["description"]
        del dado["image"]
    
# Salvando meu arquivo json filtrado para uso no banco
with open("fil_produtos.json", "w") as file:
    arquivo_novo = json.dump(dados, file, indent=4)
```

E para inserir as informações dentro do banco eu utilizei o seguinte código mesclando um pouco de SQL:

## 5- PANDAS e SQL

```
# Para ler o arquivo em json
arquivo_novo = pd.read_json("fil_produtos.json")

# Solicitando as informações do banco
pesquisar = "SELECT * FROM db_store.produtos"
# Executando a query
cursor.execute(pesquisar)
# Realizando a procura pela chave id
id_existente = set(row[0] for row in cursor.fetchall())

for id, title, price, category, rate, count in zip(
    arquivo_novo["id"], arquivo_novo["title"], arquivo_novo["price"], arquivo_novo["category"], arquivo_novo["rate"], arquivo_novo["count"]):
    if id not in id_existente:
        cursor.execute("INSERT INTO db_store.produtos (ID_PRODUTO, TITULO, PRECO, CATEGORIA, AVALIACAO, QTD_AVALIACAO) VALUES (?, ?, ?, ?, ?, ?)", (id_prod, title, price, category, rate, count))
        print(f"O Produto {id}: {title} foi inserido com sucesso!")
    else:
        print(f"O Produto {id}: {title} já está cadastrado!.")

```

Realizei a mesma estrutura para outras parte do código sempre realizando a manipulação do json, pandas, pyobdc para fazer os tratamentos e inserir dentro do banco, todo o código se encontra nos arquivos.

## 6- SQL

Utilizei a ferramenta MySQL WorkBench para realizar a construção do banco e da database:

![image](https://github.com/xleofarias/DB_STORE/assets/113566725/a4fbf577-2d53-400e-868a-4752d30f9b2c)

E para fazer um filtro nos dados criei essa query:

```
SELECT
    pedidos.DATA_PEDIDO,
	pedidos.ID_PEDIDO,
    usuarios.NOME,
    usuarios.SOBRENOME,
    produtos.ID_PRODUTO,
    produtos.TITULO,
    pedidos.QUANTIDADE,
    produtos.PRECO,
    CAST(pedidos.QUANTIDADE * produtos.PRECO AS DECIMAL(5, 2)) AS COMPRA_VALOR_TOTAL
FROM 
	pedidos
    INNER JOIN
		produtos ON pedidos.ID_PRODUTO = produtos.ID_PRODUTO
	INNER JOIN
		usuarios ON pedidos.USER_ID = usuarios.USER_ID
ORDER BY
	DATA_PEDIDO

```
## 7- POWER BI

No POWER BI criei uma home para a análise da fake loja:

![image](https://github.com/xleofarias/DB_STORE/assets/113566725/77126755-7919-40e7-940d-4278d170ebad)


O DASHBOARD se encontra assim até o momento:

![image](https://github.com/xleofarias/DB_STORE/assets/113566725/538e1d32-f227-42d8-b8a9-25d1630bd983)


Esse é um dos projetos de muitos, aceito dicas e obrigado!

