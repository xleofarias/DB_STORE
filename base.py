import requests
import pandas as pd
import pyodbc
import json
from decouple import config

# Para saber quais drivers tenho instalado
# for driver in pyodbc.drivers():
    # print(driver)

# Informações do banco
DRIVER = config('DRIVER')
DB_USER =  config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_NAME = config('DB_NAME')

# Conexão do banco
conexao = pyodbc.connect(f'DRIVER={DRIVER}; DSN={DB_NAME}; UID={DB_USER}; PWD={DB_PASSWORD}')

# Ativando meu banco para query
cursor = conexao.cursor()

# Requisição para coleta os produtos
requisicao = requests.get("https://fakestoreapi.com/products/").text

# Criando string em json
conversao_string_json = json.loads(requisicao)

# Criar DataFrame a partir do dicionário
df = pd.DataFrame(conversao_string_json)

# Salvar apenas os dados do DataFrame em um arquivo JSON
guarda = df.to_json("produtos.json", orient="records" , indent=4)

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

# ------------------------------------------------------ CÓDIGO DO CARRINHOS --------------------------------------------------------------------

# Requisição para coleta os carrinho
requisicao_carrinhos = requests.get("https://fakestoreapi.com/carts").text


# Criando string em json
conversao_string_json = json.loads(requisicao_carrinhos)

# Criar DataFrame a partir do dicionário
df_carrinhos = pd.DataFrame(conversao_string_json)

# Salvar apenas os dados do DataFrame em um arquivo JSON
guarda_arquivo = df_carrinhos.to_json("carrinhos.json", orient="records" , indent=4)

# Para abrir o campo json
with open("carrinhos.json") as file:
    # Carregar seu conteúdo e torná-lo um novo dicionário
    dados = json.load(file)
# Excluir o par chave-valor de cada pedido
    for dado in dados:
        del dado["__v"]
    
# Salvando meu arquivo json filtrado para uso no banco
with open("fil_carrinhos.json", "w") as file:
    arquivo_novo = json.dump(dados, file, indent=4)

# Abrindo o arquivo json filtrado para uso no banco
with open("fil_carrinhos.json", "r") as file:
    arquivo_novo = json.load(file)

# Função para repetir as chaves "id", "userId" e "date" em cada "products"
def repeat_keys(pedidos):
    novos_pedidos = []

    for entrada in pedidos:
        entrada_id = entrada["id"]
        entrada_user_id = entrada["userId"]
        entrada_data = entrada["date"]

        for produtos in entrada["products"]:
            nova_entrada = {
                "id": entrada_id,
                "userId": entrada_user_id,
                "date": entrada_data,
                **produtos
            }
            novos_pedidos.append(nova_entrada)

    return novos_pedidos

# Chame a função com seu JSON original
novos_pedidos = repeat_keys(arquivo_novo)

# Salvando o novo json de carrinhos
with open("fil_carrinhos.json", "w") as file:
    arquivo_novo = json.dump(novos_pedidos, file, indent=4)

# Abrindo o arquivo json filtrado para uso no banco
with open("fil_produtos.json") as file:
    arquivo_novo = json.load(file)

# Para ler os arquivos em json
pedidos = pd.read_json("fil_carrinhos.json")
pedidos_extras = pd.read_json("carrinhos_extra.json")

# Para concatenar os dois arquivos em json
pedidos_full = pd.concat([pedidos, pedidos_extras])

# Solicitando as informações do banco
pesquisar = "SELECT * FROM db_store.pedidos"
cursor.execute(pesquisar)
id_existente = set(row[0] for row in cursor.fetchall())

# Iterando sobre os novos usuários e inserindo-os se não estiverem no banco de dados
for coluna_id, coluna_user_id, coluna_date, coluna_product_id, coluna_quantidade in zip(
    pedidos_full["id"], pedidos_full["userId"], pedidos_full["date"], pedidos_full["productId"], pedidos_full["quantity"]
):    # Verificando se existe os mesmos dados no banco para não gerar duplicidade
    if coluna_id not in id_existente:
        cursor.execute("INSERT INTO db_store.pedidos (ID_PEDIDO, USER_ID, DATA_PEDIDO, ID_PRODUTO, QUANTIDADE) VALUES (?, ?, ?, ?, ?)", (coluna_id, coluna_user_id, coluna_date, coluna_product_id, coluna_quantidade))
        print(f"O pedido de nº{coluna_id} foi inserido com sucesso!")
    else:
            print(f"O pedido de nº{coluna_id} já está cadastrado!.")

# ------------------------------------------------------ CÓDIGO DOS USUARIOS ----------------------------------------------------------

requisicao_usuarios = requests.get('https://fakestoreapi.com/users').text

# Criando string em json
conversao_string_json = json.loads(requisicao_usuarios)

# Criar DataFrame a partir do dicionário
df_usuarios = pd.DataFrame(conversao_string_json)

# Salvar apenas os dados do DataFrame em um arquivo JSON
guarda_arquivo = df_usuarios.to_json("usuarios.json", orient="records" , indent=4)

# Para abrir o campo json
with open("usuarios.json") as file:
    # Carregar seu conteúdo e torná-lo um novo dicionário
    dados = json.load(file)
    for dado in dados:
        dado["city"] = dado["address"]["city"]
        dado["street"] = dado["address"]["street"]
        dado["number"] = dado["address"]["number"]
        dado["zipcode"] = dado["address"]["zipcode"]
        dado["lat"] = dado["address"]["geolocation"]["lat"]
        dado["long"] = dado["address"]["geolocation"]["long"]
        dado["firstname"] = dado["name"]["firstname"]
        dado["lastname"] = dado["name"]["lastname"]
        del dado["address"]
        del dado["name"]
        del dado["__v"]

# Salvando meu arquivo json filtrado para uso no banco
with open("fil_usuarios.json", "w") as file:
    usuarios = json.dump(dados, file, indent=4)

with open("fil_usuarios.json") as file:
    usuairos = json.load(file)

# Para ler o arquivo em json
usuarios = pd.read_json("fil_usuarios.json")

# Para ler o arquivo de produtos extra
usuarios_extras = pd.read_json("usuarios_extra.json")

# Combinando os DataFrames
usuarios_full = pd.concat([usuarios, usuarios_extras])

# Query para verificar os IDs existentes no banco de dados
query = "SELECT * FROM db_store.usuarios"
cursor.execute(query)
id_existente = set(row[0] for row in cursor.fetchall())

# Iterando sobre os novos usuários e inserindo-os se não estiverem no banco de dados
for id_user, email, nome_user, senha, telefone, cidade, rua, num_casa, cep, lat, long, nome, sobrenome in zip(
    usuarios_full["id"], usuarios_full["email"], usuarios_full["username"], usuarios_full["password"],
    usuarios_full["phone"], usuarios_full["city"], usuarios_full["street"], usuarios_full["number"],
    usuarios_full["zipcode"], usuarios_full["lat"], usuarios_full["long"], usuarios_full["firstname"],
    usuarios_full["lastname"]
):
    if id_user not in id_existente:
        cursor.execute("INSERT INTO db_store.usuarios (USER_ID, EMAIL, NOME_USER, SENHA, TELEFONE, CIDADE, RUA, NUM_CASA, CEP, LATITUDE, LONGITUDE, NOME, SOBRENOME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       (id_user, email, nome_user, senha, telefone, cidade, rua, num_casa, cep, lat, long, nome, sobrenome))
        print(f"O usuário {nome} inserido com sucesso!")
    else:
        print(f"O usuário {nome} já está cadastrado!")

# Salvando a query no banco
conexao.commit()

# Fechando a conexão com o banco
conexao.close()