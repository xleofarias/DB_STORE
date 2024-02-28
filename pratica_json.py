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
# Excluir o par chave-valor de cada pedido
    for dado in dados:
        del dado["description"]
        del dado["image"]
    
# Salvando meu arquivo json filtrado para uso no banco
with open("fil_produtos.json", "w") as file:
    arquivo_novo = json.dump(dados, file, indent=4)

# Para ler o arquivo em json
ler_arquivo = pd.read_json("fil_produtos.json")

# Para separar o rate e count a coluna rating assim se tornando 2 colunas
ler_arquivo[["rate", "count"]] = ler_arquivo["rating"].apply(pd.Series)

# Para excluir a coluna rating que não será mais necessária
arquivo_filtrado = ler_arquivo.drop("rating", axis=1)

# Valores de cada coluna
coluna_id = arquivo_filtrado["id"]
coluna_title = arquivo_filtrado["title"]
coluna_price = arquivo_filtrado["price"]
coluna_category = arquivo_filtrado["category"]
coluna_rate = arquivo_filtrado["rate"]
coluna_count = arquivo_filtrado["count"]

# Solicitando as informações do banco
pesquisar = "SELECT * FROM db_store.produtos"

# Executando a query
cursor.execute(pesquisar)

# Para retornar os valores do banco
resultado = cursor.fetchone()

# Verificando se existe os mesmos dados no banco para não gerar duplicidade
if resultado is None:
    for id_prod, title, price, category, rate, count in zip(coluna_id, coluna_title, coluna_price, coluna_category, coluna_rate, coluna_count):
        cursor.execute("INSERT INTO db_store.produtos (ID_PRODUTO, TITULO, PRECO, CATEGORIA, AVALIACAO, QTD_AVALIACAO) VALUES (?, ?, ?, ?, ?, ?)", (id_prod, title, price, category, rate, count))
        print(f"O Produto {id_prod.ID_PRODUTO}: {title.ID_PRODUTO} foi inserido com sucesso!")
else:
    cursor.execute("SELECT * FROM db_store.produtos")
    linhas = cursor.fetchall()
    for linha in linhas:
        print(f"O Produto {linha.ID_PRODUTO}: {linha.TITULO} já está cadastrado!.")


# ------------------------------------------------------CÓDIGO DO CARRINHO--------------------------------------------------------------------

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

# Para ler o arquivo em json
pedidos = pd.read_json("fil_carrinhos.json")

# Valores de cada coluna
coluna_id = pedidos["id"]
coluna_user_id = pedidos["userId"]
coluna_date = pedidos["date"]
coluna_product_id = pedidos["productId"]
coluna_quantidade = pedidos["quantity"]

# Solicitando as informações do banco
pesquisar = "SELECT * FROM db_store.pedidos"

# Executando a query
cursor.execute(pesquisar)

# Para retornar os valores do banco
resultado = cursor.fetchone()

# Verificando se existe os mesmos dados no banco para não gerar duplicidade
if resultado is None:
    for id_prod, user_id, date, produto_id, quantidade in zip(coluna_id, coluna_user_id, coluna_date, coluna_product_id, coluna_quantidade):
        cursor.execute("INSERT INTO db_store.pedidos (ID_PEDIDO, USER_ID, DATA_PEDIDO, ID_PRODUTO, QUANTIDADE) VALUES (?, ?, ?, ?, ?)", (id_prod, user_id, date, produto_id, quantidade))
        print(f"O pedido foi inserido com sucesso!")
else:
    cursor.execute("SELECT * FROM db_store.pedidos")
    linhas = cursor.fetchall()
    for linha in linhas:
        print(f"O pedido já está cadastrado!.")

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

# Valores de cada coluna
coluna_id = usuarios["id"]
coluna_email = usuarios["email"]
coluna_user = usuarios["username"]
coluna_pwd = usuarios["password"]
coluna_telefone = usuarios["phone"]
coluna_cidade = usuarios["city"]
coluna_rua = usuarios["street"]
coluna_numero = usuarios["number"]
coluna_cep = usuarios["zipcode"]
coluna_latitude = usuarios["lat"]
coluna_longitude = usuarios["long"]
coluna_nome = usuarios["firstname"]
coluna_sobrenome = usuarios["lastname"]

# Solicitando as informações do banco
pesquisar = "SELECT * FROM db_store.usuarios"

# Executando a query
cursor.execute(pesquisar)

# Para retornar os valores do banco
resultado = cursor.fetchone()

# Verificando se existe os mesmos dados no banco para não gerar duplicidade
if resultado is None:
    for id_user, email, nome_user, senha, telefone, cidade, rua, num_casa, cep, lat, long, nome, sobrenome in zip(coluna_id, coluna_email, coluna_user, coluna_pwd, coluna_telefone, coluna_cidade, coluna_rua, coluna_numero, coluna_cep, coluna_latitude, coluna_longitude, coluna_nome, coluna_sobrenome):
        cursor.execute("INSERT INTO db_store.usuarios (USER_ID, EMAIL, NOME_USER, SENHA, TELEFONE, CIDADE, RUA, NUM_CASA, CEP, LATITUDE, LONGITUDE, NOME, SOBRENOME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (id_user, email, nome_user, senha, telefone, cidade, rua, num_casa, cep, lat, long, nome, sobrenome))
        print(f"O usuário foi inserido com sucesso!")
else:
    cursor.execute("SELECT * FROM db_store.usuarios")
    linhas = cursor.fetchall()
    for linha in linhas:
        print(f"O usuário já está cadastrado!.")

# Salvando a query no banco
conexao.commit()

# Fechando a conexão com o banco
conexao.close()