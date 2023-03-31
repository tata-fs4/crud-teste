#O DESAFIO VAI SER:
#1 -> Criar uma aplicação API em Python que tenha rotas que façam o processo de CRUD. 
# Ela não pode parar de rodar em nenhuma hipótese. 
# Será uma aplicação de Consulta, registro, edição e remoção de CEPs. 
# Ela devera pedir o nome + CEP para registrar e deve enriquecer com mais coisas como Rua Bairro e etc...
# Reiniciar a aplicação nao pode perder os dados dela
#BONUS: Deve possuir uma rota que gere um arquivo CSV com os dados contidos dentro da aplicação
 # Gerar um arquivo CSV das entregas cruzando com os ceps
#BONUS: Criar um outro CRUD de Entregas. Criatividade será avaliada. Se houver cruzamento de informações dela com o CEP será um extra

#1 -> Criar o database

#2 -> Criar as tabelas 
    #2.1 -> enderecos
        # id: Int/Serial,
        # cep: Varchar(8), 
        # pais Varchar(50), 
        # estado: Varchar(50), 
        # cidade: Varchar(50), 
        # bairro: Varchar(50), 
        # rua: Varchar(100)

    #2.2 -> entregas 
        # id: (int/serial), 
        # cep: varchar(8), 
        # numero: varchar(30) 
        # complemento: varchar(50), 
        # qtd_pacote: (int), 
        # nome_remetente: varchar(30)
        
#3 -> Criar API Python
    #3.1 -> Rota CRUD: enderecos
    #3.2 -> Rota CRUD: entregas
        #3.3 -> Endpoint do arquivo consolidado das entregas.
#4 -> Enriquecer o CEP com os dados relacionados a ele: ViaCep

################################################
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from flask import Flask, request
from json import dumps
from flask.json import jsonify
from requests import get
import csv

print("Iniciando a aplicação!")

param_dic = {
    "host" : "localhost",
    "user" : "postgres",
    "password" : "postgres",
}

conn = psycopg2.connect(**param_dic)

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);

cursos = conn.cursor()


#database
print("Avaliando a existencia do banco de dados crud_api")
cursor = conn.cursor()
cursor.execute("SELECT * FROM pg_catalog.pg_database WHERE datname = 'crud_api'")
exists = cursor.fetchone()
if not exists:
    cursor.execute("CREATE DATABASE crud_api")
    print("Não Existe.")
    print("Criando o banco de dados")
    cursor.close()
else: 
    print("O banco de dados existe")
conn.close() 
param_dic_crud_api = {
    "host" : "localhost",
    "user" : "postgres",
    "password" : "postgres",
    "dbname" : "crud_api"
}
conn_crud_api = psycopg2.connect(**param_dic_crud_api)

# TABELA ENDERECOS
cursor = conn_crud_api.cursor()
print("Avaliando/Criando a tabela 'enderecos'")
query = """CREATE TABLE IF NOT EXISTS enderecos (
     id serial PRIMARY KEY,
     cep VARCHAR(8) NOT NULL,
     pais VARCHAR(50) NOT NULL,
     estado VARCHAR(50) NOT NULL,
     cidade VARCHAR(50) NOT NULL,
     bairro VARCHAR(50) NOT NULL,
     rua VARCHAR (100) NOT NULL
)
"""
cursor.execute(query, conn_crud_api)
conn_crud_api.commit()
cursor.close()

# TABELA ENTREGAS
cursor = conn_crud_api.cursor()
print("Avaliando/Criando a tabela 'entregas'")
query = """CREATE TABLE IF NOT EXISTS entregas (
     id serial PRIMARY KEY,
     cep VARCHAR(8) NOT NULL,
     numero VARCHAR(30) NOT NULL,
     complemento VARCHAR(50) NOT NULL,
     qtd_pacote VARCHAR(5) NOT NULL,
     nome_remetente VARCHAR(30) NOT NULL,
     pais VARCHAR(50) NOT NULL
)
"""
cursor.execute(query, conn_crud_api)
conn_crud_api.commit()
cursor.close()

# Criando API Python
app = Flask(__name__)
app.url_map.strict_slashes = False

# Rota CRUD enderecos
@app.route("/") # HOME
def home():
    return dumps({})
# Para saber se a página está viva

# Rota CRUD: Endereços

def db_listar_enderecos():
    with conn_crud_api.cursor() as cursor:
        try:
            print("Listando todos os dados da tabela 'endereco'")
            cursor.execute("SELECT * FROM enderecos")
            return cursor.fetchall()
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            conn_crud_api.commit()


@app.route("/enderecos/", methods=["GET"])
def listar_enderecos():
    enderecos = []
    for endereco in db_listar_enderecos():
        enderecos.append({
            "id": endereco[0],
            "cep": endereco[1],
            "rua": endereco[2],
            "pais": endereco[3],
            "estado": endereco[4],
            "cidade": endereco[5],
            "bairro": endereco[6]
        })
    return jsonify(enderecos)


def db_listar_endereco(id):
    with conn_crud_api.cursor() as cursor:
        try:
            print("Listando o dado da tabela 'endereco'")
            cursor.execute(f"SELECT * FROM enderecos WHERE id = {id}")
            return cursor.fetchone()
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            conn_crud_api.commit()


@app.route("/enderecos/<int:id>", methods=["GET"])
def listar_endereco(id):
    endereco = db_listar_endereco(id)
    if endereco:
        return jsonify({
        "id": endereco[0],
        "cep": endereco[1],
        "rua": endereco[2],
        "pais": endereco[3],
        "estado": endereco[4],
        "cidade": endereco[5],
        "bairro": endereco[6]
        })    
    else:
        return jsonify({})


def db_adicionar_enderecos(cep, rua, bairro, estado, cidade, pais):
    with conn_crud_api.cursor() as cursor:
        try:
            print(f"Adicionando cep '{cep}' na tabela 'enderecos'")
            cursor.execute(f"INSERT INTO enderecos (cep, rua, bairro, estado, cidade, pais) VALUES ('{cep}', '{rua}', '{bairro}', '{estado}', '{cidade}', '{pais}')")
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            conn_crud_api.commit()


@app.route("/enderecos/", methods=["POST"])
def adicionar_endereco():
    if request.is_json:
        if "cep" in request.json:
            cep = request.json["cep"].replace("-","")
            if len(cep) == 8 and cep.isnumeric():
                endereco_completo = get(f"https://viacep.com.br/ws/{cep}/json/").json()
                if "cep" in endereco_completo:
                    db_adicionar_enderecos(cep, endereco_completo["logradouro"], endereco_completo["bairro"], endereco_completo["uf"], endereco_completo["localidade"], "Brasil")
                    return jsonify({"message": "Dado inserido com sucesso."})
                else:
                    return jsonify({"message": "Informe um CEP valido."})
            else:
                return jsonify({"message": "Informe um CEP valido."})
        else:
            return jsonify({"message": "O JSON body precisa conter o campo 'cep'."})
    else:
        return jsonify({"message": "A informação precisa ser enviada via JSON."})


def db_alterar_enderecos(id, cep, rua, bairro, estado, cidade, pais):
    with conn_crud_api.cursor() as cursor:
        try:
            print(f"Atualizando id '{id}' da tabela 'enderecos'")
            cursor.execute(f"UPDATE enderecos SET cep = '{cep}', rua = '{rua}', bairro = '{bairro}', estado = '{estado}', cidade = '{cidade}', pais = '{pais}' WHERE id = '{id}'")
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            conn_crud_api.commit()


@app.route("/enderecos/<int:id>", methods=["PUT"])
def alterar_endereco(id):
    endereco = db_listar_endereco(id)
    if not endereco:
        return jsonify({"message": "Id informado não existente no banco."})
    if request.is_json:
        endereco = {
            "id": endereco[0],
            "cep": endereco[1],
            "rua": endereco[2],
            "pais": endereco[3],
            "estado": endereco[4],
            "cidade": endereco[5],
            "bairro": endereco[6]
        }
        if cep in request.json:
            cep = request.json["cep"].replace("-","")
            if endereco["cep"] != cep:
                if len(cep) == 8 and  cep.isnumeric():
                    endereco_completo = get(f"https://viacep.com.br/ws/{cep}/json/")
                    if "cep" in endereco_completo:
                        #TODO validar a existencia do cep na tabela antes de inserir.
                        db_alterar_enderecos(id, cep, endereco_completo["logradouro"], endereco_completo["bairro"], endereco_completo["uf"], endereco_completo["localidade"], "Brasil")
                        return jsonify({"message": "Dado atualizado com sucesso."})
                    else:
                        return jsonify({"message": "Informe um cep válido"})
                else:
                    return jsonify({"message": "Informe um cep válido"})
            else:
                return jsonify({"message": "CEP igual ao do banco de dados."})
        else:
            if "rua" not in request.json:
                return jsonify({"message": "O JSON Body precisa conter o campo 'rua'."})
            elif "bairro" not in request.json:
                return jsonify({"message": "O JSON Body precisa conter o campo 'bairro'."})
            elif "estado" not in request.json:
                return jsonify({"message": "O JSON Body precisa conter o campo 'estado'."})
            elif "cidade" not in request.json:
                return jsonify({"message": "O JSON Body precisa conter o campo 'cidade'."})
            elif "pais" not in request.json:
                return jsonify({"message": "O JSON Body precisa conter o campo 'pais'."})
            else:
                endereco["rua"] = request.json["rua"]
                endereco["bairro"] = request.json["bairro"]
                endereco["estado"] = request.json["estado"]
                endereco["cidade"] = request.json["cidade"]
                endereco["pais"] = request.json["pais"]
                db_alterar_enderecos(id, endereco["cep"], endereco["rua"], endereco["bairro"], endereco["estado"], endereco["cidade"], endereco["pais"])
                return jsonify({"message": "Dado atualizado com sucesso."})
    else:
        return jsonify({"message": "A informação precisa ser enviada via JSON."})         


def db_deletar_enderecos(id):
    with conn_crud_api.cursor() as cursor:
        try:
            print(f"Removendo id '{id}' da tabela 'enderecos'")
            cursor.execute(f"DELETE FROM enderecos WHERE id = {id}")
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            conn_crud_api.commit()


@app.route("/enderecos/<int:id>", methods=["DELETE"])
def remover_endereco(id):
    if db_listar_endereco(id):
        db_deletar_enderecos(id)
        return jsonify({"message": f"Id {id} removido com sucesso."})
    else:
        return jsonify({"message": "Id informado não existente no banco."})
    
    
# Rota CRUD entregas

def db_listar_entregas():
    with conn_crud_api.cursor() as cursor:
        try:
            print("Listando todos os dados da tabela 'entrega'")
            cursor.execute("SELECT * FROM entregas")
            return cursor.fetchall()
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            conn_crud_api.commit()


@app.route("/entregas/", methods=["GET"])
def listar_entregas():
    entregas = []
    for entrega in db_listar_entregas():
        entregas.append({
            "id": entrega[0],
            "cep": entrega[1],
            "numero": entrega[2],
            "complemento": entrega[3],
            "qtd_pacote": entrega[4],
            "nome_remetente": entrega[5],
            "pais": entrega[6]
        })
    return jsonify(entregas)


def db_listar_entrega(id):
    with conn_crud_api.cursor() as cursor:
        try:
            print("Listando o dado da tabela 'entrega'")
            cursor.execute(f"SELECT * FROM entregas WHERE id = {id}")
            return cursor.fetchone()
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            conn_crud_api.commit()


@app.route("/entregas/<int:id>", methods=["GET"])
def listar_entrega(id):
    entrega = db_listar_entrega(id)
    if entrega:
        return jsonify({
        "id": entrega[0],
        "cep": entrega[1],
        "numero": entrega[2],
        "complemento": entrega[3],
        "qtd_pacote": entrega[4],
        "nome_remetente": entrega[5],
        "pais": entrega[6]
        })    
    else:
        return jsonify({})


def db_adicionar_entregas(cep, numero, complemento, qtd_pacote, nome_remetente, pais):
    with conn_crud_api.cursor() as cursor:
        try:
            print(f"Adicionando cep '{cep}' na tabela 'entregas'")
            cursor.execute(f"INSERT INTO entregas (cep, numero, complemento, qtd_pacote, nome_remetente, pais) VALUES ('{cep}', '{numero}', '{complemento}', '{qtd_pacote}', '{nome_remetente}', '{pais}')")
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            conn_crud_api.commit()


@app.route("/entregas/", methods=["POST"])
def adicionar_entrega():
    if request.is_json:
        if "cep" in request.json:
            cep = request.json["cep"].replace("-","")
            if len(cep) == 8 and cep.isnumeric():
                entrega_completo = get(f"https://viacep.com.br/ws/{cep}/json/").json()
                if "cep" in entrega_completo:
                    db_adicionar_entregas(cep, entrega_completo["logradouro"], entrega_completo["bairro"], entrega_completo["uf"], entrega_completo["localidade"], "Brasil")
                    return jsonify({"message": "Dado inserido com sucesso."})
                else:
                    return jsonify({"message": "Informe um CEP valido."})
            else:
                return jsonify({"message": "Informe um CEP valido."})
        else:
            return jsonify({"message": "O JSON body precisa conter o campo 'cep'."})
    else:
        return jsonify({"message": "A informação precisa ser enviada via JSON."})


def db_alterar_entregas(id, cep, numero, complemento, qtd_pacote, nome_remetente):
    with conn_crud_api.cursor() as cursor:
        try:
            print(f"Atualizando id '{id}' da tabela 'entregas'")
            cursor.execute(f"UPDATE entregas SET cep = '{cep}', numero = '{numero}', complemento '{complemento}', qtd_pacote '{qtd_pacote}', nome_remetente '{nome_remetente}'  WHERE id = '{id}'")
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            conn_crud_api.commit()


@app.route("/entregas/<int:id>", methods=["PUT"])
def alterar_entrega(id):
    entrega = db_listar_entrega(id)
    if not entrega:
        return jsonify({"message": "Id informado não existente no banco."})
    if request.is_json:
        entrega = {
            "id": entrega[0],
            "cep": entrega[1],
            "numero": entrega[2],
            "complemento": entrega[3],
            "qtd_pacote": entrega[4],
            "nome_remetente": entrega[5],
            "pais": entrega[6]
        }
        if cep in request.json:
            cep = request.json["cep"].replace("-","")
            if entrega["cep"] != cep:
                if len(cep) == 8 and  cep.isnumeric():
                    entrega_completo = get(f"https://viacep.com.br/ws/{cep}/json/")
                    if "cep" in entrega_completo:
                        #TODO validar a existencia do cep na tabela antes de inserir.
                        db_alterar_entregas(id, cep, entrega_completo["logradouro"], entrega_completo["bairro"], entrega_completo["uf"], entrega_completo["localidade"])
                        return jsonify({"message": "Dado atualizado com sucesso."})
                    else:
                        return jsonify({"message": "Informe um cep válido"})
                else:
                    return jsonify({"message": "Informe um cep válido"})
            else:
                return jsonify({"message": "CEP igual ao do banco de dados."})
        else:
            if "rua" not in request.json:
                  return jsonify({"message": "O JSON Body precisa conter o campo 'rua'."})
            elif "bairro" not in request.json:
                return jsonify({"message": "O JSON Body precisa conter o campo 'bairro'."})
            elif "estado" not in request.json:
                return jsonify({"message": "O JSON Body precisa conter o campo 'estado'."})
            elif "cidade" not in request.json:
                return jsonify({"message": "O JSON Body precisa conter o campo 'cidade'."})
            elif "pais" not in request.json:
                return jsonify({"message": "O JSON Body precisa conter o campo 'pais'."})
            else:
                entrega["rua"] = request.json["rua"]
                entrega["bairro"] = request.json["bairro"]
                entrega["estado"] = request.json["estado"]
                entrega["cidade"] = request.json["cidade"]
                entrega["pais"] = request.json["pais"]
                db_alterar_entregas(id, entrega["cep"], entrega["numero"], entrega["complemento"], entrega["qtd_pacote"], entrega["nome_remetente"])
                return jsonify({"message": "Dado atualizado com sucesso."})
    else:
        return jsonify({"message": "A informação precisa ser enviada via JSON."})         


def db_deletar_entregas(id):
    with conn_crud_api.cursor() as cursor:
        try:
            print(f"Removendo id '{id}' da tabela 'entregas'")
            cursor.execute(f"DELETE FROM entregas WHERE id = {id}")
        except Exception as e:
            print(f"Erro: {e}")
        finally:
            conn_crud_api.commit()


@app.route("/entregas/<int:id>", methods=["DELETE"])
def remover_entrega(id):
    if db_listar_entrega(id):
        db_deletar_entregas(id)
        return jsonify({"message": f"Id {id} removido com sucesso."})
    else:
        return jsonify({"message": "Id informado não existente no banco."})
# Execução da API
if __name__ == "__main__":
    app.run(debug=True)
    
# Arquivo CSV
