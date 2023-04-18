# Criar uma API em python, utilizando flask de endereços, enriquecendo os endereços via cep, fazendo as tabelas de endereço e entrega
# Criar a API
# Criar o database
# Integrar ambos
# Ter a rota endereços - listar todos, listar específica, alterar, deletar.
# Ter a rota entregas - listar todas, listar específica, alterar, deletar.
# criar a TABELA de endereços contendo ID ,CEP, rua, bairro, cidade e pais
# criar a TABELA de entregas contendo ID, CEP, numero de pacote, nome do remetente, complemento, numero da casa
# Ao final, gerar um arquivo CSV

from flask import Flask, request, Response
import psycopg2
import psycopg2.extras
import requests
import pandas as pd



#instanciando o Flask:
app = Flask(__name__)

# Conectando o banco de dados, utilizando o psycopg2
con = psycopg2.connect(
    database = "crud_api",
    user = "postgres",
    password = "postgres",
    host = "localhost"
)

# Listar todos os endereços
# Utilizado o Decorator do Flask, cria-se a rota, e escolhe-se o método (no caso, GET), para criar uma função, neste caso para listar todos os endereços
@app.route('/enderecos/', methods=['GET'])
def listar_enderecos():
    cursor = con.cursor()
    cursor.execute("SELECT * FROM enderecos")
    result = cursor.fetchall()
    print(result)
    return result

# Listar endereço por ID
# Repete-se o de cima, mas neste caso, para listar apenas um endereço, identificado por sua ID
@app.route('/enderecos/<id>', methods=['GET'])
def listar_endereco(id):
    cursor = con.cursor()
    cursor.execute(f"SELECT * FROM enderecos WHERE id = {id}")
    result = cursor.fetchall()
    return result

# Listar endereço por CEP
# Novamente repete-se, mas no caso o endereço é identificado por seu CEP (se existente no DB, caso contrário, retorna vazio)
@app.route('/enderecos/cep/<cep>', methods=['GET'])
def listar_enderecos_cep(cep):
    cursor = con.cursor()
    cursor.execute(f"SELECT * FROM enderecos WHERE cep = '{cep}'")
    result = cursor.fetchall()
    return result

# Adicionar endereço
# Aqui cria-se uma rota utilizando o método POST, para adicionar um endereço novo na lista, com ID automático.
@app.route('/enderecos/', methods=['POST'])
def adicionar_endereco(cep = None):
    if cep or request.is_json:
        if not cep:
            add_endereco = request.get_json()
            cep = add_endereco["cep"]
        cep = cep.replace("-", "").replace(".", "").replace(",", "").replace("/", "")
        if len(cep) == 8:
            viacep = requests.get(f"https://viacep.com.br/ws/{cep}/json/").json()
            if viacep == {'erro': True}:
                return "O CEP é inválido"
            else:
                with con.cursor() as cursor:
                    cursor.execute(f"INSERT INTO enderecos (cep, rua, bairro, cidade, estado, pais) values ('{cep}', '{viacep['logradouro']}', '{viacep['bairro']}', '{viacep['localidade']}', '{viacep['uf']}', 'Brasil')")
                    con.commit()
                return "Inserido com sucesso!"
        else:
            return "Cep precisa ter 8 dígitos"
    else:
        return "A requisição precisa ser enviado em JSON"

# Alterar endereço
# Aqui cria-se uma rota utilizando o método PUT, para alterar um endereço específico, via sua ID.
@app.route('/enderecos/<id>', methods=['PUT'])
def alterar_endereco(id):
    if request.is_json:
        add_endereco = request.get_json()
        cep = add_endereco["cep"]
        cep = cep.replace("-", "").replace(".", "").replace(",", "").replace("/", "")
        if len(cep) == 8:
            viacep = requests.get(f"https://viacep.com.br/ws/{cep}/json/").json()
            if viacep == {'erro': True}:
                return "O CEP é inválido"
            else:
                with con.cursor() as cursor:
                    cursor.execute(f"UPDATE enderecos SET cep = '{cep}', rua = '{viacep['logradouro']}', bairro = '{viacep['bairro']}', cidade = '{viacep['localidade']}', estado = '{viacep['uf']}', pais = 'Brasil' WHERE id = {id}")
                    con.commit()
                return "Inserido com sucesso!"
        else:
            return "Cep precisa ter 8 dígitos"
    else:
        return "A requisição precisa ser enviado em JSON"

# # Deletar endereço
# Aqui cria-se uma rota utilizando o método DELETE, para deletar um endereço específico, via sua ID.
@app.route('/enderecos/<id>', methods=['DELETE'])
def deletar_endereco(id):
    with con.cursor() as cursor:
      cursor.execute(f"DELETE FROM enderecos WHERE id = {id}")
      con.commit()
    result = cursor.fetchall()
    return result



# Listar todas as entregas
@app.route('/entregas/', methods=['GET'])
def listar_entregas():
    cursor = con.cursor()
    cursor.execute(f"SELECT * FROM entregas")
    result = cursor.fetchall()
    return result

# Listar entrega por ID
@app.route('/entregas/<id>', methods=['GET'])
def listar_entrega(id):
    cursor = con.cursor()
    cursor.execute(f"SELECT * FROM entregas WHERE id = {id}")
    result = cursor.fetchall()
    return result

# Adicionar entrega
# Aqui, o que foi feito de diferente é: Caso o CEP não exista já na tabela ENDEREÇOS, ele automaticamente o adiciona, para fazer a entrega.
@app.route('/entregas/', methods=['POST'])
def adicionar_entrega():
    if request.is_json:
        conteudo = request.get_json()
        cep = conteudo["cep"]
        cep = cep.replace("-", "").replace(".", "").replace(",", "").replace("/", "")
        if len(cep) == 8:
            if not listar_enderecos_cep(cep):
                retorno = adicionar_endereco(cep)
                if retorno != "Inserido com sucesso!":
                    return retorno
            with con.cursor() as cursor:
                cursor.execute(f"INSERT INTO entregas (cep, numero, complemento, qtd_pacotes, nome_remetente) values ('{cep}','{conteudo['numero']}', '{conteudo['complemento']}', '{conteudo['qtd_pacotes']}', '{conteudo['nome_remetente']}')")
                con.commit()   
            return "Inserido com sucesso!"
        else:
            return "Cep precisa ter 8 dígitos"
    else:
        return "A requisição precisa ser enviado em JSON"

# Alterar entrega
@app.route('/entregas/<id>', methods=['PUT'])
def alterar_entrega(id):
    if request.is_json:
        conteudo = request.get_json()
        cep = conteudo["cep"]
        cep = cep.replace("-", "").replace(".", "").replace(",", "").replace("/", "")
        if len(cep) == 8:
            if not listar_enderecos_cep(cep):
                retorno = adicionar_endereco(cep)
                if retorno != "Inserido com sucesso!":
                    return retorno
            with con.cursor() as cursor:
                cursor.execute(f"UPDATE entregas SET cep = '{cep}', numero = '{conteudo['numero']}', complemento = '{conteudo['complemento']}', qtd_pacotes = '{conteudo['qtd_pacotes']}', nome_remetente = '{conteudo['nome_remetente']}' WHERE id = {id}")
                con.commit()
            return "Inserido com sucesso!"
        else:
            return "Cep precisa ter 8 dígitos"
    else:
        return "A requisição precisa ser enviado em JSON"

# Deletar entrega
@app.route('/entregas/<id>', methods=['DELETE'])
def deletar_entrega(id):
    with con.cursor() as cursor:
        cursor.execute(f"DELETE FROM entregas WHERE id = {id}")
        con.commit()  
    result = cursor.fetchall()
    return result

@app.route('/csv/', methods=['GET'])
def criar_csv():

    con = psycopg2.connect("host=localhost dbname=crud_api user=postgres password=postgres")
    cursor = con.cursor()
    
    sql = cursor.execute("COPY (SELECT * FROM entregas LEFT JOIN enderecos ON enderecos.cep = entregas.cep) TO STDOUT WITH CSV DELIMITER ';'")
    with open("D:/Trabalho/crud_api/crud.csv", "w") as file:
        cursor.copy_expert(sql, file)
    con.commit()
    con.close()
    return "Arquivo CSV exportado com sucesso."
    
    

# Aqui não apenas rodamos a API efetivamente, como o debug=True a mantém ativa, visto suas mudanças durante os testes do código.


app.run(debug=True)