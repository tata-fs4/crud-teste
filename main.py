# Criar uma API em python, utilizando flask de endereços, enriquecendo os endereços via cep, fazendo as tabelas de endereço e entrega
# Criar a API
# Criar o database
# Integrar ambos
# Ter a rota endereços - listar todos, listar específica, alterar, deletar.
# Ter a rota entregas - listar todas, listar específica, alterar, deletar.
# criar a TABELA de endereços contendo ID ,CEP, rua, bairro, cidade e pais
# criar a TABELA de entregas contendo ID, CEP, numero de pacote, nome do remetente, complemento, numero da casa
# Ao final, gerar um arquivo CSV

from flask import Flask, request, jsonify
import psycopg2
import psycopg2.extras
import requests
import pandas as pd



#instanciando o Flask:
app = Flask(__name__)

# Conectando o banco de dados, utilizando o psycopg2
def get_db_connection():
    return psycopg2.connect(
        database="crud_api",
        user="postgres",
        password="postgres",
        host="localhost"
    )

# Listar todos os endereços
# Utilizado o Decorator do Flask, cria-se a rota, e escolhe-se o método (no caso, GET), para criar uma função, neste caso para listar todos os endereços
@app.route('/enderecos/', methods=['GET'])
def listar_enderecos():
    con = get_db_connection()
    cursor = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT * FROM enderecos")
    result = cursor.fetchall()
    cursor.close()
    con.close()
    return jsonify(result)

# Listar endereço por ID
# Repete-se o de cima, mas neste caso, para listar apenas um endereço, identificado por sua ID
@app.route('/enderecos/<int:id>', methods=['GET'])
def listar_endereco(id):
    con = get_db_connection()
    cursor = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT * FROM enderecos WHERE id = %s", (id,))
    result = cursor.fetchone()
    cursor.close()
    con.close()
    return jsonify(result)

# Listar endereço por CEP
# Novamente repete-se, mas no caso o endereço é identificado por seu CEP (se existente no DB, caso contrário, retorna vazio)
@app.route('/enderecos/cep/<cep>', methods=['GET'])
def listar_enderecos_cep(cep):
    con = get_db_connection()
    cursor = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT * FROM enderecos WHERE cep = %s", (cep,))
    result = cursor.fetchall()
    cursor.close()
    con.close()
    return jsonify(result)

# Adicionar endereço
# Aqui cria-se uma rota utilizando o método POST, para adicionar um endereço novo na lista, com ID automático.
@app.route('/enderecos/', methods=['POST'])
def adicionar_endereco(cep=None):
    if cep or request.is_json:
        if not cep:
            add_endereco = request.get_json()
            cep = add_endereco["cep"]
        cep = cep.replace("-", "").replace(".", "").replace(",", "").replace("/", "")
        if len(cep) == 8:
            viacep = requests.get(f"https://viacep.com.br/ws/{cep}/json/").json()
            if viacep == {'erro': True}:
                return "O CEP é inválido", 400
            else:
                con = get_db_connection()
                cursor = con.cursor()
                cursor.execute(
                    "INSERT INTO enderecos (cep, rua, bairro, cidade, estado, pais) VALUES (%s, %s, %s, %s, %s, %s)",
                    (cep, viacep['logradouro'], viacep['bairro'], viacep['localidade'], viacep['uf'], 'Brasil')
                )
                con.commit()
                cursor.close()
                con.close()
                return "Inserido com sucesso!", 201
        else:
            return "Cep precisa ter 8 dígitos", 400
    else:
        return "A requisição precisa ser enviada em JSON", 400

# Alterar endereço
# Aqui cria-se uma rota utilizando o método PUT, para alterar um endereço específico, via sua ID.
@app.route('/enderecos/<int:id>', methods=['PUT'])
def alterar_endereco(id):
    if request.is_json:
        add_endereco = request.get_json()
        cep = add_endereco["cep"]
        cep = cep.replace("-", "").replace(".", "").replace(",", "").replace("/", "")
        if len(cep) == 8:
            viacep = requests.get(f"https://viacep.com.br/ws/{cep}/json/").json()
            if viacep == {'erro': True}:
                return "O CEP é inválido", 400
            else:
                con = get_db_connection()
                cursor = con.cursor()
                cursor.execute(
                    "UPDATE enderecos SET cep = %s, rua = %s, bairro = %s, cidade = %s, estado = %s, pais = %s WHERE id = %s",
                    (cep, viacep['logradouro'], viacep['bairro'], viacep['localidade'], viacep['uf'], 'Brasil', id)
                )
                con.commit()
                cursor.close()
                con.close()
                return "Atualizado com sucesso!", 200
        else:
            return "Cep precisa ter 8 dígitos", 400
    else:
        return "A requisição precisa ser enviada em JSON", 400"

# # Deletar endereço
# Aqui cria-se uma rota utilizando o método DELETE, para deletar um endereço específico, via sua ID.
@app.route('/enderecos/<int:id>', methods=['DELETE'])
def deletar_endereco(id):
    con = get_db_connection()
    cursor = con.cursor()
    cursor.execute("DELETE FROM enderecos WHERE id = %s", (id,))
    con.commit()
    cursor.close()
    con.close()
    return "Deletado com sucesso!", 200

# Listar todas as entregas
@app.route('/entregas/', methods=['GET'])
def listar_entregas():
    con = get_db_connection()
    cursor = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT * FROM entregas")
    result = cursor.fetchall()
    cursor.close()
    con.close()
    return jsonify(result)

# Listar entrega por ID
@app.route('/entregas/<int:id>', methods=['GET'])
def listar_entrega(id):
    con = get_db_connection()
    cursor = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT * FROM entregas WHERE id = %s", (id,))
    result = cursor.fetchone()
    cursor.close()
    con.close()
    return jsonify(result)

# Adicionar entrega
# Aqui, o que foi feito de diferente é: Caso o CEP não exista já na tabela ENDEREÇOS, ele automaticamente o adiciona, para fazer a entrega.
@app.route('/entregas/', methods=['POST'])
def adicionar_entrega():
    if request.is_json:
        conteudo = request.get_json()
        cep = conteudo["cep"]
        cep = cep.replace("-", "").replace(".", "").replace(",", "").replace("/", "")
        if len(cep) == 8:
            if not listar_enderecos_cep(cep).json:
                retorno = adicionar_endereco(cep)
                if retorno[1] != 201:
                    return retorno
            con = get_db_connection()
            cursor = con.cursor()
            cursor.execute(
                "INSERT INTO entregas (cep, numero, complemento, qtd_pacotes, nome_remetente) VALUES (%s, %s, %s, %s, %s)",
                (cep, conteudo['numero'], conteudo['complemento'], conteudo['qtd_pacotes'], conteudo['nome_remetente'])
            )
            con.commit()
            cursor.close()
            con.close()
            return "Inserido com sucesso!", 201
        else:
            return "Cep precisa ter 8 dígitos", 400
    else:
        return "A requisição precisa ser enviada em JSON", 400

# Alterar entrega
@app.route('/entregas/<int:id>', methods=['PUT'])
def alterar_entrega(id):
    if request.is_json:
        conteudo = request.get_json()
        cep = conteudo["cep"]
        cep = cep.replace("-", "").replace(".", "").replace(",", "").replace("/", "")
        if len(cep) == 8:
            if not listar_enderecos_cep(cep).json:
                retorno = adicionar_endereco(cep)
                if retorno[1] != 201:
                    return retorno
            con = get_db_connection()
            cursor = con.cursor()
            cursor.execute(
                "UPDATE entregas SET cep = %s, numero = %s, complemento = %s, qtd_pacotes = %s, nome_remetente = %s WHERE id = %s",
                (cep, conteudo['numero'], conteudo['complemento'], conteudo['qtd_pacotes'], conteudo['nome_remetente'], id)
            )
            con.commit()
            cursor.close()
            con.close()
            return "Atualizado com sucesso!", 200
        else:
            return "Cep precisa ter 8 dígitos", 400
    else:
        return "A requisição precisa ser enviada em JSON", 400

# Deletar entrega
@app.route('/entregas/<int:id>', methods=['DELETE'])
def deletar_entrega(id):
    con = get_db_connection()
    cursor = con.cursor()
    cursor.execute("DELETE FROM entregas WHERE id = %s", (id,))
    con.commit()
    cursor.close()
    con.close()
    return "Deletado com sucesso!", 200

# Criar CSV

@app.route('/csv/', methods=['GET'])
def criar_csv():
    # Query para obter todos os endereços
    cursor = con.cursor()
    cursor.execute("SELECT * FROM enderecos")
    enderecos = cursor.fetchall()
    
    # Query para obter todas as entregas
@app.route('/csv/', methods=['GET'])
def criar_csv():
    con = get_db_connection()
    cursor = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # Query para obter todos os endereços
    cursor.execute("SELECT * FROM enderecos")
    enderecos = cursor.fetchall()
    
    # Query para obter todas as entregas
    cursor.execute("SELECT * FROM entregas")
    entregas = cursor.fetchall()
    
    cursor.close()
    con.close()
    
    # Criar DataFrames do Pandas
    df_enderecos = pd.DataFrame(enderecos, columns=['ID', 'CEP', 'Rua', 'Bairro', 'Cidade', 'Estado', 'Pais'])
    df_entregas = pd.DataFrame(entregas, columns=['ID', 'CEP', 'Numero', 'Complemento', 'Qtd_Pacotes', 'Nome_Remetente'])
    
    # Salvar DataFrames em CSV
    df_enderecos.to_csv('enderecos.csv', index=False)
    df_entregas.to_csv('entregas.csv', index=False)
    
    return "CSV gerado com sucesso!"

if __name__ == '__main__':
    app.run(debug=True)
    

# Aqui não apenas rodamos a API efetivamente, como o debug=True a mantém ativa, visto suas mudanças durante os testes do código.


app.run(debug=True)
