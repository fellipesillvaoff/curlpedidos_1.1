import mysql.connector
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

def enviar_email():
    email_de = 'site@clubedoeletronico.com'  # E-mail remetente
    senha = 'Mestrado2020*'  # Senha do e-mail remetente
    email_para = 'sacclubedoeletronico@gmail.com'  # E-mail destinatário
    assunto = 'Atualização banco de dados Processada'  # Assunto do e-mail
    corpo = 'O processamento do pedido foi concluído com sucesso. \nEsta é uma mensagem automática programada pelo banco de dados.'

    # Criação do corpo do e-mail
    mensagem = MIMEMultipart()
    mensagem['From'] = email_de
    mensagem['To'] = email_para
    mensagem['Subject'] = assunto

    # Adiciona o conteúdo do e-mail com a data e hora atual
    data_atual = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    texto = f"{corpo}\n\nData e hora atual: {data_atual}"
    conteudo = MIMEText(texto, 'plain')
    mensagem.attach(conteudo)

    # Envio do e-mail
    try:
        # Configuração do servidor SMTP
        servidor_smtp = smtplib.SMTP('smtp.hostinger.com', 587)  # Altere para as configurações do seu servidor SMTP
        servidor_smtp.starttls()
        servidor_smtp.login(email_de, senha)

        # Envio do e-mail
        servidor_smtp.send_message(mensagem)
        print('E-mail enviado com sucesso!')
    except Exception as e:
        print('Ocorreu um erro ao enviar o e-mail:', str(e))
    finally:
        # Encerra a conexão com o servidor SMTP
        servidor_smtp.quit()


# Função para verificar se a tabela existe
def table_exists(cursor, table_name):
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    return cursor.fetchone() is not None


# Função para processar uma página de pedidos
def processar_pagina(pedidos):
    # Conectar ao banco de dados
    cnx = mysql.connector.connect(
        host="containers-us-west-127.railway.app",
        port='5810',
        user="root",
        password="SVMoPcKC8ybeb16LhjXz",
        database="railway"
    )

    # Criar um cursor para executar as consultas
    cursor = cnx.cursor()

    # Verificar se a tabela existe e criá-la, se necessário
    table_name = "bling_pedidos"

    if not table_exists(cursor, table_name):
        create_table_query = f"CREATE TABLE {table_name} (numero_pedido INT PRIMARY KEY, numero_pedido_loja VARCHAR(255), vendedor VARCHAR(255), desconto VARCHAR(255), observacoes VARCHAR(255), observacao_interna VARCHAR(255), data_pedido DATE, valor_frete FLOAT, total_produtos FLOAT, total_venda FLOAT, situacao VARCHAR(255), loja VARCHAR(255), data_prevista DATE, tipo_integracao VARCHAR(255), cliente_nome VARCHAR(255), cliente_cnpj VARCHAR(255), endereco VARCHAR(255), numero_endereco VARCHAR(255), cidade VARCHAR(255), uf VARCHAR(255), ie VARCHAR(255), rg VARCHAR(255), numero_cliente VARCHAR(255), complemento VARCHAR(255), bairro VARCHAR(255), cep VARCHAR(255), email VARCHAR(255), celular VARCHAR(20), fone VARCHAR(255))"
        cursor.execute(create_table_query)
        cnx.commit()
        print(f"A tabela '{table_name}' foi criada com sucesso.")

    last_entry_query = f"SELECT numero_pedido FROM {table_name} ORDER BY numero_pedido DESC LIMIT 1"
    cursor.execute(last_entry_query)
    last_entry = cursor.fetchone()

    # Inserir apenas as entradas que não estão na tabela
    insert_query = f"INSERT INTO {table_name} (numero_pedido, numero_pedido_loja, vendedor, desconto, observacoes, observacao_interna, data_pedido, valor_frete, total_produtos, total_venda, situacao, loja, data_prevista, tipo_integracao, cliente_nome, cliente_cnpj, endereco, numero_endereco, cidade, uf, ie, rg, numero_cliente, complemento, bairro, cep, email, celular, fone) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    for pedido in pedidos:
        numero_pedido = int(pedido['pedido']['numero'])
        numero_pedido_loja = pedido['pedido'].get('numeroPedidoLoja')
        vendedor = pedido['pedido']['vendedor']
        desconto = pedido['pedido'].get('desconto')
        observacoes = pedido['pedido']['observacoes']
        observacao_interna = pedido['pedido']['observacaointerna']
        data_pedido = pedido['pedido']['data']
        valor_frete = pedido['pedido']['valorfrete']
        total_produtos = pedido['pedido']['totalprodutos']
        total_venda = pedido['pedido']['totalvenda']
        situacao = pedido['pedido']['situacao']
        loja = pedido['pedido'].get('loja')
        data_prevista = pedido['pedido'].get('dataPrevista')
        tipo_integracao = pedido['pedido'].get('tipoIntegracao')
        cliente_nome = pedido['pedido']['cliente']['nome']
        cliente_cnpj = pedido['pedido']['cliente']['cnpj']
        endereco = pedido['pedido']['cliente']['endereco']
        numero_endereco = pedido['pedido']['cliente']['numero']
        cidade = pedido['pedido']['cliente']['cidade']
        uf = pedido['pedido']['cliente']['uf']
        ie = pedido['pedido']['cliente']['ie']
        rg = pedido['pedido']['cliente']['rg']
        numero_cliente = pedido['pedido']['cliente'].get('numeroCliente')
        complemento = pedido['pedido']['cliente']['complemento']
        bairro = pedido['pedido']['cliente']['bairro']
        cep = pedido['pedido']['cliente']['cep']
        email = pedido['pedido']['cliente']['email']
        celular = pedido['pedido']['cliente']['celular']
        fone = pedido['pedido']['cliente']['fone']

        if last_entry is not None and last_entry[0] is not None and numero_pedido <= int(last_entry[0]):
            continue

        cursor.execute(insert_query, (
            numero_pedido, numero_pedido_loja, vendedor, desconto, observacoes, observacao_interna, data_pedido,
            valor_frete,
            total_produtos, total_venda, situacao, loja, data_prevista, tipo_integracao, cliente_nome, cliente_cnpj,
            endereco, numero_endereco, cidade, uf,
            ie, rg, numero_cliente, complemento, bairro, cep, email, celular, fone))
        cnx.commit()

    # Enviar e-mail de notificação
    enviar_email()


# Função para obter pedidos do Bling API
def obter_pedidos():
    # Chave da API do Bling
    api_key = '36c578d5a5cabae531eb8b58e18077e2a7b1c28508d3a56794e0ec82dc02e787fa83e1a5'

    # Obtém a data atual e a data de 7 dias atrás
    data_atual = datetime.now()
    data_7_dias_atras = data_atual - timedelta(days=7)

    # Formata as datas no formato "DD/MM/AAAA"
    data_atual_formatada = data_atual.strftime('%d/%m/%Y')
    #data_atual_formatada=('22/05/2023')
    data_7_dias_atras_formatada = data_7_dias_atras.strftime('%d/%m/%Y')
    #data_7_dias_atras_formatada=('26/12/2022')

    # Parâmetros da requisição
    params = {
        'apikey': api_key,
        'filters': f'dataEmissao[{data_7_dias_atras_formatada} TO {data_atual_formatada}]'
    }
    print(params)
    # URL da API para obter pedidos
    url = 'https://bling.com.br/Api/v2/pedidos/json'

    # Faz uma requisição GET para a API do Bling com os parâmetros
    response = requests.get(url, params=params)

    # Verifica se a requisição foi bem-sucedida
    if response.status_code == 200:
        data = response.json()

        # Verifica se existem pedidos na resposta
        if 'pedidos' in data['retorno']:
            pedidos = data['retorno']['pedidos']

            # Processa a página de pedidos
            processar_pagina(pedidos)
        else:
            print('Não há pedidos para processar.')
    else:
        print('Ocorreu um erro ao obter os pedidos:', response.status_code)


# Executa a função principal
obter_pedidos()
