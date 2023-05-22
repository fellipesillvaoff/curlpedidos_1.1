# Definir a imagem base
FROM python:3.9

# Definir o diretório de trabalho
WORKDIR /app

# Copiar os arquivos do projeto para o diretório de trabalho
COPY . /app

# Instalar as dependências
RUN pip install -r requirements.txt

# Definir o comando de execução
CMD ["python", "main.py"]
