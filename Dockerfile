# Usando uma imagem Python oficial como base
FROM python:3.9-slim

# Definir o diretório de trabalho
WORKDIR /app

# Copiar requirements.txt e instalar dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o restante do código
COPY . .

# Comando padrão
CMD ["python", "main.py"]
