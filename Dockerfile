# Arquivo: Dockerfile

# 1. Use uma imagem base oficial do Python
FROM python:3.10-slim

# 2. Defina variáveis de ambiente para Python
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# 3. Defina o diretório de trabalho dentro do container
WORKDIR /app

# 4. Copie o arquivo de requisitos
COPY requirements.txt .

# 5. Instale os pacotes
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 6. Copie todo o código da sua aplicação (main.py)
COPY . .

# 7. O Cloud Run define a variável $PORT. 8080 é um fallback.
ENV PORT 8080

# 8. Comando para iniciar sua aplicação FastAPI
CMD uvicorn main:app --host 0.0.0.0 --port $PORT