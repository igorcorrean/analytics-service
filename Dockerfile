# --- Estágio 1: Build e Instalação de Dependências ---
FROM python:3.11-alpine AS builder

WORKDIR /app

# Instala dependências de compilação do sistema, se necessário
RUN apk add --no-cache gcc musl-dev linux-headers

# Copia e instala as dependências do Python no escopo do usuário
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# --- Estágio 2: Imagem Final de Execução ---
FROM python:3.11-alpine AS runner

WORKDIR /app

# Garante que os pacotes instalados no builder sejam encontrados
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONUNBUFFERED=1

# Copia apenas as dependências instaladas do estágio anterior
COPY --from=builder /root/.local /root/.local

# Copia o código-fonte da aplicação
COPY app.py .

# Expõe a porta do Health Check configurada no .env
EXPOSE 8005

# Comando para rodar a aplicação com Gunicorn utilizando threads para o Flask
CMD ["gunicorn", "--bind", "0.0.0.0:8005", "--threads", "4", "app:app"]