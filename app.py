import os
import sys
import threading
import json
import uuid
import time
import logging
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from flask import Flask, jsonify
from dotenv import load_dotenv

# Configura o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

load_dotenv()

# --- Configuração ---
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SQS_QUEUE_URL = os.getenv("AWS_SQS_URL")
DYNAMODB_TABLE_NAME = os.getenv("AWS_DYNAMODB_TABLE")
DYNAMODB_LOCAL_URL = os.getenv("AWS_DYNAMODB_LOCAL_URL") # Ex: http://dynamodb-local:8000 (apenas para dev)

if not all([SQS_QUEUE_URL, DYNAMODB_TABLE_NAME]):
    log.critical("Erro: AWS_SQS_URL e AWS_DYNAMODB_TABLE devem ser definidos.")
    sys.exit(1)

# --- Clientes Boto3 ---
try:
    session = boto3.Session(region_name=AWS_REGION)
    sqs_client = session.client("sqs")
    
    # Se houver URL local definida, usa (para dev). Caso contrário, usa o DynamoDB oficial da nuvem AWS.
    if DYNAMODB_LOCAL_URL:
        dynamodb_client = session.client("dynamodb", endpoint_url=DYNAMODB_LOCAL_URL)
        log.info(f"Conectado ao DynamoDB Local: {DYNAMODB_LOCAL_URL}")
    else:
        dynamodb_client = session.client("dynamodb")
        log.info(f"Conectado ao DynamoDB de Produção na região {AWS_REGION}")
        
except NoCredentialsError:
    # No EKS, se usar IAM Roles for Service Accounts (IRSA), o Boto3 se autentica sozinho sem chaves fixas!
    log.critical("Credenciais da AWS não encontradas. Garanta que a ServiceAccount possui a Role do IAM configurada.")
    sys.exit(1)
except Exception as e:
    log.critical(f"Erro ao inicializar o Boto3: {e}")
    sys.exit(1)


# --- SQS Worker ---

def process_message(message):
    """ Processa uma única mensagem SQS e a insere no DynamoDB """
    try:
        log.info(f"Processando mensagem ID: {message['MessageId']}")
        body = json.loads(message['Body'])
        
        event_id = str(uuid.uuid4())
        
        item = {
            'id': {'S': event_id},
            'USER': {'S': str(body['user_id'])},
            'mensagem': {'S': f"flag_evaluation:{body['flag_name']}:{body['result']}"},
            'flag_name': {'S': body['flag_name']},
            'result': {'BOOL': body['result']},
            'timestamp': {'S': body['timestamp']}
        }
        
        dynamodb_client.put_item(
            TableName=DYNAMODB_TABLE_NAME,
            Item=item
        )
        
        log.info(f"Evento {event_id} (Flag: {body['flag_name']}) salvo no DynamoDB.")
        
        # Sucesso absoluto: deleta a mensagem da fila
        sqs_client.delete_message(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=message['ReceiptHandle']
        )
        
    except json.JSONDecodeError:
        log.error(f"Poison Pill detectada! Erro ao decodificar JSON da mensagem ID: {message['MessageId']}. Descartando da fila.")
        # Se o formato é inválido, deletamos para não travar o worker em loop eterno
        sqs_client.delete_message(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=message['ReceiptHandle']
        )
    except ClientError as e:
        log.error(f"Erro do Boto3 (DynamoDB ou SQS) ao processar {message['MessageId']}: {e}")
    except Exception as e:
        log.error(f"Erro inesperado ao processar {message['MessageId']}: {e}")

def sqs_worker_loop():
    """ Loop principal do worker que ouve a fila SQS """
    log.info("Iniciando o worker SQS...")
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )
            
            messages = response.get('Messages', [])
            if not messages:
                continue
                
            log.info(f"Recebidas {len(messages)} mensagens.")
            
            for message in messages:
                process_message(message)
                
        except ClientError as e:
            log.error(f"Erro de comunicação com a AWS SQS: {e}")
            time.sleep(10) 
        except Exception as e:
            log.error(f"Erro inesperado no loop principal do SQS: {e}")
            time.sleep(10)

# --- Servidor Flask para Health Check (Kubernetes Probes) ---

app = Flask(__name__)

@app.route('/health')
def health():
    return jsonify({"status": "ok"})

def start_worker():
    worker_thread = threading.Thread(target=sqs_worker_loop, daemon=True)
    worker_thread.start()

# Inicia o worker em background
start_worker()

if __name__ == '__main__':
    port = int(os.getenv("PORT", 8005))
    app.run(host='0.0.0.0', port=port, debug=False)