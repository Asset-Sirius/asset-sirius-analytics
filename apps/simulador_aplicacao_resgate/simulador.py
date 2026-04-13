import time
import random
import csv
import io
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError

# =====================================================================
# CONFIGURAÇÕES
# =====================================================================
AWS_BUCKET_NAME = 'nome-do-seu-bucket-aqui'
AWS_REGION = 'us-east-1' # Altere para sua região se necessário
TEMPO_ESPERA_SEGUNDOS = 60*3 # Intervalo de geração (em segundos)

# Inicializa o cliente do S3 (A maquina precisa ter as credenciais ou IAM Role configurada)
try:
    s3_client = boto3.client('s3', region_name=AWS_REGION)
except Exception as e:
    print(f"Atenção: Não foi possível inicializar o S3 de imediato. Erro: {e}")

# IDs baseados nos dados estabelecidos do banco de dados mockado
CLIENTES_IDS = [1, 2, 3, 4, 5, 6, 7]
# Tuplas com (fk_classe, fk_subclasse) disponíveis no banco
SUBCLASSES_VALIDAS = [
    (1, 1), # Classe 1, Subclasse 1 (SIRIUS RF VAREJO)
    (1, 2), # Classe 1, Subclasse 2 (SIRIUS RF ALTA RENDA)
    (2, 3), # Classe 2, Subclasse 3 (SIRIUS RF MASTER)
    (3, 4), # Classe 3, Subclasse 4 (SIRIUS MULTIMERCADO GERAL)
    (4, 5)  # Classe 4, Subclasse 5 (TERCEIROS ACOES)
]

# =====================================================================
# FUNÇÕES
# =====================================================================
def gerar_registro():
    """Gera dados condizentes com a tabela aplicacao_resgate"""
    fk_cliente = random.choice(CLIENTES_IDS)
    
    fk_classe, fk_subclasse = random.choice(SUBCLASSES_VALIDAS)
    
    natureza_operacao = random.choice(['A', 'R']) # A - Aplicacao, R - Resgate
    valor = round(random.uniform(500.0, 500000.0), 2)
    data = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return [fk_cliente, fk_classe, fk_subclasse, natureza_operacao, valor, data]

def simular_e_enviar():
    """Cria a simulação como um buffer CSV e sobe para o S3"""
    print("Gerando simulacao de aplicacao/resgate...")
    registro = gerar_registro()
    
    # Nome do arquivo CSV (identificado por timestamp ex: aplicacao_resgate_20260412150500.csv)
    timestamp_str = datetime.now().strftime('%Y%m%d%H%M%S')
    file_name = f'aplicacao_resgate_{timestamp_str}.csv'
    
    # Criando o CSV em memoria (io.StringIO) para evitar escrita excessiva no disco local
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    
    # Cabecalho identico à estrutura da tabela
    writer.writerow(['fk_cliente', 'fk_classe', 'fk_subclasse', 'natureza_operacao', 'valor', 'data'])
    
    # Linha da simulação
    writer.writerow(registro)
    
    # Pega o conteúdo serializado (string)
    csv_content = csv_buffer.getvalue()

    print(f"Enviando {file_name} para o bucket '{AWS_BUCKET_NAME}'...")
    try:
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=f'simulacoes/{file_name}', # Coloca dentro da pasta simulacoes no bucket
            Body=csv_content
        )
        print(f"✓ Sucesso! Arquivo gravado em: s3://{AWS_BUCKET_NAME}/simulacoes/{file_name}")
    except NoCredentialsError:
        print("✗ Erro de Permissão: Credenciais da AWS nao encontradas. Rode 'aws configure' ou exporte as variaveis AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY.")
    except Exception as e:
        print(f"✗ Erro ao enviar para S3: {e}")

# =====================================================================
# LOOP PRINCIPAL
# =====================================================================
if __name__ == "__main__":
    print(f"--- Iniciando Simulador ASSET SIRIUS ---")
    print(f"Intervalo configurado: a cada {TEMPO_ESPERA_SEGUNDOS} segundos.\n")
    
    try:
        while True:
            simular_e_enviar()
            print(f"Aguardando próximo clico ({TEMPO_ESPERA_SEGUNDOS}s)...\n")
            time.sleep(TEMPO_ESPERA_SEGUNDOS)
    except KeyboardInterrupt:
        print("\nSimulador finalizado pelo usuario. (Ctrl+C pressionado).")
