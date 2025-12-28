import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import smtplib
from email.mime.text import MIMEText
import time
import os
from dotenv import load_dotenv

# --- CARREGA AS SENHAS DO ARQUIVO .ENV (SEGURANÇA) ---
load_dotenv()

# --- CONFIGURAÇÕES DE E-MAIL (Blindadas) ---
EMAIL_REMETENTE = os.getenv("EMAIL_REMETENTE")
SENHA_EMAIL = os.getenv("SENHA_EMAIL")
EMAIL_DESTINATARIO = os.getenv("EMAIL_DESTINATARIO")

# --- CONFIGURAÇÕES DO PROJETO ---
# Link ATUALIZADO (CSV do Tesouro Transparente)
URL_CSV = "https://www.tesourotransparente.gov.br/ckan/dataset/83e419da-1552-46bf-bfc3-05160b2c46c9/resource/66d69917-a5d8-4500-b4b2-ef1f5d062430/download/emendas-parlamentares.csv"

CREDENCIAIS_JSON = 'credentials.json'
MUNICIPIO_ALVO = "Canindé de São Francisco"
UF_ALVO = "SE"

def enviar_email(assunto, mensagem):
    """Envia e-mail avisando sobre sucesso ou erro"""
    try:
        msg = MIMEText(mensagem)
        msg['Subject'] = assunto
        msg['From'] = EMAIL_REMETENTE
        msg['To'] = EMAIL_DESTINATARIO

        # Conexão segura com o Gmail
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_REMETENTE, SENHA_EMAIL)
            server.send_message(msg)
        print(f"📧 E-mail de aviso enviado: {assunto}")
    except Exception as e:
        # Aqui usamos str(e) para evitar o erro de bytes
        print(f"❌ Não foi possível enviar e-mail: {str(e)}")

def executar_tarefa():
    """Baixa os dados, filtra e atualiza a planilha"""
    print("--- 1. Baixando e Lendo CSV do Governo (Isso pode demorar)... ---")
    
    # settings para ler o CSV brasileiro (latin1 e ponto e vírgula)
    # on_bad_lines='skip' ignora linhas quebradas que o governo às vezes deixa
    df = pd.read_csv(URL_CSV, encoding='latin1', sep=';', on_bad_lines='skip')
    
    print(f"--- 2. Filtrando dados para: {MUNICIPIO_ALVO} - {UF_ALVO}... ---")
    
    # Filtro inteligente: verifica se as colunas existem antes de filtrar
    coluna_municipio = 'Nome Ente' if 'Nome Ente' in df.columns else df.columns[0] # Tenta adivinhar
    coluna_uf = 'UF' if 'UF' in df.columns else df.columns[1]

    df_filtrado = df[
        (df[coluna_municipio] == MUNICIPIO_ALVO) & 
        (df[coluna_uf] == UF_ALVO)
    ]
    
    qtd_linhas = len(df_filtrado)
    print(f"✅ Linhas encontradas: {qtd_linhas}")

    print("--- 3. Enviando para o Google Sheets... ---")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENCIAIS_JSON, scope)
    client = gspread.authorize(creds)
    
    # Pega a primeira planilha disponível na conta do robô
    planilha = client.open_all()[0].sheet1 
    
    # Limpa o que tinha antes e coloca os dados novos
    planilha.clear()
    # Adiciona o cabeçalho + os dados
    planilha.update([df_filtrado.columns.values.tolist()] + df_filtrado.values.tolist())
    
    return qtd_linhas

# --- LOOP DE BLINDAGEM (Tenta 5 vezes se der erro) ---
MAX_TENTATIVAS = 5
tentativa = 1

while tentativa <= MAX_TENTATIVAS:
    try:
        print(f"\n🔄 Tentativa {tentativa} de {MAX_TENTATIVAS}...")
        
        # Roda a função principal
        total = executar_tarefa()
        
        # Se chegou aqui, deu certo!
        msg_sucesso = f"O robô rodou com sucesso!\n\nMunicípio: {MUNICIPIO_ALVO}\nLinhas atualizadas: {total}"
        enviar_email("✅ Sucesso: Planilha Atualizada", msg_sucesso)
        print("\n--- FIM: Processo concluído com sucesso ---")
        break # Sai do loop e encerra o script
        
    except Exception as e:
        # Se der erro (Internet, Site do governo fora, etc)
        # CORREÇÃO CRÍTICA: Convertemos 'e' para string com str(e)
        erro_texto = str(e)
        print(f"❌ Erro na tentativa {tentativa}: {erro_texto}")
        
        msg_erro = f"Ocorreu um erro ao tentar atualizar a planilha:\n\n{erro_texto}\n\nVou tentar de novo em 1 hora..."
        enviar_email(f"⚠️ Alerta de Erro (Tentativa {tentativa})", msg_erro)
        
        if tentativa == MAX_TENTATIVAS:
            print("❌ Todas as tentativas falharam.")
            enviar_email("❌ FALHA CRÍTICA", "O robô tentou 5 vezes e não conseguiu atualizar os dados.")
        else:
            print("⏳ Aguardando 1 hora antes de tentar de novo...")
            time.sleep(3600) # 3600 segundos = 1 hora
            
        tentativa += 1