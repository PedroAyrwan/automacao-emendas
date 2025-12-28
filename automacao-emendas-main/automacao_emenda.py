import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import smtplib
from email.mime.text import MIMEText
import time
import os
from dotenv import load_dotenv

# --- CARREGA AS SENHAS ---
load_dotenv()

# --- LIMPEZA DE SENHAS (Remove espaços invisíveis) ---
def limpar_senha(valor):
    if valor is None:
        return ""
    return str(valor).strip()

EMAIL_REMETENTE = limpar_senha(os.getenv("EMAIL_REMETENTE"))
SENHA_EMAIL = limpar_senha(os.getenv("SENHA_EMAIL"))
EMAIL_DESTINATARIO = limpar_senha(os.getenv("EMAIL_DESTINATARIO"))

# --- CONFIGURAÇÕES ---
URL_CSV = "https://www.tesourotransparente.gov.br/ckan/dataset/83e419da-1552-46bf-bfc3-05160b2c46c9/resource/66d69917-a5d8-4500-b4b2-ef1f5d062430/download/emendas-parlamentares.csv"
CREDENCIAIS_JSON = 'credentials.json'
MUNICIPIO_ALVO = "Canindé de São Francisco"
UF_ALVO = "SE"

def debug_secrets():
    """Mostra no log se as senhas foram lidas (SEM MOSTRAR A SENHA REAL)"""
    print("\n--- 🕵️ DIAGNÓSTICO DE SENHAS ---")
    print(f"1. Remetente: '{EMAIL_REMETENTE}' (Tamanho: {len(EMAIL_REMETENTE)})")
    print(f"2. Destinatário: '{EMAIL_DESTINATARIO}' (Tamanho: {len(EMAIL_DESTINATARIO)})")
    # Não mostramos a senha, apenas se ela existe
    tem_senha = "SIM" if len(SENHA_EMAIL) > 0 else "NÃO"
    print(f"3. Senha Configurada? {tem_senha}")
    print("----------------------------------\n")

def enviar_email(assunto, mensagem):
    try:
        # Debug antes de enviar
        if not EMAIL_REMETENTE or not SENHA_EMAIL:
            print("⚠️ Pulei o e-mail: Faltam configurações (Veja o diagnóstico acima).")
            return

        msg = MIMEText(mensagem, 'plain', 'utf-8')
        msg['Subject'] = assunto
        msg['From'] = EMAIL_REMETENTE
        msg['To'] = EMAIL_DESTINATARIO

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_REMETENTE, SENHA_EMAIL)
            server.send_message(msg)
        print(f"📧 E-mail enviado: {assunto}")
    except Exception as e:
        print(f"❌ Erro ao enviar e-mail: {str(e)}")

def executar_tarefa():
    # Roda o diagnóstico primeiro
    debug_secrets()
    
    print("--- 1. Baixando CSV... ---")
    df = pd.read_csv(URL_CSV, encoding='latin1', sep=';', on_bad_lines='skip')
    
    print(f"--- 2. Filtrando {MUNICIPIO_ALVO}... ---")
    coluna_municipio = 'Nome Ente' if 'Nome Ente' in df.columns else df.columns[0]
    coluna_uf = 'UF' if 'UF' in df.columns else df.columns[1]

    df_filtrado = df[(df[coluna_municipio] == MUNICIPIO_ALVO) & (df[coluna_uf] == UF_ALVO)]
    qtd_linhas = len(df_filtrado)
    print(f"✅ Linhas encontradas: {qtd_linhas}")

    print("--- 3. Google Sheets... ---")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENCIAIS_JSON, scope)
    client = gspread.authorize(creds)
    
    planilha = client.open("Robo_Caninde").sheet1 
    planilha.clear()
    planilha.update([df_filtrado.columns.values.tolist()] + df_filtrado.values.tolist())
    
    return qtd_linhas

# --- LOOP PRINCIPAL ---
debug_secrets() # Testa log logo no começo
try:
    total = executar_tarefa()
    enviar_email("✅ Sucesso Robô", f"Planilha atualizada com {total} linhas.")
    print("--- FIM: SUCESSO ---")
except Exception as e:
    print(f"❌ ERRO FATAL: {str(e)}")
    enviar_email("⚠️ Erro no Robô", f"Erro: {str(e)}")

