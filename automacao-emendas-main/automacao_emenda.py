import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import smtplib
from email.mime.text import MIMEText
import time
import os
import requests
from io import StringIO
from dotenv import load_dotenv

# --- CONFIGURAÇÕES INICIAIS ---
load_dotenv()

def limpar_senha(valor):
    if valor is None: return ""
    return str(valor).strip()

EMAIL_REMETENTE = limpar_senha(os.getenv("EMAIL_REMETENTE"))
SENHA_EMAIL = limpar_senha(os.getenv("SENHA_EMAIL"))
EMAIL_DESTINATARIO = limpar_senha(os.getenv("EMAIL_DESTINATARIO"))

# --- LINKS DOS ARQUIVOS ---
URL_EMENDAS = "https://www.tesourotransparente.gov.br/ckan/dataset/83e419da-1552-46bf-bfc3-05160b2c46c9/resource/66d69917-a5d8-4500-b4b2-ef1f5d062430/download/emendas-parlamentares.csv"
URL_RECEITAS = "https://agtransparenciaserviceprd.agapesistemas.com.br/service/193/orcamento/receita/orcamentaria/rel?alias=pmcaninde&recursoDESO=false&filtro=1&ano=2025&mes=12&de=01-01-2025&ate=31-12-2025&covid19=false&lc173=false&consolidado=false&tipo=csv"

CREDENCIAIS_JSON = 'credentials.json'
NOME_PLANILHA_GOOGLE = "Robo_Caninde"

# --- FUNÇÃO DE E-MAIL ---
def enviar_email(assunto, mensagem):
    if not EMAIL_REMETENTE or not SENHA_EMAIL:
        print("⚠️ E-mail não configurado. Pulei o envio.")
        return
    try:
        msg = MIMEText(mensagem, 'plain', 'utf-8')
        msg['Subject'] = assunto
        msg['From'] = EMAIL_REMETENTE
        msg['To'] = EMAIL_DESTINATARIO
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_REMETENTE, SENHA_EMAIL)
            server.send_message(msg)
        print(f"📧 E-mail enviado: {assunto}")
    except Exception as e:
        print(f"❌ Erro no e-mail: {str(e)}")

# --- CONEXÃO COM GOOGLE SHEETS ---
def conectar_google():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENCIAIS_JSON, scope)
    client = gspread.authorize(creds)
    return client.open(NOME_PLANILHA_GOOGLE)

# --- TAREFA 1: ATUALIZAR EMENDAS (Aba "emendas") ---
def tarefa_emendas(planilha_google):
    print("\n--- 1. Processando Emendas Parlamentares... ---")
    df = pd.read_csv(URL_EMENDAS, encoding='latin1', sep=';', on_bad_lines='skip')
    
    municipio = "Canindé de São Francisco"
    uf = "SE"
    col_mun = 'Nome Ente' if 'Nome Ente' in df.columns else df.columns[0]
    col_uf = 'UF' if 'UF' in df.columns else df.columns[1]
    
    df_filtrado = df[(df[col_mun] == municipio) & (df[col_uf] == uf)]
    linhas = len(df_filtrado)
    
    # MUDANÇA AQUI: Busca pela aba "emendas"
    nome_aba = "emendas"
    try:
        aba = planilha_google.worksheet(nome_aba)
    except:
        print(f"ℹ️ Aba '{nome_aba}' não encontrada. Criando nova...")
        aba = planilha_google.add_worksheet(title=nome_aba, rows=1000, cols=20)
    
    aba.clear()
    aba.update([df_filtrado.columns.values.tolist()] + df_filtrado.values.tolist())
    print(f"✅ Aba '{nome_aba}' atualizada: {linhas} linhas.")
    return linhas

# --- TAREFA 2: ATUALIZAR RECEITAS (Aba "Receitas_2025") ---
def tarefa_receitas(planilha_google):
    print("\n--- 2. Processando Receitas 2025... ---")
    
    response = requests.get(URL_RECEITAS)
    response.raise_for_status()
    
    csv_data = StringIO(response.content.decode('latin1'))
    df = pd.read_csv(csv_data, sep=';', on_bad_lines='skip')
    
    linhas = len(df)
    
    nome_aba = "Receitas_2025"
    try:
        aba = planilha_google.worksheet(nome_aba)
    except:
        print(f"ℹ️ Criando nova aba: {nome_aba}")
        aba = planilha_google.add_worksheet(title=nome_aba, rows=2000, cols=20)
    
    aba.clear()
    df = df.fillna("") 
    aba.update([df.columns.values.tolist()] + df.values.tolist())
    
    print(f"✅ Aba '{nome_aba}' atualizada: {linhas} linhas.")
    return linhas

# --- EXECUTAR TUDO ---
def executar_geral():
    planilha = conectar_google()
    qtd_emendas = tarefa_emendas(planilha)
    qtd_receitas = tarefa_receitas(planilha)
    return f"Aba 'emendas': {qtd_emendas} | Aba 'Receitas_2025': {qtd_receitas}"

# --- LOOP PRINCIPAL ---
MAX_TENTATIVAS = 5
tentativa = 1

while tentativa <= MAX_TENTATIVAS:
    try:
        print(f"🔄 Rodada {tentativa}/{MAX_TENTATIVAS}...")
        resumo = executar_geral()
        enviar_email("✅ Robô Canindé: Sucesso", f"Resumo:\n{resumo}")
        print("\n--- SUCESSO TOTAL ---")
        break
    except Exception as e:
        print(f"❌ Erro: {e}")
        if tentativa == MAX_TENTATIVAS:
            enviar_email("❌ Falha Robô", f"Erro: {str(e)}")
        else:
            time.sleep(60)
        tentativa += 1