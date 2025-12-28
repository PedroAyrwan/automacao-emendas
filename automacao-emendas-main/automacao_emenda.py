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
# Recebe a string do GitHub: "email1@gmail.com,email2@hotmail.com"
STRING_DESTINATARIOS = limpar_senha(os.getenv("EMAIL_DESTINATARIO"))

# --- LINKS ---
LINK_PLANILHA = "https://docs.google.com/spreadsheets/d/1Do1s1cAMxeEMNyV87etGV5L8jxwAp4ermInaUR74bVs/edit?usp=sharing"

URL_EMENDAS = "https://www.tesourotransparente.gov.br/ckan/dataset/83e419da-1552-46bf-bfc3-05160b2c46c9/resource/66d69917-a5d8-4500-b4b2-ef1f5d062430/download/emendas-parlamentares.csv"
URL_RECEITAS = "https://agtransparenciaserviceprd.agapesistemas.com.br/service/193/orcamento/receita/orcamentaria/rel?alias=pmcaninde&recursoDESO=false&filtro=1&ano=2025&mes=12&de=01-01-2025&ate=31-12-2025&covid19=false&lc173=false&consolidado=false&tipo=csv"

CREDENCIAIS_JSON = 'credentials.json'
NOME_PLANILHA_GOOGLE = "Robo_Caninde"

# --- FUNÇÃO DE E-MAIL (CONFIGURADA PARA MÚLTIPLOS DESTINATÁRIOS) ---
def enviar_email(assunto, mensagem):
    if not EMAIL_REMETENTE or not SENHA_EMAIL:
        print("⚠️ Configurações de e-mail ausentes nos Secrets do GitHub.")
        return
    
    # Transforma a string de e-mails em uma lista real do Python
    lista_destinatarios = [e.strip() for e in STRING_DESTINATARIOS.split(',') if e.strip()]
    
    if not lista_destinatarios:
        print("⚠️ Nenhum e-mail de destino encontrado.")
        return

    try:
        corpo_email = f"{mensagem}\n\n📊 Acesse a planilha atualizada aqui: {LINK_PLANILHA}"
        
        msg = MIMEText(corpo_email, 'plain', 'utf-8')
        msg['Subject'] = assunto
        msg['From'] = EMAIL_REMETENTE
        msg['To'] = ", ".join(lista_destinatarios)
        
        # Conexão via Hotmail/Outlook (Porta 587 + STARTTLS)
        with smtplib.SMTP('smtp-mail.outlook.com', 587) as server:
            server.starttls()
            server.login(EMAIL_REMETENTE, SENHA_EMAIL)
            server.send_message(msg)
            
        print(f"📧 E-mail enviado com sucesso para: {lista_destinatarios}")
    except Exception as e:
        print(f"❌ Falha ao enviar e-mail: {str(e)}")

# --- CONEXÃO COM GOOGLE SHEETS ---
def conectar_google():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENCIAIS_JSON, scope)
    client = gspread.authorize(creds)
    return client.open(NOME_PLANILHA_GOOGLE)

# --- TAREFA 1: ATUALIZAR EMENDAS ---
def tarefa_emendas(planilha_google):
    print("\n--- 1. Processando Emendas Parlamentares... ---")
    df = pd.read_csv(URL_EMENDAS, encoding='latin1', sep=';', on_bad_lines='skip')
    
    municipio = "Canindé de São Francisco"
    uf = "SE"
    col_mun = 'Nome Ente' if 'Nome Ente' in df.columns else df.columns[0]
    col_uf = 'UF' if 'UF' in df.columns else df.columns[1]
    
    df_filtrado = df[(df[col_mun] == municipio) & (df[col_uf] == uf)]
    linhas = len(df_filtrado)
    
    nome_aba = "emendas"
    try:
        aba = planilha_google.worksheet(nome_aba)
    except:
        print(f"ℹ️ Criando aba '{nome_aba}'...")
        aba = planilha_google.add_worksheet(title=nome_aba, rows=1000, cols=20)
    
    aba.clear()
    dados_final = [df_filtrado.columns.values.tolist()] + df_filtrado.values.tolist()
    aba.update('A1', dados_final) # Ajuste de compatibilidade A1
    
    print(f"✅ Aba '{nome_aba}' atualizada: {linhas} linhas.")
    return linhas

# --- TAREFA 2: ATUALIZAR RECEITAS ---
def tarefa_receitas(planilha_google):
    print("\n--- 2. Processando Receitas 2025... ---")
    
    response = requests.get(URL_RECEITAS)
    response.raise_for_status()
    
    # Decodifica latin1 que é o padrão desses portais de transparência
    csv_data = StringIO(response.content.decode('latin1'))
    df = pd.read_csv(csv_data, sep=';', on_bad_lines='skip')
    linhas = len(df)
    
    nome_aba = "Receitas_2025"
    try:
        aba = planilha_google.worksheet(nome_aba)
    except:
        print(f"ℹ️ Criando aba '{nome_aba}'...")
        aba = planilha_google.add_worksheet(title=nome_aba, rows=2000, cols=20)
    
    aba.clear()
    df = df.fillna("") 
    dados_final = [df.columns.values.tolist()] + df.values.tolist()
    aba.update('A1', dados_final) # Ajuste de compatibilidade A1
    
    print(f"✅ Aba '{nome_aba}' atualizada: {linhas} linhas.")
    return linhas

# --- EXECUTAR TUDO ---
def executar_geral():
    planilha = conectar_google()
    res_emendas = tarefa_emendas(planilha)
    res_receitas = tarefa_receitas(planilha)
    return f"Emendas: {res_emendas} linhas | Receitas: {res_receitas} linhas"

# --- LOOP DE EXECUÇÃO COM RE-TENTATIVA ---
MAX_TENTATIVAS = 5
tentativa = 1

while tentativa <= MAX_TENTATIVAS:
    try:
        print(f"🔄 Tentativa {tentativa} de {MAX_TENTATIVAS}...")
        relatorio = executar_geral()
        enviar_email("✅ Robô Canindé: Sucesso na Atualização", 
                     f"O robô completou a tarefa com sucesso.\n\nDetalhes:\n{relatorio}")
        print("\n--- PROCESSO CONCLUÍDO COM SUCESSO ---")
        break
    except Exception as e:
        print(f"❌ Erro detectado: {e}")
        if tentativa == MAX_TENTATIVAS:
            enviar_email("❌ Robô Canindé: Erro Crítico", 
                         f"O robô falhou após 5 tentativas.\n\nÚltimo erro: {str(e)}")
        else:
            print("Aguardando 60 segundos para tentar novamente...")
            time.sleep(60)
        tentativa += 1