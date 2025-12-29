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

# Carrega as configurações do GitHub Secrets
EMAIL_REMETENTE = limpar_senha(os.getenv("EMAIL_REMETENTE")) # pedroayrwan2@gmail.com
SENHA_EMAIL = limpar_senha(os.getenv("SENHA_EMAIL"))         # KDWVLQJAIHAWLADU
STRING_DESTINATARIOS = limpar_senha(os.getenv("EMAIL_DESTINATARIO")) # Os 3 e-mails aqui

# --- LINKS ---
LINK_PLANILHA = "https://docs.google.com/spreadsheets/d/1Do1s1cAMxeEMNyV87etGV5L8jxwAp4ermInaUR74bVs/edit?usp=sharing"

URL_EMENDAS = "https://www.tesourotransparente.gov.br/ckan/dataset/83e419da-1552-46bf-bfc3-05160b2c46c9/resource/66d69917-a5d8-4500-b4b2-ef1f5d062430/download/emendas-parlamentares.csv"
URL_RECEITAS = "https://agtransparenciaserviceprd.agapesistemas.com.br/service/193/orcamento/receita/orcamentaria/rel?alias=pmcaninde&recursoDESO=false&filtro=1&ano=2025&mes=12&de=01-01-2025&ate=31-12-2025&covid19=false&lc173=false&consolidado=false&tipo=csv"

CREDENCIAIS_JSON = 'credentials.json'
NOME_PLANILHA_GOOGLE = "Robo_Caninde"

# --- FUNÇÃO DE E-MAIL (GMAIL PARA MÚLTIPLOS DESTINATÁRIOS) ---
def enviar_email(assunto, mensagem):
    if not EMAIL_REMETENTE or not SENHA_EMAIL:
        print("⚠️ Configurações de e-mail ausentes no GitHub.")
        return
    
    # Transforma a string de e-mails do segredo em uma lista real
    lista_destinatarios = [e.strip() for e in STRING_DESTINATARIOS.split(',') if e.strip()]
    
    try:
        corpo_email = f"{mensagem}\n\n📊 Acesse a planilha aqui: {LINK_PLANILHA}"
        
        msg = MIMEText(corpo_email, 'plain', 'utf-8')
        msg['Subject'] = assunto
        msg['From'] = EMAIL_REMETENTE
        msg['To'] = ", ".join(lista_destinatarios)
        
        # Conexão segura com Gmail
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_REMETENTE, SENHA_EMAIL)
            server.send_message(msg)
            
        print(f"📧 E-mail enviado com sucesso para: {lista_destinatarios}")
    except Exception as e:
        print(f"❌ Erro no envio do e-mail: {str(e)}")

# --- CONEXÃO COM GOOGLE SHEETS ---
def conectar_google():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENCIAIS_JSON, scope)
    client = gspread.authorize(creds)
    return client.open(NOME_PLANILHA_GOOGLE)

# --- TAREFA 1: ATUALIZAR EMENDAS ---
def tarefa_emendas(planilha_google):
    print("\n--- 1. Atualizando Emendas Parlamentares... ---")
    df = pd.read_csv(URL_EMENDAS, encoding='latin1', sep=';', on_bad_lines='skip')
    
    # Filtro para Canindé
    df_filtrado = df[(df['Nome Ente'] == "Canindé de São Francisco") & (df['UF'] == "SE")]
    linhas = len(df_filtrado)
    
    aba = planilha_google.worksheet("emendas")
    aba.clear()
    dados_final = [df_filtrado.columns.values.tolist()] + df_filtrado.values.tolist()
    aba.update('A1', dados_final)
    
    print(f"✅ Aba 'emendas' atualizada: {linhas} linhas.")
    return linhas

# --- TAREFA 2: ATUALIZAR RECEITAS (COM LIMPEZA DE CABEÇALHO) ---
def tarefa_receitas(planilha_google):
    print("\n--- 2. Atualizando Receitas (Limpando CSV Ágape)... ---")
    
    response = requests.get(URL_RECEITAS)
    response.raise_for_status()
    
    # Pula as 4 linhas iniciais de cabeçalho decorativo do sistema
    csv_data = StringIO(response.content.decode('latin1'))
    df = pd.read_csv(csv_data, sep=';', skiprows=4, on_bad_lines='skip')
    
    # Seleciona apenas as colunas com dados reais (remove colunas vazias intermediárias)
    # ANO, CÓDIGO (index 2), DESCRIÇÃO (index 5), PREVISTO, REALIZADO, %
    colunas_validas = [0, 2, 5, 6, 8, 9]
    df = df.iloc[:, colunas_validas]
    
    # Renomeia para ficar organizado na planilha
    df.columns = ['Ano', 'Codigo_Receita', 'Descricao', 'Previsto_R$', 'Realizado_R$', 'Percentual_%']
    
    # Limpeza final: remove linhas vazias e o rodapé "QUANTIDADE"
    df = df.dropna(subset=['Descricao'])
    df = df[~df['Ano'].astype(str).str.contains('QUANTIDADE', na=False)]
    df = df.fillna("")
    
    linhas = len(df)
    aba = planilha_google.worksheet("Receitas_2025")
    aba.clear()
    
    dados_final = [df.columns.values.tolist()] + df.values.tolist()
    aba.update('A1', dados_final)
    
    print(f"✅ Aba 'Receitas_2025' limpa e atualizada: {linhas} linhas.")
    return linhas

# --- EXECUÇÃO PRINCIPAL ---
try:
    planilha = conectar_google()
    res_emendas = tarefa_emendas(planilha)
    res_receitas = tarefa_receitas(planilha)
    
    resumo = f"Relatório de Sincronização:\n- Emendas: {res_emendas} registros\n- Receitas: {res_receitas} registros"
    enviar_email("✅ Robô Canindé: Planilha Atualizada", resumo)
    print("\n🚀 Sucesso Total!")

except Exception as e:
    print(f"❌ Erro crítico: {e}")
    enviar_email("❌ Robô Canindé: Falha na Execução", f"Ocorreu um erro durante a atualização:\n{str(e)}")