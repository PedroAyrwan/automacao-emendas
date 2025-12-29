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
STRING_DESTINATARIOS = limpar_senha(os.getenv("EMAIL_DESTINATARIO"))

# --- LINKS ---
LINK_PLANILHA = "https://docs.google.com/spreadsheets/d/1Do1s1cAMxeEMNyV87etGV5L8jxwAp4ermInaUR74bVs/edit?usp=sharing"

URL_EMENDAS = "https://www.tesourotransparente.gov.br/ckan/dataset/83e419da-1552-46bf-bfc3-05160b2c46c9/resource/66d69917-a5d8-4500-b4b2-ef1f5d062430/download/emendas-parlamentares.csv"
URL_RECEITAS = "https://agtransparenciaserviceprd.agapesistemas.com.br/service/193/orcamento/receita/orcamentaria/rel?alias=pmcaninde&recursoDESO=false&filtro=1&ano=2025&mes=12&de=01-01-2025&ate=31-12-2025&covid19=false&lc173=false&consolidado=false&tipo=csv"
URL_FOLHA = "https://agtransparenciarhserviceprd.agapesistemas.com.br/193/rh/relatorios/relacao_vinculos_oc?regime=&matricula=&nome=&funcao=&mes=11&ano=2025&total=10000&docType=csv"

CREDENCIAIS_JSON = 'credentials.json'
NOME_PLANILHA_GOOGLE = "Robo_Caninde"

# --- FUNÇÃO DE E-MAIL ---
def enviar_email(assunto, mensagem):
    if not EMAIL_REMETENTE or not SENHA_EMAIL:
        print("⚠️ Configurações de e-mail ausentes.")
        return
    lista_destinatarios = [e.strip() for e in STRING_DESTINATARIOS.split(',') if e.strip()]
    try:
        corpo_email = f"{mensagem}\n\n📊 Acesse a planilha aqui: {LINK_PLANILHA}"
        msg = MIMEText(corpo_email, 'plain', 'utf-8')
        msg['Subject'] = assunto
        msg['From'] = EMAIL_REMETENTE
        msg['To'] = ", ".join(lista_destinatarios)
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_REMETENTE, SENHA_EMAIL)
            server.send_message(msg)
        print(f"📧 E-mail enviado para: {lista_destinatarios}")
    except Exception as e:
        print(f"❌ Erro no e-mail: {str(e)}")

# --- CONEXÃO GOOGLE ---
def conectar_google():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENCIAIS_JSON, scope)
    return gspread.authorize(creds).open(NOME_PLANILHA_GOOGLE)

# --- TAREFAS ---
def tarefa_emendas(planilha_google):
    print("\n--- 1. Atualizando Emendas... ---")
    df = pd.read_csv(URL_EMENDAS, encoding='latin1', sep=';', on_bad_lines='skip')
    df_filtrado = df[(df['Nome Ente'] == "Canindé de São Francisco") & (df['UF'] == "SE")]
    
    aba = planilha_google.worksheet("emendas")
    aba.clear()
    aba.update('A1', [df_filtrado.columns.values.tolist()] + df_filtrado.values.tolist())
    return len(df_filtrado)

def tarefa_receitas(planilha_google):
    print("\n--- 2. Atualizando Receitas... ---")
    response = requests.get(URL_RECEITAS)
    csv_data = StringIO(response.content.decode('latin1'))
    df = pd.read_csv(csv_data, sep=';', skiprows=4, on_bad_lines='skip')
    
    df = df.iloc[:, [0, 2, 5, 6, 8, 9]]
    df.columns = ['Ano', 'Codigo', 'Descricao', 'Previsto', 'Realizado', '%']
    df = df.dropna(subset=['Descricao'])
    df = df[~df['Ano'].astype(str).str.contains('QUANTIDADE', na=False)].fillna("")
    
    aba = planilha_google.worksheet("Receitas_2025")
    aba.clear()
    aba.update('A1', [df.columns.values.tolist()] + df.values.tolist())
    return len(df)

# --- TAREFA 3: FOLHA (NOME DA ABA ALTERADO) ---
def tarefa_folha(planilha_google):
    print("\n--- 3. Atualizando Folha de Pagamento... ---")
    
    # Garante URL com 10.000
    url_final = URL_FOLHA
    if "total=10000" not in url_final:
        url_final = url_final.replace("total=300", "total=10000").replace("total=5000", "total=10000")
        if "total=10000" not in url_final: url_final += "&total=10000"
    
    print(f"🔗 Baixando dados...")
    response = requests.get(url_final)
    response.raise_for_status()
    
    conteudo = response.content.decode('latin1')
    linhas = conteudo.split('\n')
    
    dados_processados = []
    cargo_atual = "" 
    
    print(f"🔄 Processando {len(linhas)} linhas brutas...")
    
    for linha in linhas:
        partes = linha.split(';')
        
        # Pula linhas muito curtas
        if len(partes) < 11: continue
            
        partes = [p.strip() for p in partes]
        
        # 1. Ignora Cabeçalhos
        if partes[2] == "CPF" or "Matrícula" in partes[3]: continue
            
        # 2. Captura Cargo (Zebra)
        if partes[2] == "" and partes[10] != "":
            cargo_atual = partes[10]
            continue
            
        # 3. Captura Pessoa
        if partes[2] != "" and partes[4] != "":
            # Garante tamanho mínimo para usar índices negativos com segurança
            while len(partes) < 22: partes.append("")

            # ESTRATÉGIA: Índices Positivos para o Início (Nomes, CPF)
            # Índices Negativos para o Fim (Valores Financeiros) -> Mais seguro!
            
            pessoa = {
                "Matricula": partes[3],
                "Nome_Servidor": partes[4],
                "CPF": partes[2],
                "Cargo": cargo_atual,      
                "Vinculo": partes[7],      
                "Secretaria": partes[9],   
                "Data_Admissao": partes[5],
                
                # DAQUI PRA BAIXO CONTAMOS DE TRÁS PRA FRENTE
                "Mes": partes[-8],            # Era [14]
                "Ano": partes[-7],            # Era [15]
                "Salario_Base": partes[-6],   # Era [16]
                "Remun_Bruta": partes[-5],    # Era [17] (Pula o vazio -4)
                "Descontos": partes[-3],      # Era [19]
                "Valor_Liquido": partes[-2]   # Era [20]
            }
            dados_processados.append(pessoa)

    df = pd.DataFrame(dados_processados)
    
    if not df.empty:
        df = df[df["Nome_Servidor"] != ""]

    # --- AQUI ESTÁ A MUDANÇA ---
    nome_aba = "folha_pagamento_geral" 
    try:
        aba = planilha_google.worksheet(nome_aba)
    except:
        # Cria a aba se não existir
        aba = planilha_google.add_worksheet(title=nome_aba, rows=15000, cols=15)
    
    aba.clear()
    
    if not df.empty:
        dados_final = [df.columns.values.tolist()] + df.values.tolist()
        aba.update('A1', dados_final)
    
    print(f"✅ Aba '{nome_aba}' atualizada: {len(df)} servidores.")
    return len(df)

# --- EXECUÇÃO ---
try:
    planilha = conectar_google()
    res1 = tarefa_emendas(planilha)
    res2 = tarefa_receitas(planilha)
    res3 = tarefa_folha(planilha)
    
    resumo = f"Relatório Canindé:\n- Emendas: {res1}\n- Receitas: {res2}\n- Servidores na Folha: {res3}"
    enviar_email("✅ Robô Canindé: Tudo Atualizado", resumo)
    print("🚀 Sucesso total!")
except Exception as e:
    print(f"❌ Erro na execução: {e}")
    enviar_email("❌ Robô Canindé: Erro Crítico", str(e))