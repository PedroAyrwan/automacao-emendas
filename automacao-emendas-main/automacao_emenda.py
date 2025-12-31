import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import smtplib
from email.mime.text import MIMEText
import requests
from io import StringIO
from datetime import datetime
import os
from dotenv import load_dotenv

# --- CONFIGURAÇÕES INICIAIS ---
load_dotenv()

def limpar_senha(valor):
    if valor is None: return ""
    return str(valor).strip()

EMAIL_REMETENTE = limpar_senha(os.getenv("EMAIL_REMETENTE"))
SENHA_EMAIL = limpar_senha(os.getenv("SENHA_EMAIL"))
STRING_DESTINATARIOS = limpar_senha(os.getenv("EMAIL_DESTINATARIO"))

# Configurações do Google Sheets
CREDENCIAIS_JSON = 'credentials.json'
NOME_PLANILHA_GOOGLE = "Robo_Caninde"
LINK_PLANILHA = "https://docs.google.com/spreadsheets/d/1Do1s1cAMxeEMNyV87etGV5L8jxwAp4ermInaUR74bVs/edit?usp=sharing"

# --- LINKS FIXOS (Emendas e Receitas Gerais) ---
URL_EMENDAS = "https://www.tesourotransparente.gov.br/ckan/dataset/83e419da-1552-46bf-bfc3-05160b2c46c9/resource/66d69917-a5d8-4500-b4b2-ef1f5d062430/download/emendas-parlamentares.csv"
URL_RECEITAS_FIXO = "https://agtransparenciaserviceprd.agapesistemas.com.br/service/193/orcamento/receita/orcamentaria/rel?alias=pmcaninde&recursoDESO=false&filtro=1&ano=2025&mes=12&de=01-01-2025&ate=31-12-2025&covid19=false&lc173=false&consolidado=false&tipo=csv"

# --- FUNÇÃO: MONTAR URL DINÂMICA (APENAS PARA FOLHAS) ---
def montar_url_folha(servico_id, mes, ano):
    return f"https://agtransparenciarhserviceprd.agapesistemas.com.br/{servico_id}/rh/relatorios/relacao_vinculos_oc?regime=&matricula=&nome=&funcao=&mes={mes}&ano={ano}&total=10000&docType=csv"

# --- TAREFA: PROCESSAR FOLHA (COM LÓGICA DE DATA) ---
def processar_folha_dinamica(servico_id, nome_aba, planilha_google):
    agora = datetime.now()
    mes_atual = agora.month
    ano_atual = agora.year
    
    # 1. Tenta o mês atual do computador
    url = montar_url_folha(servico_id, mes_atual, ano_atual)
    qtd = executar_extracao_rh(url, nome_aba, planilha_google, mes_atual, ano_atual)
    
    # 2. Se falhar (vazio), recua um mês automaticamente
    if qtd == 0:
        mes_ant = 12 if mes_atual == 1 else mes_atual - 1
        ano_ant = ano_atual - 1 if mes_atual == 1 else ano_atual
        print(f"⚠️ Mês {mes_atual} indisponível para {nome_aba}. Tentando mês {mes_ant}...")
        url_ant = montar_url_folha(servico_id, mes_ant, ano_ant)
        qtd = executar_extracao_rh(url_ant, nome_aba, planilha_google, mes_ant, ano_ant)
    
    return qtd

def executar_extracao_rh(url, nome_aba, planilha_google, mes, ano):
    try:
        res = requests.get(url)
        linhas = res.content.decode('latin1').split('\n')
        dados = []
        cargo_atual = ""
        for linha in linhas:
            partes = [p.strip() for p in linha.split(';')]
            if len(partes) < 5: continue
            if len(partes) > 10 and partes[2] == "" and partes[10] != "":
                cargo_atual = partes[10]
                continue
            if len(partes) > 5 and partes[2] != "" and partes[4] != "":
                dados.append([partes[3], partes[4], partes[2], cargo_atual, partes[7], partes[9], partes[5], mes, ano])
        
        if not dados: return 0
        df = pd.DataFrame(dados, columns=["Matricula", "Nome", "CPF", "Cargo", "Vinculo", "Secretaria", "Admissao", "Mes_Ref", "Ano_Ref"])
        aba = planilha_google.worksheet(nome_aba)
        aba.clear()
        aba.update('A1', [df.columns.values.tolist()] + df.values.tolist())
        return len(df)
    except: return 0

# --- TAREFA: PROCESSAR RECEITAS (LINK FIXO) ---
def processar_receitas_estatico(url, nome_aba, planilha_google):
    try:
        res = requests.get(url)
        conteudo = res.content.decode('latin1')
        linhas = conteudo.split('\n')
        idx_inicio = -1
        for i, linha in enumerate(linhas):
            if linha.strip().startswith("ANO;"):
                idx_inicio = i
                break
        if idx_inicio == -1: idx_inicio = 5
        
        dados = []
        for linha in linhas[idx_inicio + 1:]:
            partes = linha.split(';')
            if len(partes) < 5: continue
            try:
                if not partes[0].strip().isdigit(): continue
                dados.append([partes[0], partes[2], partes[5], partes[6], partes[8], partes[9]])
            except: continue
        
        df = pd.DataFrame(dados, columns=['Ano', 'Codigo', 'Descricao', 'Previsto', 'Realizado', '%'])
        aba = planilha_google.worksheet(nome_aba)
        aba.clear()
        aba.update('A1', [df.columns.values.tolist()] + df.values.tolist())
        return len(df)
    except: return 0

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    print(f"🚀 Robô Iniciado: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENCIAIS_JSON, scope)
        client = gspread.authorize(creds).open(NOME_PLANILHA_GOOGLE)

        # 1. Processamento Dinâmico (Folhas)
        status_geral = processar_folha_dinamica("193", "folha_pagamento_geral", client)
        status_educa = processar_folha_dinamica("350", "folha_pagamento_educacao", client)
        status_saude = processar_folha_dinamica("300", "folha_pagamento_saude", client)

        # 2. Processamento Estático (Receitas e Emendas)
        status_receita = processar_receitas_estatico(URL_RECEITAS_FIXO, "Receitas_2025", client)
        
        # Emendas (Lógica Simples de CSV direto)
        df_emendas = pd.read_csv(URL_EMENDAS, encoding='latin1', sep=';')
        df_filt = df_emendas[(df_emendas['Nome Ente'] == "Canindé de São Francisco") & (df_emendas['UF'] == "SE")]
        aba_em = client.worksheet("emendas")
        aba_em.clear()
        aba_em.update('A1', [df_filt.columns.values.tolist()] + df_filt.values.tolist())

        print("✅ Execução concluída com sucesso!")

    except Exception as e:
        print(f"❌ Erro fatal: {e}")