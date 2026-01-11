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
import traceback
from datetime import datetime

# --- CONFIGURAÇÕES INICIAIS ---
load_dotenv()

def limpar_senha(valor):
    if valor is None: return ""
    return str(valor).strip()

EMAIL_REMETENTE = limpar_senha(os.getenv("EMAIL_REMETENTE"))
SENHA_EMAIL = limpar_senha(os.getenv("SENHA_EMAIL"))
STRING_DESTINATARIOS = limpar_senha(os.getenv("EMAIL_DESTINATARIO"))

# --- CONFIGURAÇÕES GOOGLE SHEETS ---
CREDENCIAIS_JSON = 'credentials.json'
NOME_PLANILHA_GOOGLE = "Robo_Caninde"
LINK_PLANILHA = "https://docs.google.com/spreadsheets/d/1Do1s1cAMxeEMNyV87etGV5L8jxwAp4ermInaUR74bVs/edit?usp=sharing"

# --- LINKS ESTÁTICOS ---
URL_EMENDAS = "https://www.tesourotransparente.gov.br/ckan/dataset/83e419da-1552-46bf-bfc3-05160b2c46c9/resource/66d69917-a5d8-4500-b4b2-ef1f5d062430/download/emendas-parlamentares.csv"
URL_RECEITAS_FIXO = "https://agtransparenciaserviceprd.agapesistemas.com.br/service/193/orcamento/receita/orcamentaria/rel?alias=pmcaninde&recursoDESO=false&filtro=1&ano=2025&mes=12&de=01-01-2025&ate=31-12-2025&covid19=false&lc173=false&consolidado=false&tipo=csv"

# --- FUNÇÃO DE CONVERSÃO ROBUSTA (String -> Float) ---
def converter_financeiro_folha(valor_str):
    """Limpa formatação brasileira e converte para float real."""
    if not valor_str: return 0.0
    v = str(valor_str).strip()
    if v in ["", "-", "0", "0,00"]: return 0.0
    try:
        # Remove pontos de milhar e troca vírgula por ponto
        limpo = v.replace('.', '').replace(',', '.')
        return float(limpo)
    except:
        return 0.0

# --- FUNÇÃO DE E-MAIL ---
def enviar_email(assunto, mensagem):
    if not EMAIL_REMETENTE or not SENHA_EMAIL: return
    lista_destinatarios = [e.strip() for e in STRING_DESTINATARIOS.split(',') if e.strip()]
    try:
        corpo_email = f"{mensagem}\n\n📊 Planilha: {LINK_PLANILHA}"
        msg = MIMEText(corpo_email, 'plain', 'utf-8')
        msg['Subject'] = assunto
        msg['From'] = EMAIL_REMETENTE
        msg['To'] = ", ".join(lista_destinatarios)
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(EMAIL_REMETENTE, SENHA_EMAIL)
            server.send_message(msg)
    except Exception as e: print(f"Erro e-mail: {e}")

# --- CONEXÃO GOOGLE ---
def conectar_google():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENCIAIS_JSON, scope)
    return gspread.authorize(creds).open(NOME_PLANILHA_GOOGLE)

# --- TAREFAS ESTÁTICAS ---
def tarefa_emendas(planilha_google):
    print("\n--- 1. Atualizando Emendas... ---")
    df = pd.read_csv(URL_EMENDAS, encoding='latin1', sep=';', on_bad_lines='skip')
    df_filtrado = df[(df['Nome Ente'] == "Canindé de São Francisco") & (df['UF'] == "SE")]
    aba = planilha_google.worksheet("emendas")
    aba.clear()
    aba.update('A1', [df_filtrado.columns.values.tolist()] + df_filtrado.values.tolist())
    return len(df_filtrado)

def processar_receitas(url_alvo, nome_aba, planilha_google):
    print(f"\n--- Processando Receitas: {nome_aba} ---")
    response = requests.get(url_alvo)
    linhas = response.content.decode('latin1').split('\n')
    idx_inicio = next((i for i, l in enumerate(linhas) if l.strip().startswith("ANO;")), 5)
    dados = []
    for linha in linhas[idx_inicio + 1:]:
        partes = linha.split(';')
        if len(partes) < 10: continue
        try:
            if not partes[0].strip().isdigit(): continue
            dados.append([partes[0].strip(), partes[2].strip(), partes[5].strip(), converter_financeiro_folha(partes[8]), converter_financeiro_folha(partes[9])])
        except: continue
    df = pd.DataFrame(dados, columns=['Ano', 'Codigo', 'Descricao', 'Previsto', 'Realizado'])
    aba = planilha_google.worksheet(nome_aba)
    aba.clear()
    aba.update('A1', [df.columns.values.tolist()] + df.values.tolist(), value_input_option='RAW')
    return len(df)

# ==========================================
#      EXTRAÇÃO DE RH (LÓGICA DINÂMICA)
# ==========================================

def executar_extracao_rh(url, nome_aba, planilha_google, ano_ref):
    print(f"   ↳ Verificando RH: {url}")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except: return 0
    
    conteudo = response.content.decode('latin1')
    linhas = conteudo.split('\n')
    dados_processados = []
    cargo_atual = ""
    ano_str = str(ano_ref) 
    
    for linha in linhas:
        partes = [p.strip() for p in linha.split(';')]
        if len(partes) < 5 or "CPF" in partes: continue
        
        # Detecta Cargo no cabeçalho do grupo
        if len(partes) > 10 and partes[2] == "" and partes[10] != "":
            cargo_atual = partes[10]
            continue
            
        # Linha do Servidor
        if len(partes) > 5 and partes[2] != "" and partes[4] != "":
            try:
                # Localiza a coluna do Ano para orientação
                idx_ano = -1
                for i, v in enumerate(partes):
                    if v == ano_str:
                        idx_ano = i
                        break
                if idx_ano == -1: continue

                # BUSCA DINÂMICA DE NÚMEROS: Pega tudo após o Ano e filtra apenas células com números/vírgulas
                sobra = partes[idx_ano + 1:]
                numeros_encontrados = [n for n in sobra if any(char.isdigit() for char in n) and ',' in n]

                if not numeros_encontrados: continue

                # Mapeamento por posição (Líquido é sempre o último, Desconto o penúltimo)
                val_base = converter_financeiro_folha(numeros_encontrados[0])
                val_bruto = converter_financeiro_folha(numeros_encontrados[1]) if len(numeros_encontrados) > 1 else 0.0
                val_desc = converter_financeiro_folha(numeros_encontrados[-2]) if len(numeros_encontrados) >= 2 else 0.0
                val_liq = converter_financeiro_folha(numeros_encontrados[-1])

                dados_processados.append({
                    "Matricula": partes[3], "Nome_Servidor": partes[4], "CPF": partes[2],
                    "Cargo": cargo_atual, "Vinculo": partes[7], "Secretaria": partes[9],
                    "Data_Admissao": partes[5], "Mes": partes[idx_ano - 1], "Ano": partes[idx_ano],
                    "Salario_Base": val_base, "Remun_Bruta": val_bruto,
                    "Descontos": val_desc, "Valor_Liquido": val_liq
                })
            except: continue

    if not dados_processados: return 0

    df = pd.DataFrame(dados_processados)
    try:
        aba = planilha_google.worksheet(nome_aba)
    except:
        aba = planilha_google.add_worksheet(title=nome_aba, rows=15000, cols=15)
    
    aba.clear()
    colunas = ["Matricula", "Nome_Servidor", "CPF", "Cargo", "Vinculo", "Secretaria", 
               "Data_Admissao", "Mes", "Ano", "Salario_Base", "Remun_Bruta", 
               "Descontos", "Valor_Liquido"]
    df = df.reindex(columns=colunas)
    
    # IMPORTANTE: value_input_option='RAW' permite que o gspread envie o float como número
    aba.update('A1', [df.columns.values.tolist()] + df.values.tolist(), value_input_option='RAW')
    return len(df)

def processar_folha_dinamica(servico_id, nome_aba, planilha_google):
    agora = datetime.now()
    m, a = agora.month, agora.year
    for _ in range(12):
        url = f"https://agtransparenciarhserviceprd.agapesistemas.com.br/{servico_id}/rh/relatorios/relacao_vinculos_oc?regime=&matricula=&nome=&funcao=&mes={m}&ano={a}&total=10000&docType=csv"
        qtd = executar_extracao_rh(url, nome_aba, planilha_google, a)
        if qtd > 0: 
            print(f"✅ Sucesso em {m}/{a} ({qtd} registros)")
            return qtd
        if m == 1: m, a = 12, a - 1
        else: m -= 1
    return 0

# --- EXECUÇÃO ---
if __name__ == "__main__":
    try:
        planilha = conectar_google()
        tarefa_emendas(planilha)
        processar_receitas(URL_RECEITAS_FIXO, "Receitas_2025", planilha)
        
        folhas = {"193": "folha_pagamento_geral", "350": "folha_pagamento_educacao", 
                  "300": "folha_pagamento_saude", "299": "folha_pagamento_social"}
        
        for srv_id, aba_nome in folhas.items():
            processar_folha_dinamica(srv_id, aba_nome, planilha)
            
        enviar_email("🤖 Robô Canindé: Sucesso", "Relatório atualizado com valores numéricos reais.")
    except Exception as e:
        print(f"Erro fatal: {e}")
        enviar_email("⚠️ Robô Canindé: Erro", f"Falha na execução: {str(e)}")