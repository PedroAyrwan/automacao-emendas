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

# --- LINKS (ATUALIZADO PARA 5000 PESSOAS) ---
LINK_PLANILHA = "https://docs.google.com/spreadsheets/d/1Do1s1cAMxeEMNyV87etGV5L8jxwAp4ermInaUR74bVs/edit?usp=sharing"

URL_EMENDAS = "https://www.tesourotransparente.gov.br/ckan/dataset/83e419da-1552-46bf-bfc3-05160b2c46c9/resource/66d69917-a5d8-4500-b4b2-ef1f5d062430/download/emendas-parlamentares.csv"
URL_RECEITAS = "https://agtransparenciaserviceprd.agapesistemas.com.br/service/193/orcamento/receita/orcamentaria/rel?alias=pmcaninde&recursoDESO=false&filtro=1&ano=2025&mes=12&de=01-01-2025&ate=31-12-2025&covid19=false&lc173=false&consolidado=false&tipo=csv"
# AQUI: Mudei total=300 para total=5000 para pegar todo mundo
URL_FOLHA = "https://agtransparenciarhserviceprd.agapesistemas.com.br/193/rh/relatorios/relacao_vinculos_oc?regime=&matricula=&nome=&funcao=&mes=11&ano=2025&total=5000&docType=csv"

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
    # Pula cabeçalho decorativo
    df = pd.read_csv(csv_data, sep=';', skiprows=4, on_bad_lines='skip')
    
    # Seleção de colunas específicas conforme estrutura Ágape
    df = df.iloc[:, [0, 2, 5, 6, 8, 9]]
    df.columns = ['Ano', 'Codigo', 'Descricao', 'Previsto', 'Realizado', '%']
    df = df.dropna(subset=['Descricao'])
    df = df[~df['Ano'].astype(str).str.contains('QUANTIDADE', na=False)].fillna("")
    
    aba = planilha_google.worksheet("Receitas_2025")
    aba.clear()
    aba.update('A1', [df.columns.values.tolist()] + df.values.tolist())
    return len(df)

def tarefa_folha(planilha_google):
    print("\n--- 3. Atualizando Folha de Pagamento (Correção Total)... ---")
    
    response = requests.get(URL_FOLHA)
    response.raise_for_status()
    conteudo_csv = response.content.decode('latin1')
    
    # 1. Encontrar a linha do primeiro cabeçalho
    temp_df = pd.read_csv(StringIO(conteudo_csv), sep=';', header=None, nrows=20, on_bad_lines='skip')
    linha_cabecalho = 0
    for index, row in temp_df.iterrows():
        linha_texto = row.astype(str).str.cat(sep=' ')
        if "Matrícula" in linha_texto or "ome" in linha_texto:
            linha_cabecalho = index
            break
            
    print(f"ℹ️ Cabeçalho detectado na linha: {linha_cabecalho}")
    
    # 2. Carregar CSV completo
    df = pd.read_csv(StringIO(conteudo_csv), sep=';', skiprows=linha_cabecalho, on_bad_lines='skip')
    
    # 3. MAPA DE TRADUÇÃO MELHORADO
    mapa_colunas = {
        "Matrícula": "Matricula",
        "CPF": "CPF",
        "ome": "Nome_Servidor",    
        "argo": "Cargo",           # Tenta pegar "Cargo" ou "argo"
        "inculo": "Vinculo",      
        "unção": "Funcao_Confianca", # Função de chefia
        "Admissão": "Data_Admissao",
        "Més": "Mes",
        "Ano": "Ano",
        "Salário Ba": "Salario_Base",
        "Remun. B": "Remun_Bruta",
        "Desc- Legais": "Descontos",
        "Valor Liq": "Valor_Liquido"
    }
    
    colunas_finais = []
    
    # Renomeia as colunas
    for coluna_csv in list(df.columns):
        coluna_limpa = coluna_csv.strip()
        nome_novo = None
        for chave_feia, valor_bonito in mapa_colunas.items():
            if chave_feia in coluna_limpa:
                nome_novo = valor_bonito
                break
        
        if nome_novo:
            df.rename(columns={coluna_csv: nome_novo}, inplace=True)
            colunas_finais.append(nome_novo)
    
    # Mantém só as colunas traduzidas
    if colunas_finais:
        df = df[colunas_finais]
    
    # 4. FILTRO ANTI-REPETIÇÃO (Remove cabeçalhos que aparecem no meio do arquivo)
    if 'Nome_Servidor' in df.columns:
        df = df[~df['Nome_Servidor'].astype(str).str.contains('Nome|ome', case=False, na=False)]
    
    if 'Matricula' in df.columns:
         df = df[~df['Matricula'].astype(str).str.contains('Matrícula', case=False, na=False)]

    # 5. Limpeza Final
    # Remove vazios baseados no Nome
    col_filtro = 'Nome_Servidor' if 'Nome_Servidor' in df.columns else df.columns[0]
    df = df.dropna(subset=[col_filtro])
    df = df.fillna("")
    
    # Envia para o Google Sheets
    nome_aba = "Folha_Pagamento"
    try:
        aba = planilha_google.worksheet(nome_aba)
    except:
        aba = planilha_google.add_worksheet(title=nome_aba, rows=6000, cols=20)
    
    aba.clear()
    dados_final = [df.columns.values.tolist()] + df.values.tolist()
    aba.update('A1', dados_final)
    
    print(f"✅ Aba '{nome_aba}' corrigida: {len(df)} servidores.")
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