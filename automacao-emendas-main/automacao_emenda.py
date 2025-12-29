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
URL_FOLHA = "https://agtransparenciarhserviceprd.agapesistemas.com.br/193/rh/relatorios/relacao_vinculos_oc?regime=&matricula=&nome=&funcao=&mes=11&ano=2025&total=99&docType=csv"

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
    print("\n--- 3. Atualizando Folha de Pagamento (Filtro e Correção)... ---")
    
    response = requests.get(URL_FOLHA)
    response.raise_for_status()
    conteudo_csv = response.content.decode('latin1')
    
    # 1. Encontrar a linha do cabeçalho automaticamente
    temp_df = pd.read_csv(StringIO(conteudo_csv), sep=';', header=None, nrows=20, on_bad_lines='skip')
    linha_cabecalho = 0
    for index, row in temp_df.iterrows():
        linha_texto = row.astype(str).str.cat(sep=' ')
        # Procura por palavras-chave mesmo que estejam cortadas no CSV original
        if "Matrícula" in linha_texto or "ome" in linha_texto:
            linha_cabecalho = index
            break
            
    print(f"ℹ️ Cabeçalho encontrado na linha: {linha_cabecalho}")
    
    # 2. Carregar o CSV pulando as linhas iniciais inúteis
    df = pd.read_csv(StringIO(conteudo_csv), sep=';', skiprows=linha_cabecalho, on_bad_lines='skip')
    
    # 3. MAPA DE TRADUÇÃO
    mapa_colunas = {
        "Matrícula": "Matricula",
        "CPF": "CPF",
        "ome": "Nome_Servidor",    # Pega "Nome" ou "ome"
        "inculo": "Vinculo",       # Pega "Vinculo" ou "inculo"
        "unção": "Cargo_Funcao",   # Pega "Função" ou "unção"
        "Admissão": "Data_Admissao",
        "Més": "Mes",
        "Ano": "Ano",
        "Salário Ba": "Salario_Base",
        "Remun. B": "Remun_Bruta",
        "Desc- Legais": "Descontos",
        "Valor Liq": "Valor_Liquido"
    }
    
    colunas_finais = []
    
    # Varre as colunas originais e traduz usando o mapa
    for coluna_csv in list(df.columns): # list() cria uma cópia segura para iterar
        coluna_limpa = coluna_csv.strip()
        
        nome_novo = None
        for chave_feia, valor_bonito in mapa_colunas.items():
            if chave_feia in coluna_limpa:
                nome_novo = valor_bonito
                break
        
        if nome_novo:
            df.rename(columns={coluna_csv: nome_novo}, inplace=True)
            colunas_finais.append(nome_novo)
    
    # 4. Mantém apenas as colunas que conseguimos traduzir
    if colunas_finais:
        df = df[colunas_finais]
        print(f"✅ Colunas processadas: {colunas_finais}")
    else:
        print("⚠️ Atenção: Nenhuma coluna conhecida foi identificada.")

    # 5. Remove linhas vazias baseando-se na primeira coluna disponível
    col_filtro = 'Nome_Servidor' if 'Nome_Servidor' in df.columns else df.columns[0]
    df = df.dropna(subset=[col_filtro])
    df = df.fillna("")
    
    # Envia para o Google Sheets
    nome_aba = "Folha_Pagamento"
    try:
        aba = planilha_google.worksheet(nome_aba)
    except:
        aba = planilha_google.add_worksheet(title=nome_aba, rows=5000, cols=15)
    
    aba.clear()
    dados_final = [df.columns.values.tolist()] + df.values.tolist()
    aba.update('A1', dados_final)
    
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