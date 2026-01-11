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

# --- FUNÇÃO DE CONVERSÃO EXCLUSIVA PARA FOLHA (String -> Float) ---
def limpar_cifrao(valor_str):
    """
    Remove o prefixo 'R$ ' e limpa a string para que o Google Sheets
    reconheça como número (mantendo o formato brasileiro de pontos e vírgulas).
    """
    if not valor_str or str(valor_str).strip() in ["", "-"]:
        return 0.0
    try:
        # Remove o 'R$ ' e espaços, permitindo que o Sheets interprete o resto como número
        limpo = str(valor_str).replace("R$", "").replace("\xa0", "").strip()
        # Para garantir que a soma funcione, removemos o ponto de milhar e trocamos a vírgula por ponto
        limpo = limpo.replace('.', '').replace(',', '.')
        return float(limpo)
    except:
        return 0.0

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

# --- 1. TAREFA EMENDAS ---
def tarefa_emendas(planilha_google):
    print("\n--- 1. Atualizando Emendas... ---")
    df = pd.read_csv(URL_EMENDAS, encoding='latin1', sep=';', on_bad_lines='skip')
    df_filtrado = df[(df['Nome Ente'] == "Canindé de São Francisco") & (df['UF'] == "SE")]
    
    aba = planilha_google.worksheet("emendas")
    aba.clear()
    aba.update('A1', [df_filtrado.columns.values.tolist()] + df_filtrado.values.tolist())
    return len(df_filtrado)

# --- 2. TAREFA RECEITAS ---
def processar_receitas(url_alvo, nome_aba, planilha_google):
    print(f"\n--- Processando Receitas: {nome_aba} ... ---")
    try:
        response = requests.get(url_alvo)
        response.raise_for_status()
    except Exception as e:
        raise Exception(f"Erro ao baixar CSV: {str(e)}")

    conteudo = response.content.decode('latin1')
    linhas = conteudo.split('\n')
    idx_inicio = next((i for i, l in enumerate(linhas) if l.strip().startswith("ANO;")), 5)
    
    dados = []
    for linha in linhas[idx_inicio + 1:]:
        partes = linha.split(';')
        if len(partes) < 5: continue
        try:
            while len(partes) < 10: partes.append("")
            p_ano = partes[0].strip()
            if not p_ano.isdigit(): continue
            dados.append([p_ano, partes[2].strip(), partes[5].strip(), partes[6].strip(), partes[8].strip(), partes[9].strip()])
        except: continue
            
    df = pd.DataFrame(dados, columns=['Ano', 'Codigo', 'Descricao', 'Previsto', 'Realizado', '%'])
    try:
        aba = planilha_google.worksheet(nome_aba)
    except:
        aba = planilha_google.add_worksheet(title=nome_aba, rows=15000, cols=15)
    aba.clear()
    if not df.empty:
        aba.update('A1', [df.columns.values.tolist()] + df.values.tolist())
    return len(df)

# ==========================================
#      LÓGICA PARA FOLHAS (RH) COM FLOAT
# ==========================================

def executar_extracao_rh(url, nome_aba, planilha_google, ano_ref):
    """Baixa o CSV, limpa o R$ e converte valores para que o Sheets possa somar."""
    print(f"   ↳ Processando Folha: {nome_aba}")
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
        if len(partes) < 5 or "Matrícula" in partes[3]: continue
        
        if len(partes) > 10 and partes[2] == "" and partes[10] != "":
            cargo_atual = partes[10]
            continue
            
        if len(partes) > 5 and partes[2] != "" and partes[4] != "":
            try:
                idx_ano = len(partes) - 1 - partes[::-1].index(ano_str) if ano_str in partes else -1
                if idx_ano == -1: continue

                sobra = [x for x in partes[idx_ano + 1:] if x != ""]
                
                # Tratamento de conversão para Float (Somas funcionando)
                val_base = limpar_cifrao(partes[idx_ano + 1])
                val_bruto = limpar_cifrao(partes[idx_ano + 2])
                val_desc = limpar_cifrao(sobra[-2]) if len(sobra) >= 2 else 0.0
                val_liq = limpar_cifrao(sobra[-1])

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
    
    # Envia como USER_ENTERED para o Sheets reconhecer o Float como número
    planilha_google.values_update(
        f"{nome_aba}!A1",
        params={'valueInputOption': 'USER_ENTERED'},
        body={'values': [df.columns.values.tolist()] + df.values.tolist()}
    )

    # NOVO: Formata automaticamente as colunas financeiras (J a M) para Moeda R$
    aba.format("J2:M15000", {
        "numberFormat": {
            "type": "CURRENCY",
            "pattern": "R$ #,##0.00"
        }
    })
    
    return len(df)

def montar_url_rh(servico_id, mes, ano):
    return f"https://agtransparenciarhserviceprd.agapesistemas.com.br/{servico_id}/rh/relatorios/relacao_vinculos_oc?regime=&matricula=&nome=&funcao=&mes={mes}&ano={ano}&total=10000&docType=csv"

def processar_folha_dinamica(servico_id, nome_aba, planilha_google, limite_meses_retrocesso=12):
    print(f"\n--- Iniciando Extração Dinâmica: {nome_aba} ---")
    agora = datetime.now()
    mes_busca, ano_busca = agora.month, agora.year
    for _ in range(limite_meses_retrocesso):
        url = montar_url_rh(servico_id, mes_busca, ano_busca)
        qtd = executar_extracao_rh(url, nome_aba, planilha_google, ano_busca)
        if qtd > 0: return qtd
        if mes_busca == 1: mes_busca, ano_busca = 12, ano_busca - 1
        else: mes_busca -= 1
    return 0

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    status = {"Conexao": "Pendente", "Emendas": "Pendente", "Receitas": "Pendente", "Folha_Geral": "Pendente", "Folha_Educacao": "Pendente", "Folha_Saude": "Pendente", "Folha_Social": "Pendente"}
    
    try:
        planilha = conectar_google()
        status["Conexao"] = "✅ OK"

        status["Emendas"] = f"✅ {tarefa_emendas(planilha)} linhas"
        status["Receitas"] = f"✅ {processar_receitas(URL_RECEITAS_FIXO, 'Receitas_2025', planilha)} linhas"
        
        folhas = {"193": "folha_pagamento_geral", "350": "folha_pagamento_educacao", 
                  "300": "folha_pagamento_saude", "299": "folha_pagamento_social"}
        
        for srv_id, aba_nome in folhas.items():
            qtd = processar_folha_dinamica(srv_id, aba_nome, planilha)
            if srv_id == "193": status["Folha_Geral"] = f"✅ {qtd} servidores"
            elif srv_id == "350": status["Folha_Educacao"] = f"✅ {qtd} servidores"
            elif srv_id == "300": status["Folha_Saude"] = f"✅ {qtd} servidores"
            elif srv_id == "299": status["Folha_Social"] = f"✅ {qtd} servidores"

    except Exception as e:
        print(f"Erro fatal: {e}")
    finally:
        assunto = "🤖 Robô Canindé: Atualização de Dados"
        msg = f"Relatório de execução:\n\n" + "\n".join([f"{k}: {v}" for k, v in status.items()])
        enviar_email(assunto, msg)
        print("🏁 Fim.")