import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import smtplib
from email.mime.text import MIMEText
import os
import requests
from dotenv import load_dotenv
from datetime import datetime

# --- CONFIGURAÇÕES INICIAIS ---
load_dotenv()

def limpar_config(valor):
    return str(valor).strip() if valor else ""

EMAIL_REMETENTE = limpar_config(os.getenv("EMAIL_REMETENTE"))
SENHA_EMAIL = limpar_config(os.getenv("SENHA_EMAIL"))
STRING_DESTINATARIOS = limpar_config(os.getenv("EMAIL_DESTINATARIO"))

CREDENCIAIS_JSON = 'credentials.json'
NOME_PLANILHA_GOOGLE = "Robo_Caninde"
LINK_PLANILHA = "https://docs.google.com/spreadsheets/d/1Do1s1cAMxeEMNyV87etGV5L8jxwAp4ermInaUR74bVs/edit?usp=sharing"

URL_RECEITAS_FIXO = "https://agtransparenciaserviceprd.agapesistemas.com.br/service/193/orcamento/receita/orcamentaria/rel?alias=pmcaninde&recursoDESO=false&filtro=1&ano=2025&mes=12&de=01-01-2025&ate=31-12-2025&covid19=false&lc173=false&consolidado=false&tipo=csv"

# --- FUNÇÃO DE CONVERSÃO (Onde estava o erro do 0) ---
def converter_para_float(valor_original):
    """Lê a string do site, limpa pontos e vírgulas e transforma em número real."""
    if not valor_original: return 0.0
    v = str(valor_original).strip()
    if v in ["", "-", "0", "0,00"]: return 0.0
    try:
        # Remove ponto de milhar e troca vírgula por ponto decimal
        v_limpo = v.replace('.', '').replace(',', '.')
        return float(v_limpo)
    except:
        return 0.0

# --- FUNÇÃO DE CONEXÃO ---
def conectar_google():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENCIAIS_JSON, scope)
    return gspread.authorize(creds).open(NOME_PLANILHA_GOOGLE)

# --- PROCESSO DE RH (Lógica Dinâmica de Colunas) ---
def executar_extracao_rh(url, nome_aba, planilha_google, ano_ref):
    print(f"   ↳ A extrair dados de: {url}")
    try:
        res = requests.get(url, timeout=30)
        linhas = res.content.decode('latin1').split('\n')
    except: return 0
    
    dados_finais = []
    cargo_atual = ""
    ano_procurado = str(ano_ref)

    for linha in linhas:
        partes = [p.strip() for p in linha.split(';')]
        if len(partes) < 5 or "CPF" in partes: continue
        
        # Identifica se a linha é um cabeçalho de Cargo
        if len(partes) > 10 and partes[2] == "" and partes[10] != "":
            cargo_atual = partes[10]
            continue
            
        # Se for linha de servidor (tem CPF e Nome)
        if len(partes) > 5 and partes[2] != "" and partes[4] != "":
            try:
                # Localiza a posição do Ano na linha para alinhar os valores financeiros
                idx_ano = -1
                for i, v in enumerate(partes):
                    if v == ano_procurado:
                        idx_ano = i
                        break
                
                if idx_ano != -1:
                    # EM VEZ DE COLUNA FIXA, FILTRAMOS O QUE É NÚMERO DEPOIS DO ANO
                    # Isso evita pegar colunas vazias intermediárias
                    campos_depois_ano = partes[idx_ano + 1:]
                    valores_numericos = [c for c in campos_depois_ano if any(char.isdigit() for char in c) and ',' in c]
                    
                    if len(valores_numericos) >= 2:
                        # Mapeamento dinâmico:
                        s_base = converter_para_float(valores_numericos[0])
                        s_bruto = converter_para_float(valores_numericos[1])
                        # O Líquido é sempre o último número da linha
                        s_liq = converter_para_float(valores_numericos[-1])
                        # O Desconto é o penúltimo
                        s_desc = converter_para_float(valores_numericos[-2]) if len(valores_numericos) > 2 else 0.0
                        
                        dados_finais.append({
                            "Matricula": partes[3], "Nome": partes[4], "CPF": partes[2],
                            "Cargo": cargo_atual, "Secretaria": partes[9], "Mes": partes[idx_ano-1],
                            "Salario_Base": s_base, "Bruto": s_bruto, "Descontos": s_desc, "Liquido": s_liq
                        })
            except: continue

    if not dados_finais: return 0
    
    df = pd.DataFrame(dados_finais)
    aba = planilha_google.worksheet(nome_aba)
    aba.clear()
    # value_input_option='RAW' faz com que o Google entenda que é um NÚMERO e não texto
    aba.update('A1', [df.columns.values.tolist()] + df.values.tolist(), value_input_option='RAW')
    return len(df)

def processar_folha(id_srv, aba_nome, planilha):
    agora = datetime.now()
    m, a = agora.month, agora.year
    for _ in range(12):
        url = f"https://agtransparenciarhserviceprd.agapesistemas.com.br/{id_srv}/rh/relatorios/relacao_vinculos_oc?mes={m}&ano={a}&total=10000&docType=csv"
        qtd = executar_extracao_rh(url, aba_nome, planilha, a)
        if qtd > 0: return qtd
        m, a = (12, a-1) if m == 1 else (m-1, a)
    return 0

# --- EXECUÇÃO ---
if __name__ == "__main__":
    try:
        planilha = conectar_google()
        
        # Lista de Secretarias para atualizar
        secretarias = {
            "193": "folha_pagamento_geral",
            "350": "folha_pagamento_educacao",
            "300": "folha_pagamento_saude",
            "299": "folha_pagamento_social"
        }
        
        for id_srv, aba in secretarias.items():
            processar_folha(id_srv, aba, planilha)
            
        print("🏁 Concluído com sucesso. Valores convertidos para Float.")
    except Exception as e:
        print(f"❌ Erro: {e}")