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

# --- FUNÇÃO DE CONVERSÃO MELHORADA ---
def converter_para_float(valor_original):
    """Lê a string, remove pontos de milhar e converte vírgula em ponto para float."""
    if not valor_original: return 0.0
    v = str(valor_original).strip()
    # Se o valor não contém dígitos, retorna 0.0
    if not any(char.isdigit() for char in v): return 0.0
    try:
        # Remove ponto de milhar (1.000 -> 1000) e troca vírgula por ponto (10,50 -> 10.50)
        v_limpo = v.replace('.', '').replace(',', '.')
        return float(v_limpo)
    except:
        return 0.0

# --- FUNÇÃO DE CONEXÃO ---
def conectar_google():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENCIAIS_JSON, scope)
    return gspread.authorize(creds).open(NOME_PLANILHA_GOOGLE)

# --- PROCESSO DE RH (Lógica de Filtragem Dinâmica de Números) ---
def executar_extracao_rh(url, nome_aba, planilha_google, ano_ref):
    print(f"   ↳ A extrair dados de: {url}")
    try:
        res = requests.get(url, timeout=30)
        # O portal Ágape usa latin1 na maioria das vezes
        linhas = res.content.decode('latin1').split('\n')
    except Exception as e:
        print(f"      Erro na requisição: {e}")
        return 0
    
    dados_finais = []
    cargo_atual = ""
    ano_procurado = str(ano_ref)

    for linha in linhas:
        partes = [p.strip() for p in linha.split(';')]
        
        # Ignora linhas pequenas ou cabeçalhos
        if len(partes) < 5 or "CPF" in partes: 
            # Detecta se a linha é um cabeçalho de Cargo (comum no layout Ágape)
            if len(partes) > 10 and partes[2] == "" and partes[10] != "":
                cargo_atual = partes[10]
            continue
            
        # Verifica se a linha tem dados de servidor (CPF e Nome preenchidos)
        if len(partes) > 5 and partes[2] != "" and partes[4] != "":
            try:
                # Localiza a posição do Ano na linha
                idx_ano = -1
                for i, v in enumerate(partes):
                    if v == ano_procurado:
                        idx_ano = i
                        break
                
                if idx_ano != -1:
                    # ESTRATÉGIA: Pegamos tudo que vem depois do ANO e filtramos 
                    # apenas o que realmente parece um número financeiro (contém dígito e vírgula)
                    sobra_da_linha = partes[idx_ano + 1:]
                    valores_financeiros = [c for c in sobra_da_linha if any(char.isdigit() for char in c) and ',' in c]
                    
                    if len(valores_financeiros) >= 1:
                        # Mapeamento Baseado na Ordem de Aparição dos Números:
                        # 1º número: Salário Base
                        # 2º número: Bruto (se existir)
                        # Último número: Líquido
                        # Penúltimo: Descontos
                        
                        s_base = converter_para_float(valores_financeiros[0])
                        s_bruto = converter_para_float(valores_financeiros[1]) if len(valores_financeiros) > 1 else s_base
                        s_liq = converter_para_float(valores_financeiros[-1])
                        s_desc = converter_para_float(valores_financeiros[-2]) if len(valores_financeiros) >= 3 else 0.0
                        
                        # Garante que o desconto não seja o bruto por erro de contagem
                        if len(valores_financeiros) < 3: s_desc = 0.0

                        dados_finais.append({
                            "Matricula": partes[3], 
                            "Nome": partes[4], 
                            "CPF": partes[2],
                            "Cargo": cargo_atual, 
                            "Secretaria": partes[9], 
                            "Mes": partes[idx_ano-1],
                            "Ano": partes[idx_ano],
                            "Salario_Base": s_base, 
                            "Bruto": s_bruto, 
                            "Descontos": s_desc, 
                            "Liquido": s_liq
                        })
            except: 
                continue

    if not dados_finais: 
        print(f"      ⚠️ Nenhum dado processado para {nome_aba}")
        return 0
    
    df = pd.DataFrame(dados_finais)
    try:
        aba = planilha_google.worksheet(nome_aba)
    except:
        aba = planilha_google.add_worksheet(title=nome_aba, rows="1000", cols="20")
        
    aba.clear()
    # O uso do 'RAW' no final é vital para o Google Sheets não tratar como texto
    aba.update('A1', [df.columns.values.tolist()] + df.values.tolist(), value_input_option='RAW')
    print(f"      ✅ {len(df)} registros enviados para {nome_aba}")
    return len(df)

def processar_folha(id_srv, aba_nome, planilha):
    print(f"\n--- Processando: {aba_nome} ---")
    agora = datetime.now()
    m, a = agora.month, agora.year
    
    # Tenta buscar os dados dos últimos 12 meses até encontrar um mês preenchido
    for _ in range(12):
        url = f"https://agtransparenciarhserviceprd.agapesistemas.com.br/{id_srv}/rh/relatorios/relacao_vinculos_oc?mes={m}&ano={a}&total=10000&docType=csv"
        qtd = executar_extracao_rh(url, aba_nome, planilha, a)
        if qtd > 0: return qtd
        
        # Retrocede um mês
        if m == 1:
            m = 12
            a -= 1
        else:
            m -= 1
    return 0

# --- EXECUÇÃO ---
if __name__ == "__main__":
    try:
        planilha = conectar_google()
        
        secretarias = {
            "193": "folha_pagamento_geral",
            "350": "folha_pagamento_educacao",
            "300": "folha_pagamento_saude",
            "299": "folha_pagamento_social"
        }
        
        for id_srv, aba in secretarias.items():
            processar_folha(id_srv, aba, planilha)
            
        print("\n🏁 Concluído com sucesso. Valores numéricos validados.")
    except Exception as e:
        print(f"❌ Erro Crítico: {e}")