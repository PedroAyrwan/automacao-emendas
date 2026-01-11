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

# --- LINKS ESTÁTICOS (Não mudam com a data) ---
URL_EMENDAS = "https://www.tesourotransparente.gov.br/ckan/dataset/83e419da-1552-46bf-bfc3-05160b2c46c9/resource/66d69917-a5d8-4500-b4b2-ef1f5d062430/download/emendas-parlamentares.csv"
URL_RECEITAS_FIXO = "https://agtransparenciaserviceprd.agapesistemas.com.br/service/193/orcamento/receita/orcamentaria/rel?alias=pmcaninde&recursoDESO=false&filtro=1&ano=2025&mes=12&de=01-01-2025&ate=31-12-2025&covid19=false&lc173=false&consolidado=false&tipo=csv"

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

# --- 1. TAREFA EMENDAS (Estatica) ---
def tarefa_emendas(planilha_google):
    print("\n--- 1. Atualizando Emendas... ---")
    df = pd.read_csv(URL_EMENDAS, encoding='latin1', sep=';', on_bad_lines='skip')
    df_filtrado = df[(df['Nome Ente'] == "Canindé de São Francisco") & (df['UF'] == "SE")]
    
    aba = planilha_google.worksheet("emendas")
    aba.clear()
    aba.update('A1', [df_filtrado.columns.values.tolist()] + df_filtrado.values.tolist())
    return len(df_filtrado)

# --- 2. TAREFA RECEITAS (Estatica - Link Fixo) ---
def processar_receitas(url_alvo, nome_aba, planilha_google):
    print(f"\n--- Processando Receitas: {nome_aba} ... ---")
    try:
        response = requests.get(url_alvo)
        response.raise_for_status()
    except Exception as e:
        raise Exception(f"Erro ao baixar CSV: {str(e)}")

    conteudo = response.content.decode('latin1')
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
#      LÓGICA DINÂMICA PARA FOLHAS (RH)
# ==========================================

def montar_url_rh(servico_id, mes, ano):
    """Gera a URL do RH para um mês e ano específicos, forçando 10.000 registros."""
    base = f"https://agtransparenciarhserviceprd.agapesistemas.com.br/{servico_id}/rh/relatorios/relacao_vinculos_oc"
    # total=10000 garante que vem a lista completa
    params = f"?regime=&matricula=&nome=&funcao=&mes={mes}&ano={ano}&total=10000&docType=csv"
    return base + params

def executar_extracao_rh(url, nome_aba, planilha_google, ano_ref):
    """Baixa o CSV, processa e salva no Google Sheets. Retorna quantidade de linhas."""
    print(f"   ↳ Baixando: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception as err:
        print(f"     Erro conexão: {err}")
        return 0
    
    conteudo = response.content.decode('latin1')
    linhas = conteudo.split('\n')
    
    dados_processados = []
    cargo_atual = ""
    ano_str = str(ano_ref) 
    
    for linha in linhas:
        partes = [p.strip() for p in linha.split(';')]
        while len(partes) > 0 and partes[-1] == "": partes.pop()
        
        if len(partes) < 5: continue
        if len(partes) > 3 and (partes[2] == "CPF" or "Matrícula" in partes[3]): continue
        
        # Captura Cargo (linha de cabeçalho de grupo)
        if len(partes) > 10 and partes[2] == "" and partes[10] != "":
            cargo_atual = partes[10]
            continue
            
        # Captura Pessoa
        if len(partes) > 5 and partes[2] != "" and partes[4] != "":
            try:
                # Procura o Ano de Referência na linha para alinhar as colunas
                if ano_str in partes:
                    idx_ano = len(partes) - 1 - partes[::-1].index(ano_str)
                else:
                    # Fallback: Tenta achar 2025 ou 2024 se o ano_ref não estiver explícito
                    if "2025" in partes: idx_ano = len(partes) - 1 - partes[::-1].index("2025")
                    elif "2024" in partes: idx_ano = len(partes) - 1 - partes[::-1].index("2024")
                    else: continue

                mes_dado = partes[idx_ano - 1]
                ano_dado = partes[idx_ano]
                salario_base = partes[idx_ano + 1]
                remun_bruta = partes[idx_ano + 2]
                
                resto_linha = partes[idx_ano + 3 : ]
                valores_financeiros = [x for x in resto_linha if x != ""]
                
                if len(valores_financeiros) >= 2:
                    descontos = valores_financeiros[-2]
                    val_liquido = valores_financeiros[-1]
                elif len(valores_financeiros) == 1:
                    descontos = "0,00"
                    val_liquido = valores_financeiros[-1]
                else:
                    descontos = "0,00"
                    val_liquido = "0,00"

                pessoa = {
                    "Matricula": partes[3], "Nome_Servidor": partes[4], "CPF": partes[2],
                    "Cargo": cargo_atual, "Vinculo": partes[7], "Secretaria": partes[9],
                    "Data_Admissao": partes[5], "Mes": mes_dado, "Ano": ano_dado,
                    "Salario_Base": salario_base, "Remun_Bruta": remun_bruta,
                    "Descontos": descontos, "Valor_Liquido": val_liquido
                }
                dados_processados.append(pessoa)
            except: continue

    if not dados_processados:
        return 0

    df = pd.DataFrame(dados_processados)
    if not df.empty: df = df[df["Nome_Servidor"] != ""]
    
    try:
        aba = planilha_google.worksheet(nome_aba)
    except:
        aba = planilha_google.add_worksheet(title=nome_aba, rows=15000, cols=15)
    
    aba.clear()
    
    if not df.empty:
        colunas_ordenadas = ["Matricula", "Nome_Servidor", "CPF", "Cargo", "Vinculo", "Secretaria", 
                             "Data_Admissao", "Mes", "Ano", "Salario_Base", "Remun_Bruta", 
                             "Descontos", "Valor_Liquido"]
        df = df.reindex(columns=colunas_ordenadas)
        aba.update('A1', [df.columns.values.tolist()] + df.values.tolist())
    
    return len(df)

def processar_folha_dinamica(servico_id, nome_aba, planilha_google, limite_meses_retrocesso=12):
    """
    Tenta baixar dados começando do mês atual.
    Se falhar, recua 1 mês e tenta de novo, repetindo até o limite (padrão 12 meses).
    """
    print(f"\n--- Processando Dinâmico: {nome_aba} (ID {servico_id}) ---")
    
    agora = datetime.now()
    mes_busca = agora.month
    ano_busca = agora.year
    
    for tentativa in range(limite_meses_retrocesso):
        print(f"🔄 Tentativa {tentativa + 1}/{limite_meses_retrocesso}: Buscando competência {mes_busca}/{ano_busca}...")
        
        url = montar_url_rh(servico_id, mes_busca, ano_busca)
        
        # Tenta baixar e processar usando o ano da busca como referência
        qtd = executar_extracao_rh(url, nome_aba, planilha_google, ano_busca)
        
        if qtd > 0:
            print(f"✅ SUCESSO! Dados encontrados em {mes_busca}/{ano_busca} ({qtd} registros).")
            return qtd
        
        print(f"⚠️ Competência {mes_busca}/{ano_busca} vazia. Recuando 1 mês...")
        
        # Lógica para voltar 1 mês (tratando virada de ano: Janeiro -> Dezembro do ano anterior)
        if mes_busca == 1:
            mes_busca = 12
            ano_busca -= 1
        else:
            mes_busca -= 1

    print(f"❌ Falha: Nenhum dado encontrado após {limite_meses_retrocesso} meses de busca retroativa.")
    return 0

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    status = {
        "Conexao": "Pendente",
        "Emendas": "Pendente",
        "Receitas": "Pendente",
        "Folha_Geral": "Pendente",
        "Folha_Educacao": "Pendente",
        "Folha_Saude": "Pendente",
        "Folha_Social": "Pendente"
    }
    
    try:
        try:
            planilha = conectar_google()
            status["Conexao"] = "✅ OK"
        except Exception as e:
            status["Conexao"] = f"❌ Erro Crítico: {str(e)}"
            raise e 

        # 1. EMENDAS (Estatica)
        try:
            qtd = tarefa_emendas(planilha)
            status["Emendas"] = f"✅ Sucesso ({qtd} linhas)"
        except Exception as e:
            status["Emendas"] = f"❌ Erro: {str(e)}"

        # 2. RECEITAS (Estatica)
        try:
            qtd = processar_receitas(URL_RECEITAS_FIXO, "Receitas_2025", planilha)
            status["Receitas"] = f"✅ Sucesso ({qtd} linhas)"
        except Exception as e:
            status["Receitas"] = f"❌ Erro: {str(e)}"

        # 3. FOLHA GERAL (Dinâmica - ID 193)
        try:
            qtd = processar_folha_dinamica("193", "folha_pagamento_geral", planilha)
            status["Folha_Geral"] = f"✅ Sucesso ({qtd} servidores)"
        except Exception as e:
            status["Folha_Geral"] = f"❌ Falha: {str(e)}"

        # 4. FOLHA EDUCAÇÃO (Dinâmica - ID 350)
        try:
            qtd = processar_folha_dinamica("350", "folha_pagamento_educacao", planilha)
            status["Folha_Educacao"] = f"✅ Sucesso ({qtd} servidores)"
        except Exception as e:
            status["Folha_Educacao"] = f"❌ Falha: {str(e)}"

        # 5. FOLHA SAÚDE (Dinâmica - ID 300)
        try:
            qtd = processar_folha_dinamica("300", "folha_pagamento_saude", planilha)
            status["Folha_Saude"] = f"✅ Sucesso ({qtd} servidores)"
        except Exception as e:
            status["Folha_Saude"] = f"❌ Falha: {str(e)}"

        # 6. FOLHA ASSISTÊNCIA SOCIAL (Dinâmica - ID 299)
        try:
            qtd = processar_folha_dinamica("299", "folha_pagamento_social", planilha)
            status["Folha_Social"] = f"✅ Sucesso ({qtd} servidores)"
        except Exception as e:
            status["Folha_Social"] = f"❌ Falha: {str(e)}"

    except Exception as e:
        print(f"Erro fatal: {e}")

    finally:
        assunto = "🤖 Robô Canindé: Relatório Completo"
        if any("❌" in v for v in status.values()): assunto = "⚠️ Robô Canindé: ALERTA DE ERRO"
        
        msg = f"""Status Geral:
        
        🔌 Conexão: {status['Conexao']}
        💰 Emendas: {status['Emendas']}
        📉 Receitas: {status['Receitas']}
        👥 Folha Geral: {status['Folha_Geral']}
        🎓 Folha Educação: {status['Folha_Educacao']}
        🏥 Folha Saúde: {status['Folha_Saude']}
        🤝 Folha Social: {status['Folha_Social']}
        """
        enviar_email(assunto, msg)
        print("🏁 Fim.")