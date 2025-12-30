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

# Link de Receitas (Geral - Serviço 193)
URL_RECEITAS = "https://agtransparenciaserviceprd.agapesistemas.com.br/service/193/orcamento/receita/orcamentaria/rel?alias=pmcaninde&recursoDESO=false&filtro=1&ano=2025&mes=12&de=01-01-2025&ate=31-12-2025&covid19=false&lc173=false&consolidado=false&tipo=csv"

# Link da Folha Geral (RH - Serviço 193)
URL_FOLHA = "https://agtransparenciarhserviceprd.agapesistemas.com.br/193/rh/relatorios/relacao_vinculos_oc?regime=&matricula=&nome=&funcao=&mes=11&ano=2025&total=10000&docType=csv"

# Link da Folha Educação (Atualizado - Serviço 350 - Receita)
URL_FOLHA_EDUCACAO = "https://agtransparenciaserviceprd.agapesistemas.com.br/service/350/orcamento/receita/orcamentaria/rel?alias=pmcaninde&recursoDESO=false&filtro=1&ano=2025&mes=12&de=01-01-2025&ate=31-12-2025&covid19=false&lc173=false&consolidado=false&tipo=csv"

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

# --- TAREFA 1: EMENDAS ---
def tarefa_emendas(planilha_google):
    print("\n--- 1. Atualizando Emendas... ---")
    df = pd.read_csv(URL_EMENDAS, encoding='latin1', sep=';', on_bad_lines='skip')
    df_filtrado = df[(df['Nome Ente'] == "Canindé de São Francisco") & (df['UF'] == "SE")]
    
    aba = planilha_google.worksheet("emendas")
    aba.clear()
    aba.update('A1', [df_filtrado.columns.values.tolist()] + df_filtrado.values.tolist())
    return len(df_filtrado)

# --- TAREFA: PROCESSAR RECEITAS (Manual e Robusto) ---
def processar_receitas(url_alvo, nome_aba, planilha_google):
    print(f"\n--- Processando Receitas: {nome_aba} ... ---")
    
    try:
        response = requests.get(url_alvo)
        response.raise_for_status()
    except Exception as e:
        raise Exception(f"Erro ao baixar CSV: {str(e)}")

    conteudo = response.content.decode('latin1')
    linhas = conteudo.split('\n')
    
    # 1. Encontrar onde começam os dados (Linha de Cabeçalho "ANO;")
    idx_inicio = -1
    for i, linha in enumerate(linhas):
        if linha.strip().startswith("ANO;"):
            idx_inicio = i
            break
    
    if idx_inicio == -1:
        # Se não achou "ANO;", tenta pular umas 5 linhas padrão
        idx_inicio = 5 
    
    dados = []
    # Processa as linhas ABAIXO do cabeçalho
    print(f"🔄 Processando a partir da linha {idx_inicio + 2}...")
    
    for linha in linhas[idx_inicio + 1:]:
        partes = linha.split(';')
        
        # Filtra linhas vazias ou de rodapé
        if len(partes) < 5: continue
        
        # O arquivo tem colunas vazias extras. Vamos pegar pelos índices fixos observados:
        # 0: Ano
        # 2: Código (As vezes vem no 1 ou 2 dependendo do ; extra)
        # Vamos limpar vazios para pegar sequencialmente? Não, melhor índice fixo do CSV.
        # No CSV enviado: 2025;;1000...;;;RECEITAS...
        # 0: 2025
        # 2: Código
        # 5: Descrição
        # 6: Previsto
        # 8: Realizado
        # 9: %
        
        try:
            # Garante que tem tamanho suficiente
            while len(partes) < 10: partes.append("")
            
            p_ano = partes[0].strip()
            # Se não tiver ano, ignora (pode ser linha de total ou lixo)
            if not p_ano.isdigit(): continue
            
            p_codigo = partes[2].strip()
            p_descricao = partes[5].strip()
            p_previsto = partes[6].strip()
            p_realizado = partes[8].strip()
            p_porc = partes[9].strip()
            
            dados.append([p_ano, p_codigo, p_descricao, p_previsto, p_realizado, p_porc])
        except:
            continue
            
    df = pd.DataFrame(dados, columns=['Ano', 'Codigo', 'Descricao', 'Previsto', 'Realizado', '%'])
    
    try:
        aba = planilha_google.worksheet(nome_aba)
    except:
        aba = planilha_google.add_worksheet(title=nome_aba, rows=15000, cols=15)
    
    aba.clear()
    
    if not df.empty:
        aba.update('A1', [df.columns.values.tolist()] + df.values.tolist())
    
    print(f"✅ Aba '{nome_aba}' atualizada: {len(df)} registros.")
    return len(df)

# --- TAREFA: PROCESSAR FOLHA (RH) ---
def processar_folha(url_alvo, nome_aba, planilha_google):
    print(f"\n--- Processando Folha RH: {nome_aba} ... ---")
    
    url_final = url_alvo
    if "rh/relatorios" in url_final:
        if "total=" in url_final and "total=10000" not in url_final:
             url_final = url_final.replace("total=300", "total=10000").replace("total=5000", "total=10000")
        elif "total=" not in url_final and "?" in url_final:
             url_final += "&total=10000"
    
    try:
        response = requests.get(url_final)
        response.raise_for_status() 
    except Exception as err:
        raise Exception(f"Erro de Conexão RH: {str(err)}")
    
    conteudo = response.content.decode('latin1')
    linhas = conteudo.split('\n')
    
    dados_processados = []
    cargo_atual = "" 
    
    print(f"🔄 Analisando {len(linhas)} linhas...")
    
    for linha in linhas:
        partes = [p.strip() for p in linha.split(';')]
        while len(partes) > 0 and partes[-1] == "": partes.pop()
        
        if len(partes) < 5: continue
        if len(partes) > 3 and (partes[2] == "CPF" or "Matrícula" in partes[3]): continue
        
        if len(partes) > 10 and partes[2] == "" and partes[10] != "":
            cargo_atual = partes[10]
            continue
            
        if len(partes) > 5 and partes[2] != "" and partes[4] != "":
            try:
                if "2025" in partes:
                    idx_ano = len(partes) - 1 - partes[::-1].index("2025")
                elif "2024" in partes:
                    idx_ano = len(partes) - 1 - partes[::-1].index("2024")
                else:
                    continue

                mes = partes[idx_ano - 1]
                ano = partes[idx_ano]
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
                    "Matricula": partes[3],
                    "Nome_Servidor": partes[4],
                    "CPF": partes[2],
                    "Cargo": cargo_atual,      
                    "Vinculo": partes[7],      
                    "Secretaria": partes[9],   
                    "Data_Admissao": partes[5],
                    "Mes": mes,             
                    "Ano": ano,
                    "Salario_Base": salario_base,
                    "Remun_Bruta": remun_bruta,
                    "Descontos": descontos,
                    "Valor_Liquido": val_liquido
                }
                dados_processados.append(pessoa)
            except: continue

    df = pd.DataFrame(dados_processados)
    if not df.empty: df = df[df["Nome_Servidor"] != ""]
    
    try:
        aba = planilha_google.worksheet(nome_aba)
    except:
        aba = planilha_google.add_worksheet(title=nome_aba, rows=15000, cols=15)
    
    aba.clear()
    if not df.empty:
        colunas_ordenadas = ["Matricula", "Nome_Servidor", "CPF", "Cargo", "Vinculo", "Secretaria", "Data_Admissao", "Mes", "Ano", "Salario_Base", "Remun_Bruta", "Descontos", "Valor_Liquido"]
        df = df.reindex(columns=colunas_ordenadas)
        aba.update('A1', [df.columns.values.tolist()] + df.values.tolist())
    
    return len(df)

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    status = {"Conexao": "Pendente", "Emendas": "Pendente", "Receitas": "Pendente", "Folha_Geral": "Pendente", "Folha_Educacao": "Pendente"}
    
    try:
        try:
            planilha = conectar_google()
            status["Conexao"] = "✅ OK"
        except Exception as e:
            status["Conexao"] = f"❌ Erro Crítico: {str(e)}"
            raise e 

        try:
            qtd = tarefa_emendas(planilha)
            status["Emendas"] = f"✅ Sucesso ({qtd} linhas)"
        except Exception as e:
            status["Emendas"] = f"❌ Erro: {str(e)}"

        # Receitas Gerais (193)
        try:
            qtd = processar_receitas(URL_RECEITAS, "Receitas_2025", planilha)
            status["Receitas"] = f"✅ Sucesso ({qtd} linhas)"
        except Exception as e:
            status["Receitas"] = f"❌ Erro: {str(e)}"

        # Folha Geral (193) - RH
        try:
            qtd = processar_folha(URL_FOLHA, "folha_pagamento_geral", planilha)
            status["Folha_Geral"] = f"✅ Sucesso ({qtd} servidores)"
        except Exception as e:
            status["Folha_Geral"] = f"❌ Falha: {str(e)}"

        # Folha Educação (350) - RECEITA (Link fornecido é de receita)
        try:
            qtd = processar_receitas(URL_FOLHA_EDUCACAO, "folha_pagamento_educacao", planilha)
            status["Folha_Educacao"] = f"✅ Sucesso ({qtd} linhas)"
        except Exception as e:
            status["Folha_Educacao"] = f"❌ Falha: {str(e)}"

    except Exception as e:
        print(f"Erro fatal: {e}")

    finally:
        assunto = "🤖 Robô Canindé: Relatório"
        if any("❌" in v for v in status.values()): assunto = "⚠️ Robô Canindé: ALERTA DE ERRO"
        
        msg = f"Status:\n\n🔌 Conexão: {status['Conexao']}\n💰 Emendas: {status['Emendas']}\n📉 Receitas: {status['Receitas']}\n👥 Folha Geral: {status['Folha_Geral']}\n🎓 Folha Educação: {status['Folha_Educacao']}"
        enviar_email(assunto, msg)
        print("🏁 Fim.")