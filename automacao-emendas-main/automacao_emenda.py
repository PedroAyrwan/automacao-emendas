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
import traceback # Importante para pegar detalhes do erro

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

# Link da Folha Geral
URL_FOLHA = "https://agtransparenciarhserviceprd.agapesistemas.com.br/193/rh/relatorios/relacao_vinculos_oc?regime=&matricula=&nome=&funcao=&mes=11&ano=2025&total=10000&docType=csv"

# Link Novo (Educação)
URL_FOLHA_EDUCACAO = "https://agtransparenciaserviceprd.agapesistemas.com.br/service/193/orcamento/receita/orcamentaria/rel?alias=pmcaninde&recursoDESO=false&filtro=1&ano=2025&mes=12&de=01-01-2025&ate=31-12-2025&covid19=false&lc173=false&consolidado=false&tipo=csv"

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

# --- TAREFA 2: RECEITAS ---
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

# --- LÓGICA DE PROCESSAMENTO DE FOLHA (Scanner Inteligente) ---
def processar_dados_folha(url_alvo, nome_aba, planilha_google):
    print(f"\n--- Processando Folha: {nome_aba} ... ---")
    
    # Ajusta total para 10000
    url_final = url_alvo
    if "total=" in url_final and "total=10000" not in url_final:
         url_final = url_final.replace("total=300", "total=10000").replace("total=5000", "total=10000")
    elif "total=" not in url_final and "?" in url_final:
         url_final += "&total=10000"
    
    print(f"🔗 Baixando dados de {url_final[:60]}...")
    response = requests.get(url_final)
    response.raise_for_status() # Vai gerar erro se o link estiver quebrado
    
    conteudo = response.content.decode('latin1')
    linhas = conteudo.split('\n')
    
    dados_processados = []
    cargo_atual = "" 
    
    print(f"🔄 Analisando {len(linhas)} linhas...")
    
    for linha in linhas:
        partes = [p.strip() for p in linha.split(';')]
        
        while len(partes) > 0 and partes[-1] == "":
            partes.pop()
            
        if len(partes) < 5: continue
        if len(partes) > 3 and (partes[2] == "CPF" or "Matrícula" in partes[3]): continue
            
        if len(partes) > 10 and partes[2] == "" and partes[10] != "":
            cargo_atual = partes[10]
            continue
            
        if len(partes) > 5 and partes[2] != "" and partes[4] != "":
            try:
                cpf = partes[2]
                matricula = partes[3]
                nome = partes[4]
                admissao = partes[5]
                vinculo = partes[7]     
                secretaria = partes[9]  
                
                # ÂNCORA 2025
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
                
                # SCANNER DE VAZIOS (FINAL DA LINHA)
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

            except (IndexError, ValueError):
                continue

            pessoa = {
                "Matricula": matricula,
                "Nome_Servidor": nome,
                "CPF": cpf,
                "Cargo": cargo_atual,      
                "Vinculo": vinculo,      
                "Secretaria": secretaria,   
                "Data_Admissao": admissao,
                "Mes": mes,             
                "Ano": ano,
                "Salario_Base": salario_base,
                "Remun_Bruta": remun_bruta,
                "Descontos": descontos,
                "Valor_Liquido": val_liquido
            }
            dados_processados.append(pessoa)

    df = pd.DataFrame(dados_processados)
    
    if not df.empty:
        df = df[df["Nome_Servidor"] != ""]

    try:
        aba = planilha_google.worksheet(nome_aba)
    except:
        aba = planilha_google.add_worksheet(title=nome_aba, rows=15000, cols=15)
    
    aba.clear()
    
    if not df.empty:
        colunas_ordenadas = [
            "Matricula", "Nome_Servidor", "CPF", "Cargo", "Vinculo", "Secretaria", 
            "Data_Admissao", "Mes", "Ano", "Salario_Base", "Remun_Bruta", 
            "Descontos", "Valor_Liquido"
        ]
        df = df.reindex(columns=colunas_ordenadas)
        dados_final = [df.columns.values.tolist()] + df.values.tolist()
        aba.update('A1', dados_final)
    
    print(f"✅ Aba '{nome_aba}' atualizada: {len(df)} registros.")
    return len(df)

def tarefa_folha_geral(planilha_google):
    return processar_dados_folha(URL_FOLHA, "folha_pagamento_geral", planilha_google)

def tarefa_folha_educacao(planilha_google):
    return processar_dados_folha(URL_FOLHA_EDUCACAO, "folha_pagamento_educacao", planilha_google)

# --- EXECUÇÃO PRINCIPAL COM RELATÓRIO DE ERROS ---
if __name__ == "__main__":
    # Variáveis de status para o relatório
    status = {
        "Conexao": "Pendente",
        "Emendas": "Pendente",
        "Receitas": "Pendente",
        "Folha_Geral": "Pendente",
        "Folha_Educacao": "Pendente"
    }
    
    try:
        # 1. Tenta conectar
        try:
            planilha = conectar_google()
            status["Conexao"] = "✅ OK"
        except Exception as e:
            status["Conexao"] = f"❌ Erro Crítico: {str(e)}"
            raise e # Se não conectar, nem adianta continuar

        # 2. Executa Emendas
        try:
            qtd = tarefa_emendas(planilha)
            status["Emendas"] = f"✅ Sucesso ({qtd} linhas)"
        except Exception as e:
            print(f"Erro em Emendas: {e}")
            status["Emendas"] = f"❌ Erro: {str(e)}"

        # 3. Executa Receitas
        try:
            qtd = tarefa_receitas(planilha)
            status["Receitas"] = f"✅ Sucesso ({qtd} linhas)"
        except Exception as e:
            print(f"Erro em Receitas: {e}")
            status["Receitas"] = f"❌ Erro: {str(e)}"

        # 4. Executa Folha Geral
        try:
            qtd = tarefa_folha_geral(planilha)
            status["Folha_Geral"] = f"✅ Sucesso ({qtd} servidores)"
        except Exception as e:
            print(f"Erro na Folha Geral: {e}")
            status["Folha_Geral"] = f"❌ Erro: {str(e)}"

        # 5. Executa Folha Educação
        try:
            qtd = tarefa_folha_educacao(planilha)
            status["Folha_Educacao"] = f"✅ Sucesso ({qtd} servidores)"
        except Exception as e:
            print(f"Erro na Folha Educação: {e}")
            status["Folha_Educacao"] = f"❌ Erro: {str(e)}"

    except Exception as e:
        # Captura erro genérico se algo fora do comum acontecer
        print(f"Erro fatal no script: {e}")

    finally:
        # --- MONTA O E-MAIL FINAL (SEMPRE ENVIA) ---
        assunto_email = "🤖 Robô Canindé: Relatório de Execução"
        
        # Verifica se houve algum erro para mudar o ícone do assunto
        if any("❌" in v for v in status.values()):
            assunto_email = "⚠️ Robô Canindé: AVISO DE ERRO"

        mensagem_final = f"""
        Olá! Aqui está o resumo da execução do robô:

        🔌 Conexão Google: {status['Conexao']}
        
        💰 Emendas Parlamentares:
        {status['Emendas']}
        
        📉 Receitas Municipais:
        {status['Receitas']}
        
        👥 Folha de Pagamento (Geral):
        {status['Folha_Geral']}
        
        🎓 Folha de Pagamento (Educação):
        {status['Folha_Educacao']}
        
        ---------------------------------------
        Data de execução: {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}
        """
        
        enviar_email(assunto_email, mensagem_final)
        print("🏁 Fim da execução.")