import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- CONFIGURAÇÕES ---
URL_CSV = "https://www.tesourotransparente.gov.br/ckan/dataset/83e419da-1552-46bf-bfc3-05160b2c46c9/resource/66d69917-a5d8-4500-b4b2-ef1f5d062430/download/emendas-parlamentares.csv"
ID_PLANILHA = "1Do1s1cAMxeEMNyV87etGV5L8jxwAp4ermInaUR74bVs" 
NOME_ABA = "folha1" # Atualizado conforme seu pedido

def processar_dados():
    print("--- 1. Baixando e Lendo CSV (Aguarde...) ---")
    try:
        df = pd.read_csv(URL_CSV, sep=';', encoding='latin1', on_bad_lines='skip')
        
        print(f"Total de linhas baixadas: {len(df)}")
        
        # --- 2. FILTRANDO OS DADOS ---
        print(f"--- Aplicando filtro para: Canindé de São Francisco - SE ---")
        
        # O truque aqui: converter tudo para maiúsculo (.str.upper()) para garantir que ache 
        # mesmo que esteja escrito minúsculo ou misturado.
        # Filtra onde 'Nome Ente' contém CANINDÉ e 'UF' é SE.
        
        filtro_cidade = df['Nome Ente'].str.contains("CANINDÉ DE SÃO FRANCISCO", case=False, na=False)
        filtro_estado = df['UF'] == 'SE'
        
        df_filtrado = df[filtro_cidade & filtro_estado]
        
        # Se não achar nada, avisa
        if len(df_filtrado) == 0:
            print("AVISO: Nenhuma linha encontrada com esse nome. Verifique a grafia.")
            # Dica: Às vezes o governo escreve "MUN DE CANINDE..."
        
        print(f"Linhas encontradas após filtro: {len(df_filtrado)}")
        return df_filtrado

    except Exception as e:
        print(f"Erro ao processar dados: {e}")
        return None

def salvar_no_sheets(df):
    if df is None or len(df) == 0:
        print("Nada para salvar (tabela vazia).")
        return

    print(f"--- 3. Enviando para a aba '{NOME_ABA}' do Google Sheets ---")
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        client = gspread.authorize(creds)

        # Tenta abrir a aba específica "folha1"
        try:
            sheet = client.open_by_key(ID_PLANILHA).worksheet(NOME_ABA)
        except gspread.WorksheetNotFound:
            # Se não achar a "folha1", tenta pegar a primeira aba (plano B)
            print(f"Aba '{NOME_ABA}' não encontrada. Tentando a primeira aba disponível...")
            sheet = client.open_by_key(ID_PLANILHA).get_worksheet(0)
        
        sheet.clear()
        
        # Preenche vazios para não dar erro
        df = df.fillna('')
        
        # Envia
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        
        print("--- SUCESSO! Dados de Canindé enviados para a planilha. ---")
        
    except Exception as e:
        print(f"Erro no Google Sheets: {e}")

# --- EXECUÇÃO ---
if __name__ == "__main__":
    dados = processar_dados()
    salvar_no_sheets(dados)