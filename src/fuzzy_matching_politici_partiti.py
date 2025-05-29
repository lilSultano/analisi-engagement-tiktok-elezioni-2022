import pandas as pd
from fuzzywuzzy import process

file_principale_path = 'src/output-data/social_data_con_nomi_reali.csv'
colonna_partito_da_mappare = 'PARTITO_REALE'

file_riferimento_path = 'datasets/Politiche2022_Scrutini_Camera_Italia.csv'
colonna_lista_riferimento = 'LISTA'
colonna_partito_ufficiale_riferimento = 'LISTA'

soglia_similarita = 70

try:
    df_principale = pd.read_csv(file_principale_path)
    df_riferimento = pd.read_csv(file_riferimento_path)
except Exception:
    exit()

if colonna_partito_da_mappare not in df_principale.columns:
    exit()
if colonna_lista_riferimento not in df_riferimento.columns:
    exit()
if colonna_partito_ufficiale_riferimento and colonna_partito_ufficiale_riferimento not in df_riferimento.columns:
    colonna_partito_ufficiale_riferimento = None

df_principale_filtrato = df_principale[
    (df_principale['MATCHED'] == True) & 
    (df_principale['NOME_POLITICO_REALE'].notna()) & 
    (df_principale['NOME_POLITICO_REALE'] != '')
].copy()

df_riferimento[colonna_lista_riferimento + '_lower'] = df_riferimento[colonna_lista_riferimento].astype(str).str.lower().str.strip()
scelte_liste_lower = df_riferimento[colonna_lista_riferimento + '_lower'].unique().tolist()
df_principale_filtrato[colonna_partito_da_mappare + '_lower'] = df_principale_filtrato[colonna_partito_da_mappare].fillna('').astype(str).str.lower().str.strip()

def trova_migliore_corrispondenza(nome_da_mappare, lista_scelte, soglia):
    if pd.isna(nome_da_mappare) or nome_da_mappare == "":
        return None, 0, None
    match = process.extractOne(nome_da_mappare, lista_scelte, score_cutoff=soglia)
    if match:
        lista_matchata_lower = match[0]
        punteggio = match[1]
        riga_riferimento = df_riferimento[df_riferimento[colonna_lista_riferimento + '_lower'] == lista_matchata_lower].iloc[0]
        lista_originale_matchata = riga_riferimento[colonna_lista_riferimento]
        partito_ufficiale_matchato = riga_riferimento[colonna_partito_ufficiale_riferimento] if colonna_partito_ufficiale_riferimento else lista_originale_matchata
        return lista_originale_matchata, punteggio, partito_ufficiale_matchato
    return None, 0, None

risultati_mapping = df_principale_filtrato[colonna_partito_da_mappare + '_lower'].apply(
    lambda x: trova_migliore_corrispondenza(x, scelte_liste_lower, soglia_similarita)
)

df_principale_filtrato['lista_mappata_fuzzy'] = risultati_mapping.apply(lambda x: x[0])
df_principale_filtrato['punteggio_fuzzy'] = risultati_mapping.apply(lambda x: x[1])
df_principale_filtrato['partito_mappato_ufficiale'] = risultati_mapping.apply(lambda x: x[2])

df_principale = df_principale.merge(
    df_principale_filtrato[['NOME_POLITICO_REALE', 'lista_mappata_fuzzy', 'punteggio_fuzzy', 'partito_mappato_ufficiale']], 
    on='NOME_POLITICO_REALE', 
    how='left'
)

df_principale.drop(columns=[colonna_partito_da_mappare + '_lower'], inplace=True, errors='ignore')
df_riferimento.drop(columns=[colonna_lista_riferimento + '_lower'], inplace=True, errors='ignore')

output_file_path = 'src/output-data/risultato_mappatura_fuzzy.csv'
df_principale.to_csv(output_file_path, index=False, encoding='utf-8')

df_match_validi = df_principale[
    (df_principale['MATCHED'] == True) & 
    (df_principale['NOME_POLITICO_REALE'].notna()) & 
    (df_principale['NOME_POLITICO_REALE'] != '') &
    (df_principale['partito_mappato_ufficiale'].notna())
].copy()

df_finale_mapping = df_match_validi[['NOME_POLITICO_REALE', 'PARTITO_REALE', 'partito_mappato_ufficiale', 'punteggio_fuzzy']].copy()
df_finale_mapping.rename(columns={
    'NOME_POLITICO_REALE': 'NOME_POLITICO',
    'PARTITO_REALE': 'PARTITO_ORIGINALE', 
    'partito_mappato_ufficiale': 'PARTITO_UFFICIALE',
    'punteggio_fuzzy': 'MATCH_SCORE_PARTITO'
}, inplace=True)

df_finale_mapping = df_finale_mapping.sort_values('MATCH_SCORE_PARTITO', ascending=False).drop_duplicates('NOME_POLITICO', keep='first')

output_finale_path = 'src/output-data/mapping_politici_partiti_finale.csv'
df_finale_mapping.to_csv(output_finale_path, index=False, encoding='utf-8')