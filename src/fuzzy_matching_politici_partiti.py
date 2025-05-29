import pandas as pd
from fuzzywuzzy import process


# --- Configurazione ---
file_principale_path = 'src/output-data/social_data_con_nomi_reali.csv'
colonna_partito_da_mappare = 'PARTITO_REALE' # Colonna nel file principale

file_riferimento_path = 'datasets/Politiche2022_Scrutini_Camera_Italia.csv'
colonna_lista_riferimento = 'LISTA' # Colonna nel file di riferimento da usare per il fuzzy match
# Se vuoi mappare a un nome di partito "ufficiale" presente nel file di riferimento:
colonna_partito_ufficiale_riferimento = 'LISTA' # Opzionale

soglia_similarita = 70  # Percentuale (0-100). Match sotto questa soglia potrebbero essere ignorati o revisionati.
# Soglia ridotta per catturare più variazioni dei nomi dei partiti

# --- Caricamento CSV ---
try:
    df_principale = pd.read_csv(file_principale_path)
    df_riferimento = pd.read_csv(file_riferimento_path)
except FileNotFoundError as e:
    print(f"Errore: File non trovato - {e}")
    exit()
except Exception as e:
    print(f"Errore durante il caricamento dei CSV: {e}")
    exit()

# --- Preparazione dati per il fuzzy matching ---
# Assicurati che le colonne esistano
if colonna_partito_da_mappare not in df_principale.columns:
    print(f"Errore: Colonna '{colonna_partito_da_mappare}' non trovata in '{file_principale_path}'")
    exit()
if colonna_lista_riferimento not in df_riferimento.columns:
    print(f"Errore: Colonna '{colonna_lista_riferimento}' non trovata in '{file_riferimento_path}'")
    exit()
if colonna_partito_ufficiale_riferimento and colonna_partito_ufficiale_riferimento not in df_riferimento.columns:
    print(f"Attenzione: Colonna partito ufficiale '{colonna_partito_ufficiale_riferimento}' non trovata in '{file_riferimento_path}'. Si userà la colonna lista.")
    colonna_partito_ufficiale_riferimento = None # Disabilita se non trovata

# Filtra solo i record con nomi politici reali validi e che sono stati matchati
df_principale_filtrato = df_principale[
    (df_principale['MATCHED'] == True) & 
    (df_principale['NOME_POLITICO_REALE'].notna()) & 
    (df_principale['NOME_POLITICO_REALE'] != '')
].copy()

print(f"Record con politici validi da processare: {len(df_principale_filtrato)}")

# Pulisci e prendi le liste uniche dal CSV di riferimento
# Converti in stringa, metti in minuscolo e togli spazi extra per migliorare il matching
df_riferimento[colonna_lista_riferimento + '_lower'] = df_riferimento[colonna_lista_riferimento].astype(str).str.lower().str.strip()
scelte_liste_lower = df_riferimento[colonna_lista_riferimento + '_lower'].unique().tolist()

# Prepara la colonna da mappare nel df principale (lower, strip)
# Gestisci i casi in cui PARTITO_REALE potrebbe essere vuoto o NaN
df_principale_filtrato[colonna_partito_da_mappare + '_lower'] = df_principale_filtrato[colonna_partito_da_mappare].fillna('').astype(str).str.lower().str.strip()

# --- Funzione per trovare la migliore corrispondenza ---
def trova_migliore_corrispondenza(nome_da_mappare, lista_scelte, soglia):
    if pd.isna(nome_da_mappare) or nome_da_mappare == "":
        return None, 0, None # (lista_matchata, punteggio, partito_ufficiale_matchato)

    # process.extractOne restituisce una tupla (scelta_migliore, punteggio)
    match = process.extractOne(nome_da_mappare, lista_scelte, score_cutoff=soglia)

    if match:
        lista_matchata_lower = match[0]
        punteggio = match[1]

        # Trova la riga originale in df_riferimento per ottenere la 'lista' originale e 'partito_ufficiale'
        riga_riferimento = df_riferimento[df_riferimento[colonna_lista_riferimento + '_lower'] == lista_matchata_lower].iloc[0]
        lista_originale_matchata = riga_riferimento[colonna_lista_riferimento]

        partito_ufficiale_matchato = None
        if colonna_partito_ufficiale_riferimento:
            partito_ufficiale_matchato = riga_riferimento[colonna_partito_ufficiale_riferimento]
        else: # Se non c'è colonna partito ufficiale, usa la lista stessa
            partito_ufficiale_matchato = lista_originale_matchata

        return lista_originale_matchata, punteggio, partito_ufficiale_matchato
    return None, 0, None

# --- Applicazione del fuzzy matching ---
print("Inizio mappatura fuzzy...")

# Applica la funzione e spacchetta i risultati in nuove colonne
risultati_mapping = df_principale_filtrato[colonna_partito_da_mappare + '_lower'].apply(
    lambda x: trova_migliore_corrispondenza(x, scelte_liste_lower, soglia_similarita)
)

df_principale_filtrato['lista_mappata_fuzzy'] = risultati_mapping.apply(lambda x: x[0])
df_principale_filtrato['punteggio_fuzzy'] = risultati_mapping.apply(lambda x: x[1])
df_principale_filtrato['partito_mappato_ufficiale'] = risultati_mapping.apply(lambda x: x[2])

# Merge dei risultati con il dataframe originale
df_principale = df_principale.merge(
    df_principale_filtrato[['NOME_POLITICO_REALE', 'lista_mappata_fuzzy', 'punteggio_fuzzy', 'partito_mappato_ufficiale']], 
    on='NOME_POLITICO_REALE', 
    how='left'
)


# --- Revisione e Salvataggio ---
print("\nAnteprima dei risultati del mapping:")
colonne_da_mostrare = [colonna_partito_da_mappare, 'lista_mappata_fuzzy', 'punteggio_fuzzy', 'partito_mappato_ufficiale']
print(df_principale[colonne_da_mostrare].head())

# Mostra i casi non mappati o con basso punteggio per revisione
print(f"\nPartiti non mappati o con punteggio < {soglia_similarita} (da revisionare):")
revisione_necessaria = df_principale[
    (df_principale['punteggio_fuzzy'] < soglia_similarita) | (df_principale['punteggio_fuzzy'] == 0)
]
if not revisione_necessaria.empty:
    print(revisione_necessaria[colonne_da_mostrare])
else:
    print(f"Nessun partito richiede revisione urgente (tutti mappati con punteggio >= {soglia_similarita}).")

# Rimuovi la colonna temporanea in lowercase
df_principale.drop(columns=[colonna_partito_da_mappare + '_lower'], inplace=True, errors='ignore')
df_riferimento.drop(columns=[colonna_lista_riferimento + '_lower'], inplace=True, errors='ignore')


output_file_path = 'src/output-data/risultato_mappatura_fuzzy.csv'
df_principale.to_csv(output_file_path, index=False, encoding='utf-8')
print(f"\nRisultato salvato in: {output_file_path}")

# --- Creazione file finale con mapping politici-partiti ---
print("\nCreazione file finale con mapping politici-partiti...")

# Filtra solo i record con match validi e nomi politici reali
df_match_validi = df_principale[
    (df_principale['MATCHED'] == True) & 
    (df_principale['NOME_POLITICO_REALE'].notna()) & 
    (df_principale['NOME_POLITICO_REALE'] != '') &
    (df_principale['partito_mappato_ufficiale'].notna())
].copy()

# Crea un DataFrame finale con colonne essenziali
df_finale_mapping = df_match_validi[['NOME_POLITICO_REALE', 'PARTITO_REALE', 'partito_mappato_ufficiale', 'punteggio_fuzzy']].copy()
df_finale_mapping.rename(columns={
    'NOME_POLITICO_REALE': 'NOME_POLITICO',
    'PARTITO_REALE': 'PARTITO_ORIGINALE', 
    'partito_mappato_ufficiale': 'PARTITO_UFFICIALE',
    'punteggio_fuzzy': 'MATCH_SCORE_PARTITO'
}, inplace=True)

# Rimuovi duplicati mantenendo il record con punteggio più alto
df_finale_mapping = df_finale_mapping.sort_values('MATCH_SCORE_PARTITO', ascending=False).drop_duplicates('NOME_POLITICO', keep='first')

# Mostra statistiche
print(f"Politici con mapping valido: {len(df_finale_mapping)}")
print(f"Partiti ufficiali mappati: {df_finale_mapping['PARTITO_UFFICIALE'].nunique()}")

# Salva il file finale
output_finale_path = 'src/output-data/mapping_politici_partiti_finale.csv'
df_finale_mapping.to_csv(output_finale_path, index=False, encoding='utf-8')
print(f"Mapping finale salvato in: {output_finale_path}")

print(f"\nAnteprima mapping finale:")
print(df_finale_mapping.head(10))

print("\nCompletato.")