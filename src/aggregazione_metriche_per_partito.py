#!/usr/bin/env python3
"""
Script per il fuzzy matching tra nomi social media e nomi reali dei politici
Mappa author_username e author_name dai dati social ai nomi reali nel dataset dei rappresentanti
"""

import pandas as pd
from fuzzywuzzy import fuzz, process
import re
import sys
import os

def clean_name(name):
    """
    Pulisce e normalizza i nomi per il matching
    """
    if pd.isna(name) or name == '':
        return ''
    
    # Converti a stringa e rimuovi spazi extra
    name = str(name).strip()
    
    # Rimuovi caratteri speciali comuni nei social media
    name = re.sub(r'[_@#\d]', '', name)
    
    # Rimuovi emoji e simboli
    name = re.sub(r'[^\w\s]', '', name)
    
    # Rimuovi parole comuni social
    common_words = ['ufficiale', 'official', 'real', 'page', 'account']
    for word in common_words:
        name = re.sub(rf'\b{word}\b', '', name, flags=re.IGNORECASE)
    
    # Normalizza spazi
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name.lower()

def extract_possible_names(username, author_name):
    """
    Estrae possibili nomi da username e author_name
    """
    names = []
    
    # Aggiungi author_name se presente
    if pd.notna(author_name) and author_name.strip():
        names.append(clean_name(author_name))
    
    # Aggiungi username pulito
    if pd.notna(username) and username.strip():
        cleaned_username = clean_name(username)
        if cleaned_username:
            names.append(cleaned_username)
    
    # Prova anche a separare il username con vari delimitatori
    if pd.notna(username) and username.strip():
        # Separa per underscore, punti, numeri
        parts = re.split(r'[._\d]+', username)
        for part in parts:
            if len(part) > 2:  # Solo parti significative
                cleaned_part = clean_name(part)
                if cleaned_part and cleaned_part not in names:
                    names.append(cleaned_part)
    
    return list(set([n for n in names if n]))  # Rimuovi duplicati e vuoti

def fuzzy_match_politician(social_names, politicians_df, threshold=70):
    """
    Trova il miglior match tra i nomi social e i politici reali
    """
    if not social_names:
        return None, 0
    
    best_match = None
    best_score = 0
    best_politician = None
    
    # Lista dei nomi reali dei politici
    politician_names = politicians_df['Name'].fillna('').tolist()
    
    for social_name in social_names:
        if not social_name:
            continue
            
        # Trova il miglior match per questo nome social
        match_result = process.extractOne(social_name, politician_names, scorer=fuzz.token_sort_ratio)
        
        if match_result and match_result[1] > best_score and match_result[1] >= threshold:
            best_score = match_result[1]
            best_match = match_result[0]
            # Trova i dati completi del politico
            best_politician = politicians_df[politicians_df['Name'] == best_match].iloc[0]
    
    return best_politician, best_score

def main():
    """
    Funzione principale per eseguire il fuzzy matching
    """
    print("🚀 Avvio script fuzzy matching username -> nomi politici...")
    
    # Percorsi dei file
    social_data_path = 'datasets/Facebook_Tiktok_metadata.csv'
    politicians_path = 'datasets/twitter_representatives_handles.csv'
    output_path = 'src/output-data/social_data_con_nomi_reali.csv'
    
    # Verifica esistenza file
    if not os.path.exists(social_data_path):
        print(f"❌ File non trovato: {social_data_path}")
        return
    
    if not os.path.exists(politicians_path):
        print(f"❌ File non trovato: {politicians_path}")
        return
    
    # Carica i dati
    print("📂 Caricamento dati...")
    try:
        social_df = pd.read_csv(social_data_path)
        politicians_df = pd.read_csv(politicians_path)
    except Exception as e:
        print(f"❌ Errore nel caricamento: {e}")
        return
    
    print(f"📊 Dati social caricati: {len(social_df)} righe")
    print(f"👥 Politici nel dataset: {len(politicians_df)} righe")
    
    # Prepara il DataFrame risultato
    result_df = social_df.copy()
    
    # Aggiungi colonne per i risultati del matching
    result_df['NOME_POLITICO_REALE'] = ''
    result_df['TWITTER_HANDLE_REALE'] = ''
    result_df['PARTITO_REALE'] = ''
    result_df['GENERE'] = ''
    result_df['MATCH_SCORE'] = 0
    result_df['MATCHED'] = False
    
    # Statistiche
    total_rows = len(social_df)
    matched_count = 0
    
    print("\n🔍 Inizio fuzzy matching...")
    
    # Raggruppa per author_username e author_name per efficienza
    unique_authors = social_df[['author_username', 'author_name']].drop_duplicates()
    
    # Dizionario per cachare i risultati del matching
    match_cache = {}
    
    for idx, author in unique_authors.iterrows():
        username = author['author_username']
        author_name = author['author_name']
        
        # Crea chiave cache
        cache_key = f"{username}|{author_name}"
        
        if cache_key in match_cache:
            continue
        
        # Estrai possibili nomi
        possible_names = extract_possible_names(username, author_name)
        
        if not possible_names:
            match_cache[cache_key] = {
                'matched': False,
                'score': 0
            }
            continue
        
        # Esegui fuzzy matching
        best_politician, score = fuzzy_match_politician(possible_names, politicians_df)
        
        if best_politician is not None:
            match_cache[cache_key] = {
                'matched': True,
                'nome_reale': best_politician['Name'],
                'twitter_handle': best_politician['Twitter-Handle'],
                'partito': best_politician['Party'],
                'genere': best_politician['Gender'],
                'score': score
            }
            matched_count += 1
            print(f"✅ Match trovato: {username} -> {best_politician['Name']} ({score}%)")
        else:
            match_cache[cache_key] = {
                'matched': False,
                'score': 0
            }
    
    # Applica i risultati del matching al DataFrame completo
    print("\n📝 Applicazione risultati...")
    
    for idx, row in result_df.iterrows():
        cache_key = f"{row['author_username']}|{row['author_name']}"
        
        if cache_key in match_cache and match_cache[cache_key]['matched']:
            match_data = match_cache[cache_key]
            result_df.at[idx, 'NOME_POLITICO_REALE'] = match_data['nome_reale']
            result_df.at[idx, 'TWITTER_HANDLE_REALE'] = match_data['twitter_handle']
            result_df.at[idx, 'PARTITO_REALE'] = match_data['partito']
            result_df.at[idx, 'GENERE'] = match_data['genere']
            result_df.at[idx, 'MATCH_SCORE'] = match_data['score']
            result_df.at[idx, 'MATCHED'] = True
    
    # Salva risultati
    print(f"\n💾 Salvataggio risultati in: {output_path}")
    result_df.to_csv(output_path, index=False)
    
    # Statistiche finali
    matched_rows = len(result_df[result_df['MATCHED'] == True])
    unique_politicians = len(result_df[result_df['MATCHED'] == True]['NOME_POLITICO_REALE'].unique())
    
    print(f"\n📈 STATISTICHE FINALI:")
    print(f"Total righe processate: {total_rows}")
    print(f"Righe con match trovato: {matched_rows} ({matched_rows/total_rows*100:.1f}%)")
    print(f"Autori unici mappati: {matched_count}")
    print(f"Politici reali identificati: {unique_politicians}")
    
    # Mostra alcuni esempi di match
    print(f"\n🎯 ESEMPI DI MATCH TROVATI:")
    examples = result_df[result_df['MATCHED'] == True][
        ['author_username', 'author_name', 'NOME_POLITICO_REALE', 'PARTITO_REALE', 'MATCH_SCORE']
    ].drop_duplicates().head(10)
    
    for _, ex in examples.iterrows():
        print(f"  {ex['author_username']} | {ex['author_name']} -> {ex['NOME_POLITICO_REALE']} ({ex['PARTITO_REALE']}) [{ex['MATCH_SCORE']}%]")
    
    print(f"\n✅ Processo completato! File salvato: {output_path}")

if __name__ == "__main__":
    main()