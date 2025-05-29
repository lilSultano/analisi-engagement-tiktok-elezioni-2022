#!/usr/bin/env python3
"""
Script per il fuzzy matching tra nomi social media e nomi reali dei politici
Mappa author_username e author_name dai dati social ai nomi reali nel dataset dei rappresentanti
"""

import pandas as pd
from fuzzywuzzy import fuzz, process
import re
import os

def clean_name(name):
    if pd.isna(name) or name == '':
        return ''
    name = str(name).strip()
    name = re.sub(r'[_@#\d]', '', name)
    name = re.sub(r'[^\w\s]', '', name)
    common_words = ['ufficiale', 'official', 'real', 'page', 'account']
    for word in common_words:
        name = re.sub(rf'\b{word}\b', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s+', ' ', name).strip()
    return name.lower()

def extract_possible_names(username, author_name):
    names = []
    if pd.notna(author_name) and author_name.strip():
        names.append(clean_name(author_name))
    if pd.notna(username) and username.strip():
        cleaned_username = clean_name(username)
        if cleaned_username:
            names.append(cleaned_username)
        parts = re.split(r'[._\d]+', username)
        for part in parts:
            if len(part) > 2:
                cleaned_part = clean_name(part)
                if cleaned_part and cleaned_part not in names:
                    names.append(cleaned_part)
    return list(set([n for n in names if n]))

def fuzzy_match_politician(social_names, politicians_df, threshold=70):
    if not social_names:
        return None, 0
    best_score = 0
    best_politician = None
    politician_names = politicians_df['Name'].fillna('').tolist()
    for social_name in social_names:
        if not social_name:
            continue
        match_result = process.extractOne(social_name, politician_names, scorer=fuzz.token_sort_ratio)
        if match_result and match_result[1] > best_score and match_result[1] >= threshold:
            best_score = match_result[1]
            best_politician = politicians_df[politicians_df['Name'] == match_result[0]].iloc[0]
    return best_politician, best_score

def main():
    social_data_path = 'datasets/Facebook_Tiktok_metadata.csv'
    politicians_path = 'datasets/twitter_representatives_handles.csv'
    output_path = 'src/output-data/social_data_con_nomi_reali.csv'

    if not os.path.exists(social_data_path) or not os.path.exists(politicians_path):
        return

    try:
        social_df = pd.read_csv(social_data_path)
        politicians_df = pd.read_csv(politicians_path)
    except Exception:
        return

    result_df = social_df.copy()
    result_df['NOME_POLITICO_REALE'] = ''
    result_df['TWITTER_HANDLE_REALE'] = ''
    result_df['PARTITO_REALE'] = ''
    result_df['GENERE'] = ''
    result_df['MATCH_SCORE'] = 0
    result_df['MATCHED'] = False

    unique_authors = social_df[['author_username', 'author_name']].drop_duplicates()
    match_cache = {}

    for _, author in unique_authors.iterrows():
        username = author['author_username']
        author_name = author['author_name']
        cache_key = f"{username}|{author_name}"
        if cache_key in match_cache:
            continue
        possible_names = extract_possible_names(username, author_name)
        if not possible_names:
            match_cache[cache_key] = {'matched': False, 'score': 0}
            continue
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
        else:
            match_cache[cache_key] = {'matched': False, 'score': 0}

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

    result_df.to_csv(output_path, index=False)

if __name__ == "__main__":
    main()