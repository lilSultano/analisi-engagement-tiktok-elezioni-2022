#!/usr/bin/env python3

import pandas as pd
import numpy as np
import os
from pathlib import Path

def main():
    try:
        social_df = pd.read_csv('src/output-data/social_data_con_nomi_reali.csv')
        social_df = social_df[social_df['MATCHED'] == True]
        
        election_df = pd.read_csv('datasets/Politiche2022_Scrutini_Camera_Italia.csv')
        
        mapping_file = 'src/output-data/mapping_politici_partiti_finale.csv'
        if os.path.exists(mapping_file):
            mapping_df = pd.read_csv(mapping_file)
        else:
            mapping_df = None
            
    except Exception:
        return

    def normalizza_nome_partito(nome):
        if pd.isna(nome):
            return nome
        nome = str(nome).strip().upper()
        mappings = {
            'FRATELLI D\'ITALIA': 'FRATELLI D\'ITALIA',
            'FRATELLI D\'ITALIA CON GIORGIA MELONI': 'FRATELLI D\'ITALIA',
            'FRATELLI D\'ITALIA - ALLEANZA NAZIONALE': 'FRATELLI D\'ITALIA',
            'LEGA': 'LEGA',
            'LEGA PER SALVINI PREMIER': 'LEGA',
            'LEGA - SALVINI PREMIER': 'LEGA',
            'LEGA SALVINI PREMIER': 'LEGA',
            'FORZA ITALIA': 'FORZA ITALIA',
            'FORZA ITALIA - BERLUSCONI PRESIDENTE': 'FORZA ITALIA',
            'PARTITO DEMOCRATICO': 'PARTITO DEMOCRATICO',
            'PD': 'PARTITO DEMOCRATICO',
            'PARTITO DEMOCRATICO - ITALIA DEMOCRATICA E PROGRESSISTA': 'PARTITO DEMOCRATICO',
            'MOVIMENTO 5 STELLE': 'MOVIMENTO 5 STELLE',
            'M5S': 'MOVIMENTO 5 STELLE',
            'MOVIMENTO CINQUE STELLE': 'MOVIMENTO 5 STELLE',
            'MOVIMENTO 5STELLE': 'MOVIMENTO 5 STELLE',
            '5 STELLE': 'MOVIMENTO 5 STELLE',
            'CINQUE STELLE': 'MOVIMENTO 5 STELLE',
            'MOVIMIENTO 5 STELLE': 'MOVIMENTO 5 STELLE',
            'AZIONE - ITALIA VIVA': 'AZIONE',
            'AZIONE': 'AZIONE',
            'ITALIA VIVA': 'AZIONE',
            'AZIONE - ITALIA VIVA - CALENDA': 'AZIONE',
            'ALLEANZA VERDI E SINISTRA': 'ALLEANZA VERDI E SINISTRA',
            'VERDI': 'ALLEANZA VERDI E SINISTRA',
            'SINISTRA ITALIANA': 'ALLEANZA VERDI E SINISTRA',
            'EUROPA VERDE': 'ALLEANZA VERDI E SINISTRA',
            'SINISTRA ITALIANA - VERDI': 'ALLEANZA VERDI E SINISTRA',
            '+EUROPA': '+EUROPA',
            'PIU\' EUROPA': '+EUROPA',
            'PIÙ EUROPA': '+EUROPA',
            'IMPEGNO CIVICO': 'IMPEGNO CIVICO',
            'NOI MODERATI': 'NOI MODERATI',
            'NOI CON L\'ITALIA': 'NOI MODERATI',
            'ITALEXIT': 'ITALEXIT',
            'UNIONE POPOLARE': 'UNIONE POPOLARE',
            'VITA': 'VITA'
        }
        if nome in mappings:
            return mappings[nome]
        for key, value in mappings.items():
            if key in nome or nome in key:
                return value
        if any(pattern in nome for pattern in ['5 STELLE', 'STELLE', 'M5S', 'CINQUE']):
            return 'MOVIMENTO 5 STELLE'
        return nome

    social_df['PARTITO'] = social_df['PARTITO_REALE'].apply(normalizza_nome_partito)
    election_df['PARTITO'] = election_df['LISTA'].apply(normalizza_nome_partito)

    social_df['ENGAGEMENT'] = (
        social_df['video_diggcount'].fillna(0) +
        social_df['video_sharecount'].fillna(0) +
        social_df['video_commentcount'].fillna(0)
    )

    social_agg = social_df.groupby('PARTITO').agg({
        'video_id': 'count',
        'video_playcount': 'sum',
        'video_diggcount': 'sum',
        'video_sharecount': 'sum',
        'video_commentcount': 'sum',
        'ENGAGEMENT': 'sum'
    }).reset_index()

    social_agg.columns = [
        'PARTITO', 'NUMERO_POST_TIKTOK', 'TOTALE_VIEWS_TIKTOK',
        'TOTALE_LIKES', 'TOTALE_SHARES', 'TOTALE_COMMENTS', 'ENGAGEMENT_TOTALE'
    ]

    election_agg = election_df.groupby('PARTITO').agg({
        'VOTI LISTE': 'sum'
    }).reset_index()

    election_agg.columns = ['PARTITO', 'TOTALE_VOTI_LISTA']
    election_agg = election_agg[election_agg['TOTALE_VOTI_LISTA'] > 0]

    merged_data = social_agg.merge(election_agg, on='PARTITO', how='inner')

    if len(merged_data) == 0:
        return

    total_votes = merged_data['TOTALE_VOTI_LISTA'].sum()
    merged_data['PERCENTUALE_VOTI'] = (merged_data['TOTALE_VOTI_LISTA'] / total_votes) * 100
    merged_data = merged_data.sort_values('TOTALE_VOTI_LISTA', ascending=False).reset_index(drop=True)

    output_dir = Path('src/output-data')
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / 'merged_data.csv'
    merged_data.to_csv(output_file, index=False)

if __name__ == "__main__":
    main()