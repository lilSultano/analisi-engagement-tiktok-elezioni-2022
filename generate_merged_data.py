#!/usr/bin/env python3
"""
🔄 SCRIPT PREPROCESSING: GENERAZIONE MERGED_DATA.CSV
===============================================
Questo script esegue tutto il preprocessing dei dati:
- Carica dati social e elettorali
- Pulisce e normalizza i nomi dei partiti
- Aggrega metriche per partito
- Crea il dataset finale merged_data.csv

Eseguire prima di aprire il notebook per avere i dati pronti.
"""

import pandas as pd
import numpy as np
import os
from pathlib import Path

def main():
    print("🚀 AVVIO PREPROCESSING DATI...")
    print("=" * 50)
    
    # 1. CARICAMENTO DATI
    print("\n📂 Caricamento dati...")
    
    try:
        # Dati social
        social_df = pd.read_csv('src/output-data/social_data_con_nomi_reali.csv')
        # Filtra solo i record matched
        social_df = social_df[social_df['MATCHED'] == True]
        print(f"✅ Social data caricati: {len(social_df)} righe (matched)")
        
        # Dati elezioni
        election_df = pd.read_csv('datasets/Politiche2022_Scrutini_Camera_Italia.csv')
        print(f"✅ Dati elettorali caricati: {len(election_df)} righe")
        
        # Mapping partiti (se esiste)
        mapping_file = 'src/output-data/mapping_politici_partiti_finale.csv'
        if os.path.exists(mapping_file):
            mapping_df = pd.read_csv(mapping_file)
            print(f"✅ Mapping partiti caricato: {len(mapping_df)} righe")
        else:
            mapping_df = None
            print("⚠️ File mapping non trovato, procedo senza")
            
    except Exception as e:
        print(f"❌ ERRORE nel caricamento: {e}")
        return
    
    # 2. PULIZIA E NORMALIZZAZIONE NOMI PARTITI
    print("\n🧹 Pulizia e normalizzazione nomi partiti...")
    
    def normalizza_nome_partito(nome):
        """Normalizza i nomi dei partiti per il matching"""
        if pd.isna(nome):
            return nome
        
        nome = str(nome).strip().upper()
        
        # Mapping delle normalizzazioni
        mappings = {
            'FRATELLI D\'ITALIA': 'FRATELLI D\'ITALIA',
            'FRATELLI D\'ITALIA CON GIORGIA MELONI': 'FRATELLI D\'ITALIA',
            'LEGA': 'LEGA',
            'LEGA PER SALVINI PREMIER': 'LEGA',
            'LEGA - SALVINI PREMIER': 'LEGA',
            'FORZA ITALIA': 'FORZA ITALIA',
            'PARTITO DEMOCRATICO': 'PARTITO DEMOCRATICO',
            'PD': 'PARTITO DEMOCRATICO',
            'MOVIMENTO 5 STELLE': 'MOVIMENTO 5 STELLE',
            'M5S': 'MOVIMENTO 5 STELLE',
            'AZIONE - ITALIA VIVA': 'AZIONE',
            'AZIONE': 'AZIONE',
            'ITALIA VIVA': 'AZIONE',
            'ALLEANZA VERDI E SINISTRA': 'ALLEANZA VERDI E SINISTRA',
            'VERDI': 'ALLEANZA VERDI E SINISTRA',
            'SINISTRA ITALIANA': 'ALLEANZA VERDI E SINISTRA',
            '+EUROPA': '+EUROPA',
            'PIU\' EUROPA': '+EUROPA',
            'IMPEGNO CIVICO': 'IMPEGNO CIVICO',
            'NOI MODERATI': 'NOI MODERATI',
            'ITALEXIT': 'ITALEXIT',
            'UNIONE POPOLARE': 'UNIONE POPOLARE',
            'VITA': 'VITA'
        }
        
        # Cerca match diretto
        for key, value in mappings.items():
            if key in nome:
                return value
                
        return nome
    
    # Applica normalizzazione
    social_df['PARTITO'] = social_df['PARTITO_REALE'].apply(normalizza_nome_partito)
    election_df['PARTITO'] = election_df['LISTA'].apply(normalizza_nome_partito)
    
    print(f"✅ Normalizzazione completata")
    
    # 3. AGGREGAZIONE DATI SOCIAL
    print("\n📱 Aggregazione dati social...")
    
    # Calcola metriche di engagement
    social_df['ENGAGEMENT'] = (social_df['video_diggcount'].fillna(0) + 
                              social_df['video_sharecount'].fillna(0) + 
                              social_df['video_commentcount'].fillna(0))
    
    # Aggrega per partito
    social_agg = social_df.groupby('PARTITO').agg({
        'video_id': 'count',
        'video_playcount': 'sum',
        'video_diggcount': 'sum',
        'video_sharecount': 'sum',
        'video_commentcount': 'sum',
        'ENGAGEMENT': 'sum'
    }).reset_index()
    
    # Rinomina colonne
    social_agg.columns = [
        'PARTITO', 'NUMERO_POST_TIKTOK', 'TOTALE_VIEWS_TIKTOK',
        'TOTALE_LIKES', 'TOTALE_SHARES', 'TOTALE_COMMENTS', 'ENGAGEMENT_TOTALE'
    ]
    
    print(f"✅ Aggregazione social completata: {len(social_agg)} partiti")
    
    # 4. AGGREGAZIONE DATI ELETTORALI
    print("\n🗳️ Aggregazione dati elettorali...")
    
    election_agg = election_df.groupby('PARTITO').agg({
        'VOTI LISTE': 'sum'
    }).reset_index()
    
    election_agg.columns = ['PARTITO', 'TOTALE_VOTI_LISTA']
    election_agg = election_agg[election_agg['TOTALE_VOTI_LISTA'] > 0]
    
    print(f"✅ Aggregazione elettorale completata: {len(election_agg)} partiti")
    
    # 5. MERGE E CREAZIONE DATASET FINALE
    print("\n🔗 Creazione dataset finale integrato...")
    
    merged_data = social_agg.merge(election_agg, on='PARTITO', how='inner')
    
    if len(merged_data) == 0:
        print("❌ ERRORE: Nessun partito in comune tra dati social e elettorali!")
        print("\nPartiti social:", social_agg['PARTITO'].tolist())
        print("\nPartiti elettorali (sample):", election_agg['PARTITO'].head(10).tolist())
        return
    
    # 6. CALCOLA PERCENTUALI E RANKING
    total_votes = merged_data['TOTALE_VOTI_LISTA'].sum()
    merged_data['PERCENTUALE_VOTI'] = (merged_data['TOTALE_VOTI_LISTA'] / total_votes) * 100
    merged_data = merged_data.sort_values('TOTALE_VOTI_LISTA', ascending=False).reset_index(drop=True)
    
    # 7. SALVATAGGIO CSV
    output_dir = Path('src/output-data')
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / 'merged_data.csv'
    
    merged_data.to_csv(output_file, index=False)
    
    print(f"\n✅ DATASET FINALE CREATO E SALVATO:")
    print(f"📂 File: {output_file}")
    print(f"📊 Partiti analizzati: {len(merged_data)}")
    print(f"🗳️ Voti totali: {merged_data['TOTALE_VOTI_LISTA'].sum():,}")
    print(f"📱 Post TikTok: {merged_data['NUMERO_POST_TIKTOK'].sum()}")
    print(f"👀 Views totali: {merged_data['TOTALE_VIEWS_TIKTOK'].sum():,}")
    print(f"💝 Engagement totale: {merged_data['ENGAGEMENT_TOTALE'].sum():,}")
    
    # 8. PREVIEW DATASET
    print(f"\n📋 PREVIEW DATASET FINALE:")
    preview_cols = ['PARTITO', 'TOTALE_VOTI_LISTA', 'PERCENTUALE_VOTI', 'NUMERO_POST_TIKTOK', 'ENGAGEMENT_TOTALE']
    print(merged_data[preview_cols].head(10).to_string(index=False))
    
    print(f"\n🎉 PREPROCESSING COMPLETATO!")
    print(f"📝 Ora puoi aprire il notebook e caricare direttamente: merged_data = pd.read_csv('src/output-data/merged_data.csv')")

if __name__ == "__main__":
    main()
