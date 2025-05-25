import pandas as pd
print("\n=== AGGREGAZIONE DATI CON MAPPING MIGLIORATO ===")
print("Utilizzo del file 'mapping_politici_partiti_finale.csv' per un'aggregazione più accurata")

try:
    # 1. Carica il mapping finale politici -> partiti ufficiali
    df_mapping_finale = pd.read_csv('src/output-data/mapping_politici_partiti_finale.csv')
    print(f"✅ Mapping finale caricato: {len(df_mapping_finale)} politici mappati")
    print(f"📊 Partiti ufficiali nel mapping: {df_mapping_finale['PARTITO_UFFICIALE'].nunique()}")
    
    # 2. Carica i dati social con nomi reali
    df_social_reali = pd.read_csv('src/output-data/social_data_con_nomi_reali.csv')
    print(f"📱 Dati social caricati: {len(df_social_reali)} righe")
    
    # 3. Filtra solo i record con match validi
    df_social_matched = df_social_reali[
        (df_social_reali['MATCHED'] == True) & 
        (df_social_reali['NOME_POLITICO_REALE'].notna()) & 
        (df_social_reali['NOME_POLITICO_REALE'] != '')
    ].copy()
    
    print(f"🎯 Record con politici validi: {len(df_social_matched)}")
    
    # 4. Merge con il mapping finale per ottenere i partiti ufficiali
    df_social_finale = df_social_matched.merge(
        df_mapping_finale[['NOME_POLITICO', 'PARTITO_UFFICIALE', 'MATCH_SCORE_PARTITO']], 
        left_on='NOME_POLITICO_REALE', 
        right_on='NOME_POLITICO', 
        how='left'
    )
    
    # 5. Filtra solo i record con partito ufficiale
    df_social_finale = df_social_finale[
        df_social_finale['PARTITO_UFFICIALE'].notna()
    ].copy()
    
    print(f"🏛️ Record con partiti ufficiali: {len(df_social_finale)}")
    print(f"🗳️ Partiti ufficiali trovati: {df_social_finale['PARTITO_UFFICIALE'].nunique()}")
    
    # 6. Aggregazione per partito ufficiale
    print("\n📊 Aggregazione metriche per partito ufficiale...")
    
    social_metrics_agg = df_social_finale.groupby('PARTITO_UFFICIALE').agg({
        'video_id': 'count',  # Numero post
        'video_playcount': 'sum',  # Totale views
        'video_diggcount': 'sum',  # Totale likes
        'video_commentcount': 'sum',  # Totale commenti
        'video_sharecount': 'sum',  # Totale shares
        'NOME_POLITICO_REALE': 'nunique'  # Numero politici unici
    }).reset_index()
    
    # 7. Rinomina colonne per coerenza
    social_metrics_agg.rename(columns={
        'PARTITO_UFFICIALE': 'PARTITO',
        'video_id': 'NUMERO_POST_TIKTOK',
        'video_playcount': 'TOTALE_VIEWS_TIKTOK',
        'video_diggcount': 'TOTALE_LIKES_TIKTOK',
        'video_commentcount': 'TOTALE_COMMENTI_TIKTOK',
        'video_sharecount': 'TOTALE_SHARES_TIKTOK',
        'NOME_POLITICO_REALE': 'NUMERO_POLITICI_MAPPATI'
    }, inplace=True)
    
    # 8. Calcola metriche aggiuntive
    social_metrics_agg['ENGAGEMENT_TOTALE'] = (
        social_metrics_agg['TOTALE_LIKES_TIKTOK'] + 
        social_metrics_agg['TOTALE_COMMENTI_TIKTOK'] + 
        social_metrics_agg['TOTALE_SHARES_TIKTOK']
    )
    
    social_metrics_agg['ENGAGEMENT_PER_POST'] = (
        social_metrics_agg['ENGAGEMENT_TOTALE'] / social_metrics_agg['NUMERO_POST_TIKTOK']
    ).fillna(0)
    
    social_metrics_agg['VIEWS_PER_POST'] = (
        social_metrics_agg['TOTALE_VIEWS_TIKTOK'] / social_metrics_agg['NUMERO_POST_TIKTOK']
    ).fillna(0)
    
    # 9. Riempi NaN con 0
    social_metrics_agg = social_metrics_agg.fillna(0)
    
    # 10. Mostra statistiche
    print(f"\n📈 STATISTICHE AGGREGAZIONE MIGLIORATA:")
    print(f"Partiti con attività social: {len(social_metrics_agg)}")
    print(f"Post totali analizzati: {social_metrics_agg['NUMERO_POST_TIKTOK'].sum():,}")
    print(f"Views totali: {social_metrics_agg['TOTALE_VIEWS_TIKTOK'].sum():,}")
    print(f"Engagement totale: {social_metrics_agg['ENGAGEMENT_TOTALE'].sum():,}")
    print(f"Politici mappati: {social_metrics_agg['NUMERO_POLITICI_MAPPATI'].sum()}")
    
    # 11. Top 10 partiti per engagement
    print(f"\n🏆 TOP 10 PARTITI PER ENGAGEMENT:")
    top_engagement = social_metrics_agg.sort_values('ENGAGEMENT_TOTALE', ascending=False).head(10)
    for idx, row in top_engagement.iterrows():
        print(f"{row['PARTITO']}: {row['ENGAGEMENT_TOTALE']:,} (da {row['NUMERO_POLITICI_MAPPATI']} politici)")
    
    # 12. Salva l'aggregazione migliorata
    social_metrics_agg.to_csv('src/output-data/social_metrics_aggregated_improved.csv', index=False)
    print(f"\n💾 Aggregazione salvata in: src/output-data/social_metrics_aggregated_improved.csv")

    # 13. Aggiorna la variabile social_activity per compatibilità
    social_activity = social_metrics_agg.copy()
    print(f"\n✅ Variabile 'social_activity' aggiornata con {len(social_activity)} partiti")
    
except FileNotFoundError as e:
    print(f"❌ Errore: File non trovato - {e}")
    print("Assicurati di aver eseguito gli script merger-partiti.py e merger.py")
except Exception as e:
    print(f"❌ Errore durante l'aggregazione: {e}")
    import traceback
    traceback.print_exc()