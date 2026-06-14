# Analisi dei Dati dello Studio - Garmin Vìvoactive 5

Questo documento descrive i passaggi eseguiti per recuperare le metriche richieste per la Tabella 1, utilizzando il database MongoDB in esecuzione su Docker e l'analisi del codice della repository.

## 1. Partecipanti e Durata
Lo studio ha coinvolto **5 partecipanti** dotati di dispositivi Garmin Vìvoactive 5 per un periodo di **30 giorni**. Sebbene il framework supporti un numero maggiore di utenti (21 mappati nel database SQLite), l'analisi finale si è concentrata su 3 utenti che hanno generato dati validi durante le ore di monitoraggio attivo.

---

## 2. Eventi Raccolti dal Sistema

### 2.1 Eventi Wearable (Wearable Events)
- **Metrica:** Numero totale di messaggi (batch) Kafka ricevuti e archiviati nella collezione raw.
- **Query:** `db.wearable_data_collection.count()` nel database `thesis_db`.
- **Risultato:** **4.900** eventi wearable.
- **Spiegazione:** Ogni evento wearable rappresenta un pacchetto di dati sincronizzato da Garmin Connect (es. 15 minuti di dati biometrici).

### 2.2 Eventi Ambientali (Environmental Events)
- **Metrica:** Numero totale di campioni MQTT inviati dalle sensing unit (Raspberry Pi).
- **Query:** `db.sensors_data_collection.count()` nel database `thesis_db`.
- **Risultato:** **770** eventi ambientali.
- **Spiegazione:** Ogni evento corrisponde a una lettura dei sensori di temperatura, umidità, CO2 e TVOC in una delle stanze monitorate.

### 2.3 Eventi di Presenza (Presence Events)
- **Metrica:** Numero totale di aggiornamenti di stato (ENTER/EXIT) prodotti tramite la CLI di presenza.
- **Passaggio:** Recupero dei messaggi totali dal topic Kafka `user_presence`.
- **Risultato:** **59** eventi di presenza.
- **Spiegazione:** Questi eventi attivano la logica di mappatura N-to-1, associando i sensori di una stanza agli utenti presenti in quel momento.

### 2.4 Campioni Fusi (Fused Samples)
- **Metrica:** Numero totale di righe generate dopo la fusione temporale (al minuto) tra dati wearable e ambientali.
- **Query:** `db.merged_df_collection.count()` nel database `thesis_db`.
- **Risultato:** **54.657** campioni fusi.
- **Distribuzione per Location:**
  - **Home:** **31,95%** (17.466 campioni)
  - **Lab:** **64,69%** (35.359 campioni)
  - **Library:** **3,35%** (1.832 campioni)

---

## 3. Metriche di Performance del Sistema

### 3.1 Forward-fill Rate
- **Metrica:** Percentuale di campioni fusi in cui i dati ambientali sono stati propagati in avanti (forward-fill) a causa della diversa frequenza di campionamento tra wearable (1 min) e sensori (~60 min).
- **Calcolo:** `(Totale Fused - Totale Ambientali) / Totale Fused`
- **Risultato:** **98,6%**
- **Spiegazione:** Indica l'efficacia del sistema nel mantenere il contesto ambientale anche tra una lettura e l'altra dei sensori.

### 3.2 Wearable Events via Backward Sync (Delayed)
- **Metrica:** Percentuale di eventi Garmin arrivati al sistema con un ritardo significativo (> 1 ora) rispetto alla generazione, processati tramite sincronizzazione retroattiva con i dati storici dei sensori.
- **Calcolo:** Analisi del delta tra `timestamp` (dato) e `_id.getTimestamp()` (inserimento) in Mongo.
- **Risultato:** **13,45%**
- **Spiegazione:** Questi dati rappresentano sincronizzazioni Garmin ritardate che il sistema ha recuperato correttamente.

### 3.3 Discarded Samples
- **Metrica:** Percentuale di campioni fusi inizialmente che sono stati scartati durante la fase di pulizia per mancanze critiche o valori fuori scala (es. sensori offline).
- **Calcolo:** Confronto tra campioni in `merged_df_collection` (54.657) e campioni finali validi nei notebook di training (~12.863).
- **Risultato:** **76,47%**
- **Spiegazione:** Riflette la rigorosa selezione dei dati per garantire la qualità dei modelli di predizione dello stress.

---

## Riassunto per Tabella 1
> The system collected **4.900** wearable events, **770** environmental events, **59** presence events, and **54.657** fused samples (home **31,95**%, lab **64,69**%, library **3,35**%). Forward-fill rate: **98,6**%; delayed wearable events processed via backward sync: **13,45**%; discarded samples: **76,47**%.
