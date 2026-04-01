# pg_flashback — Production Readiness Roadmap

> Son güncelleme: 2026-04-01
> Mevcut durum: 14/14 test geçiyor, Faz 1 + Faz 2 tamamlandı

---

## Faz 1: Güvenlik ✅ TAMAMLANDI

Bu düzeltmeler olmadan hiçbir ciddi DBA production'da kullanmaz.

### 1.1 — Concurrent Restore Kilidi ✅
- **Sorun:** `restore_in_progress` tek bir AtomicBool. İki session aynı anda restore çağırırsa biri diğerinin bayrağını ezer → DDL hook yanlış zamanda devreye girer, veri bozulur.
- **Çözüm:** Restore başında `pg_advisory_xact_lock()` al. Transaction bitince otomatik serbest kalır.
- **Dosya:** `src/restore/planner.rs` — flashback_restore fonksiyonunun başı
- **Test:** İki concurrent session ile restore dene, ikincisi beklemeli.

### 1.2 — Worker Restore Guard ✅
- **Sorun:** Background worker, restore sırasında checkpoint alabilir ve yarım kalmış tablo state'ini snapshot'lar → bozuk checkpoint. Retention purge da aynı şekilde tehlikeli.
- **Çözüm:** `flush_staging_to_delta_log()`, `run_periodic_checkpoints()` ve `run_retention_purge()` çağrılarından önce `is_restore_in_progress()` kontrolü.
- **Dosya:** `src/storage/worker.rs` — worker main loop
- **Test:** Restore sırasında worker'ın checkpoint almaması test edilmeli.

### 1.3 — TOAST / Large Row Koruması ✅
- **Sorun:** Trigger `to_jsonb(NEW)` çağırıyor. 1GB TEXT sütunu olan bir satır güncellendiğinde tüm satır uncompress edilip memory'ye alınır → OOM, sunucu ölür.
- **Çözüm:** Trigger'da `pg_column_size(NEW)` kontrolü. GUC ile ayarlanabilir sınır (default 8KB). Aşarsa WARNING logla, capture'ı atla.
- **Dosya:** `src/api.rs` — flashback_capture_row_trigger fonksiyonu
- **Test:** 10KB+ row INSERT/UPDATE → WARNING logu, capture atlanmalı, DML başarılı olmalı.

### 1.4 — Restore Audit Log ✅
- **Sorun:** Enterprise ortamda her restore'un kaydı olmalı. Kim, ne zaman, hangi tabloyu, hangi zamana döndürdü?
- **Çözüm:** `flashback.restore_log` tablosu. Her restore başında ve sonunda kayıt.
- **Dosya:** `src/storage/schema.rs` (tablo), `src/restore/planner.rs` (INSERT)
- **Test:** Restore sonrası restore_log'da kayıt var mı kontrolü.

---

## Faz 2: Güvenilirlik ✅ TAMAMLANDI

### 2.1 — Write Overhead Benchmark ✅
- 4 senaryo benchmark scripti: `scripts/run_benchmark.sh`
- Sonuçlar (100K bulk INSERT: ~5.8x, 10K single-row: ~2.8x, 15K mixed: ~1.8x, 5K wide: ~10.4x)
- Trigger-based overhead beklenen seviyede; batch INSERT'lerde yüksek, typical OLTP'de makul

### 2.2 — Global Enable/Disable GUC ✅
- `SET pg_flashback.enabled = off;` → trigger capture durur, worker idle kalır
- Emergency kill switch, superuser-only (Suset context)
- **Dosya:** `src/storage/worker.rs` — `ENABLED_GUC` + `is_capture_enabled()`

### 2.3 — pg_stat_flashback Monitoring View ✅
- `flashback.pg_stat_flashback` view
- tracked_tables, pending_events, total_deltas, delta_storage, staging_storage, total_snapshots, total_restores, successful/failed_restores, last_restore_at, capture_enabled, max_row_size, worker_interval_ms
- **Dosya:** `src/storage/schema.rs`

### 2.4 — Retention Status Fonksiyonu ✅
- `flashback_retention_status()` — tablo bazında delta sayısı ve retention_warning flag
- WARNING flag retention ihlali durumunda true döner
- **Dosya:** `src/api.rs`

### 2.5 — Worker Graceful Degradation ✅
- Worker loop'ta `pg_flashback.enabled` kontrolü — disabled iken flush/checkpoint/retention atlanır
- Kill switch aktifken worker idle kalır ama çalışmaya devam eder
- **Dosya:** `src/storage/worker.rs` — worker main loop

---

## Faz 3: Enterprise (Sonraki 4-6 hafta)

### 3.1 — Multi-Database Desteği
### 3.2 — pg_dump/pg_restore Uyumu
### 3.3 — Streaming Replication Testi
### 3.4 — Extension Upgrade Path (ALTER EXTENSION UPDATE)
### 3.5 — PGXN Yayını
### 3.6 — CI Pipeline (GitHub Actions)

---

## Mevcut Skorlar

| Kriter | Skor | Faz 1 sonrası | Faz 2 sonrası |
|--------|------|---------------|---------------|
| Veri güvenliği | 9/10 | ✅ advisory lock + TOAST guard | 9/10 |
| Sunucu stabilitesi | 9/10 | ✅ worker restore guard | 9/10 + kill switch |
| Monitoring | 4/10 | restore_log eklendi | ✅ 8/10 pg_stat_flashback + retention_status |
| Dokümantasyon | 2/10 | | 5/10 ROADMAP + benchmark |
| Test coverage | 8/10 | 14/14 test | 8/10 14/14 test |
| **Toplam** | **9/10** | **Faz 2'ye hazır** | **Faz 3'e hazır** |
