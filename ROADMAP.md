# pg_flashback — Production Readiness Roadmap

> Son güncelleme: 2026-04-02
> Mevcut durum: 29/29 test geçiyor, Faz 1 + Faz 2 + Faz 3 tamamlandı

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

## Faz 3: Performans & Yeni Özellikler ✅ TAMAMLANDI

### 3.1 — delta_log Composite Indexes ✅
- `(rel_oid, event_time) WHERE committed_at IS NOT NULL` — restore sorgularını hızlandırır
- `(rel_oid, committed_at DESC)` — checkpoint/retention sorguları için
- **Dosya:** `src/storage/schema.rs`

### 3.2 — Bulk Snapshot Restore (INSERT...SELECT) ✅
- **Önceki:** jsonb_agg ile tüm snapshot'ı memory'ye yüklüyordu → OOM riski
- **Şimdi:** `INSERT INTO target SELECT * FROM snapshot_table` — zero-copy, bulk load
- **Dosya:** `src/restore/planner.rs`

### 3.3 — Per-Table Advisory Lock ✅
- **Önceki:** Global `pg_advisory_xact_lock(3589442679)` → tüm tablolar sıralı restore
- **Şimdi:** `pg_advisory_xact_lock(358944, rel_oid::integer)` → farklı tablolar paralel restore edilebilir
- **Dosya:** `src/restore/planner.rs`

### 3.4 — Trigger Guard Optimizasyonu ✅
- **Önceki:** Her trigger fire'da `pg_locks` SPI sorgusu → yoğun OLTP'de %10-20 overhead
- **Şimdi:** Sadece process-local AtomicBool kontrolü — SPI yok, zero-cost
- Diğer session'ların trigger'ları artık yanlışlıkla suppress edilmiyor (bug fix)
- **Dosya:** `src/runtime_guard.rs`

### 3.5 — Native JSONB (json→jsonb dönüşümü kaldırıldı) ✅
- staging_events tablosu artık JSONB kullanıyor (önceki: JSON)
- Trigger'lar `to_jsonb()` kullanıyor (önceki: `to_json()`)
- Worker'daki `::jsonb` cast kaldırıldı
- **Dosyalar:** `src/storage/schema.rs`, `src/api.rs`, `src/storage/worker.rs`

### 3.6 — Flashback Query (SELECT AS OF) ✅
- **Killer feature:** Tabloyu restore etmeden geçmişteki halini sorgula
- `flashback_query('public.orders', '2025-01-01 12:00'::timestamptz)` → SETOF record
- Custom sorgu: `flashback_query('public.orders', ts, 'SELECT * FROM $FB_TABLE WHERE total > 100')`
- Temp table ON COMMIT DROP — transaction bitince otomatik temizlenir
- **Dosya:** `src/restore/planner.rs`
- **Test:** `tests/sql/integration/flashback_query_basic.sql`

### 3.7 — Restore Progress Reporting ✅
- `RAISE NOTICE 'flashback_restore [table]: snapshot loaded from ...'`
- `RAISE NOTICE 'flashback_restore [table]: replaying N events ...'`
- Her 10,000 event'te progress: `RAISE NOTICE '... 10000/50000 (20.0%)'`
- Completion: `RAISE NOTICE '... complete — N events applied'`
- **Dosya:** `src/restore/planner.rs`

---

## Faz 4: Enterprise (Sonraki adımlar)

### 4.1 — Multi-Database Desteği ✅ (v0.4.0)
### 4.2 — pg_dump/pg_restore Uyumu
### 4.3 — Streaming Replication Testi
### 4.4 — Extension Upgrade Path (ALTER EXTENSION UPDATE) ✅ (v0.4.0)
### 4.5 — PGXN Yayını
### 4.6 — delta_log Time-Based Partitioning
### 4.7 — Parallel Restore (multi-threaded event replay) ✅ (v0.4.0, GUC hint)
### 4.8 — Native Partition Support ✅ (v0.4.0)
### 4.9 — CI/CD (GitHub Actions) ✅ (v0.4.0)

---

## Faz 5: Büyük Tablo Performansı

pg_flashback v0.4.0 itibariyle tasarım odağı **"surgical, small-to-medium table restore"**:
- **< 100K satır** → 1-5s restore, production-ready
- **> 500K satır** → pg_dump/restore ile rekabet edemez, bu scope dışı

Gerçek büyük tablo performansı için gerekli mimari değişiklikler:

### 5.1 — Binary Delta Format
- **Sorun:** JSONB per-row event → her satır için `jsonb_populate_record()` cast, ~10-50µs/row
- **Çözüm:** PostgreSQL internal binary format (`bytea`) veya CSV-style columnar delta
- **Beklenen kazanç:** 5-10x replay hızlanması
- **Karmaşıklık:** Yüksek (schema evolution ile uyum sağlanmalı)

### 5.2 — WAL-Based Capture (Logical Replication Replace)
- **Sorun:** Trigger-based capture → her DML'de trigger overhead (~1.5-10x write latency)
- **Çözüm:** `pg_logical_replication_slot` decode → trigger yok, WAL'dan direkt okuma
- **Beklenen kazanım:** Write overhead sıfır, replay için WAL segment re-read
- **Kısıt:** `wal_level = logical` gerektirir, superuser erişimi

### 5.3 — Incremental Table Snapshot
- **Sorun:** Checkpoint tamamen yeni snapshot → büyük tablolarda GB'larca kopyalama
- **Çözüm:** Sadece değişen page'leri kaydet (block-level diff, PostgreSQL visibility map kullanarak)
- **Beklenen kazanım:** Checkpoint 10x daha hızlı, depolama 5x daha az

### 5.4 — Partitioned Restore (Parallel per Partition)
- **Durum:** `flashback_restore_parallel` GUC hint veriyor ama gerçek paralellik yok
- **Çözüm:** Her partition için ayrı `dblink` veya `pg_background` worker → gerçek paralel replay
- **Beklenen kazanım:** N partition → ~N/2x hızlanma

---

## Mevcut Skorlar

| Kriter | Skor | Faz 1 sonrası | Faz 2 sonrası | Faz 3 sonrası |
|--------|------|---------------|---------------|---------------|
| Veri güvenliği | 9/10 | ✅ advisory lock + TOAST guard | 9/10 | 10/10 per-table lock + correct trigger guard |
| Sunucu stabilitesi | 9/10 | ✅ worker restore guard | 9/10 + kill switch | 10/10 zero-SPI triggers |
| Monitoring | 4/10 | restore_log eklendi | ✅ 8/10 pg_stat_flashback | 9/10 + progress reporting |
| Performans | 5/10 | | | ✅ 8/10 bulk restore + indexes + native jsonb |
| Özellik zenginliği | 6/10 | | | ✅ 9/10 flashback_query |
| Test coverage | 8/10 | 14/14 test | 8/10 14/14 test | ✅ 9/10 29/29 test |
| **Toplam** | **9/10** | **Faz 2'ye hazır** | **Faz 3'e hazır** | **Production-ready** |
