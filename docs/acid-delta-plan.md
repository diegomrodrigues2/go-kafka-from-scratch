# Plano de Evolução ACID inspirado no Delta Lake

Documento de referência para introduzir um `_delta_log` transacional no projeto **go-kafka-from-scratch**, garantindo propriedades ACID semelhantes às do Delta Lake.

## 1. Objetivos e requisitos
- **Atomicidade**: cada lote publicado só se torna visível após um commit JSON atômico.
- **Consistência**: offsets crescentes sem lacunas; somente arquivos presentes no `_delta_log` definem o estado válido.
- **Isolamento (OCC/MVCC)**: produtores concorrentes detectam conflitos via número de versão esperado.
- **Durabilidade**: dados e metadados são persistidos; reinicializações usam checkpoints + replay para reconstruir o estado.
- **Compatibilidade**: preservar APIs atuais (`internal/api`, `pkg/client`) e fluxos de replicação enquanto adicionamos a camada transacional.

## 2. Layout e formatos do `_delta_log`

### 2.1 Estrutura de diretórios por partição
```
{DATA_DIR}/topic={topic}-part={partition}/
  ├── 00000000000000000000.log
  ├── 00000000000000000000.index
  ├── ...
  └── _delta_log/
       ├── 00000000000000000000.json          # commit versão 0
       ├── ...
       └── 00000000000000000010.checkpoint.json
```

### 2.2 Schema de commit (`CommitLogEntry`)
```jsonc
{
  "version": 11,
  "timestamp": "2025-11-07T18:15:02.883Z",
  "txn_id": "broker-1:1730993702883:42",
  "operations": [
    {
      "type": "add",
      "origin_id": 1,
      "origin_seq": 78,
      "offset_start": 420,
      "offset_end": 427,
      "segment": "00000000000000000400.log",
      "position": 8192,
      "size": 384
    }
  ],
  "checkpoint_hint": false
}
```
Campos obrigatórios:
- `version`: número sequencial global por partição.
- `operations`: lista imutável de adições (remoções futuras podem ser suportadas).
- `offset_start/offset_end`: intervalos contínuos escritos no segmento.
- `segment/position/size`: metadados suficientes para auditoria e recuperação.

### 2.3 Schema de checkpoint (`Checkpoint`)
```jsonc
{
  "version": 20,
  "timestamp": "...",
  "last_offset": 840,
  "segments": [
    {"name": "00000000000000000800.log", "base": 800, "next_offset": 860, "size": 2048}
  ],
  "origins": [
    {"origin_id": 1, "last_seq": 120},
    {"origin_id": 2, "last_seq": 44}
  ]
}
```
- checkpoints cobrem **todas** as estruturas necessárias para reconstruir `partitionLog` sem ler commits anteriores.
- cadência inicial: a cada 10 commits confirmar um checkpoint (`version % 10 == 0`).

## 3. Novo módulo `internal/deltalog`

### 3.1 Responsabilidades
1. Escrita de commits numerados de forma otimista.
2. Criação de checkpoints (sincronamente após o commit gatilho, depois assíncrono).
3. Replay durante bootstrap: carregar último checkpoint, aplicar commits pendentes, informar `logstore`.
4. API para consulta de versão e locking otimista.

### 3.2 Interface proposta
```go
type Store interface {
    CurrentVersion(ctx context.Context) (uint64, error)
    BeginTransaction(ctx context.Context) (*Txn, error)
    Commit(ctx context.Context, tx *Txn, ops []Operation, expectedVersion uint64) (uint64, error)
    Abort(ctx context.Context, tx *Txn) error
    LoadState(ctx context.Context) (*Snapshot, error)
    MaybeCheckpoint(ctx context.Context, version uint64, snapshot *Snapshot) error
}
```
Suporte a:
- `Operation` (add/remove) com metadados de offsets.
- `Snapshot` contendo `Segments`, `OriginSeqs`, `NextOffset`.
- Execução atômica baseada em escrita de arquivo com nome fixo (`%020d.json`) usando `os.O_CREATE|os.O_EXCL`.

### 3.3 Persistência
- arquivos JSON formatados com `json.Encoder` (indent opcional somente para debug).
- `fsync` após escrita (Go: `file.Sync()`).
- checkpoints com compressão opcional (postergar).

## 4. Alterações em módulos existentes

### 4.1 `internal/logstore`
1. **Carregamento** (`Open`): utilizar `_delta_log` como fonte de verdade. Passos:
   - localizar diretório `_delta_log`; se não existir, criar commit inicial com `version=0`.
   - carregar último checkpoint → aplicar commits restantes → montar lista de segmentos em memória.
   - truncar bytes não confirmados em `.log`/`.index` comparando com o snapshot.
2. **Escrita em duas fases**:
   - `Log.AppendTransactional(ctx, batch []Record) (*TxnHandle, error)` grava bytes em segmentos e retorna descritores (`segment`, `position`, `len`).
   - `TxnHandle.Commit(...)` chama `deltalog.Store.Commit`.
   - Abort reverte ponteiros de arquivo (via `segment.Truncate(position)`).
3. **API pública**: manter `Append` existente para fluxos não transacionais no curto prazo; internamente passa a usar Begin/Commit para lotes pequenos.

### 4.2 `internal/broker`
- `partitionLog` passa a manter `currentVersion` e usar `deltalog` para OCC.
- `Publish`/`replication`:
  1. `BeginTransaction` → `appendLocal` (buffer) → `Commit` com `expectedVersion`.
  2. Em conflito, retornar erro 409 ao HTTP e permitir retry (com reappend, pois offsets mudam).
- Replicação líder/follower:
  - followers aplicam commits recebidos através do `_delta_log`, não mais direto no `logstore`.
  - Hinted handoff passa a enfileirar operações transacionais (payload + versão).

### 4.3 `internal/api`
- Ajustar handlers para lidar com novos erros (409 Conflict, 503 se checkpoint em progresso demorado).
- Possível endpoint de batch transacional (`POST .../batch`) enviando array de mensagens.

## 5. Fluxos operacionais

### 5.1 Escrita transacional
1. Broker recebe lote → `BeginTransaction`.
2. Escreve bytes no segmento ativo (com flush local).
3. Constrói `operations` usando offsets/segment positions.
4. Invoca `deltalog.Commit` com `expectedVersion`.
5. Em caso de sucesso, responde offsets finais; em falha, `Abort` remove bytes (truncate).

### 5.2 Leitura consistente
- `partitionLog.readRecords` somente expõe offsets ≤ `snapshot.LastCommittedOffset`.
- Replicas lendo diretamente dos segmentos usam mesmo snapshot, garantindo leitores monotônicos.

### 5.3 Recuperação
1. Ao iniciar, `deltalog.LoadState`:
   - lê checkpoint mais recente.
   - aplica commits restantes em ordem (ordenar por nome de arquivo).
2. Reconstrói `segments`, `originSeq` e `nextOffset`.
3. Trunca arquivos `.log/.index` usando `fs.Truncate` caso tenham bytes além do último offset confirmado.

## 6. Controle de concorrência e isolamento
- OCC baseado na próxima versão esperada: `expectedVersion = currentVersion`.
- Operação de commit só cria arquivo `%020d.json` se ainda não existir; caso exista, outro produtor venceu (conflito).
- `TxnID` usado para idempotência de retries (broker pode reaplicar transação se confirmar que não foi persistida).
- Leitores sempre examinam `currentVersion` e só usam snapshots imutáveis → efeito MVCC simples.

## 7. Estratégia de testes
1. **Unitários `internal/deltalog`**: criação de commit, detecção de conflito, checkpoint + reload.
2. **Logstore**:
   - teste de recovery truncando bytes extras e verificando offsets.
   - commit abort → bytes não visíveis.
3. **Broker**:
   - concorrência: simular dois produtores com OCC.
   - replicação: líder falha após escrever dados mas antes de commit → follower não vê dados inválidos.
4. **Integração API**:
   - POST simultâneos validam retornos 409 e sucesso após retry.
   - GET não lê offsets não confirmados.
5. **Chaos / durability** (tests/ e2e):
   - matar processo entre escrita de dados e commit → recovery garante truncamento.
   - remover checkpoint mais recente → replay de commits reconstroi estado.

## 8. Roteiro incremental sugerido
1. **Fase A** – Introduzir `internal/deltalog` (commit + leitura + testes unitários) sem integrar ao `logstore`.
2. **Fase B** – Adaptar `logstore.Log` para usar delta log em modos opt-in (flag em `broker.Config`), mantendo caminho legado para comparar.
3. **Fase C** – Integrar Broker/replicação à API transacional, acrescentando novos códigos de erro e métricas.
4. **Fase D** – Implementar checkpoints, recuperação completa e testes de caos.
5. **Fase E** – Expandir API (batch, leitura por versão) e otimizações (compressão, limpeza de logs antigos).

Este plano serve como fundação para iniciar a implementação incremental mantendo o sistema operável enquanto as garantias ACID são introduzidas.

