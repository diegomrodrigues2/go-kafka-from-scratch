# Go Kafka From Scratch – Delta Log Edition

Um broker de logs distribuídos escrito em Go que combina os conceitos centrais do Kafka (tópicos, partições, segmentos e replicação) com a camada ACID inspirada no Delta Lake. Cada partição mantém um `_delta_log` local com commits JSON atômicos e checkpoints periódicos, permitindo recuperação determinística e detecção de conflitos via controle de concorrência otimista.

> Se preferir um panorama passo a passo do desenho, consulte `docs/acid-delta-plan.md`.

## Visão Geral da Arquitetura

- **API HTTP (`internal/api/`)** – expõe endpoints REST para publicar, buscar, consultar métricas e acessar o delta log.
- **Broker (`internal/broker/`)** – coordena tópicos/partições, aplica transações ACID, gerencia replicação e mantém origem/seq por produtor.
- **LogStore (`internal/logstore/`)** – provê a abstração de segmentos (`.log` + `.index`) e agora executa todas as escritas via transações que só se tornam visíveis após o commit no `_delta_log`.
- **DeltaLog (`internal/deltalog/`)** – implementa o protocolo de metadados estilo Delta Lake: arquivos `%020d.json` para commits, `%020d.checkpoint.json` para snapshots e replay determinístico.
- **Clientes (`pkg/client/`)** – exemplos de produtor/consumidor com semântica read-your-writes usando as mesmas rotas HTTP.

```
{DATA_DIR}/topic={topic}-part={p}/
  ├── 00000000000000000000.log
  ├── 00000000000000000000.index
  ├── ...
  └── _delta_log/
        ├── 00000000000000000000.json
        ├── ...
        └── 00000000000000000010.checkpoint.json
```

## Como o ACID funciona

1. **Begin** – o broker abre uma transação (`logstore.Txn`) e armazena os payloads pendentes em memória.
2. **Write-Ahead** – os bytes são gravados no segmento ativo, mas ainda não são visíveis; mantemos o snapshot do ponteiro para rollback.
3. **Commit JSON** – o `_delta_log` recebe um arquivo `%020d.json` com as operações (`type=add`, offsets, segmento, posição, originID/seq). O arquivo é criado via `O_CREATE|O_EXCL` para garantir OCC.
4. **Checkpoint** – a cada 10 commits (valor padrão), escrevemos `%020d.checkpoint.json` com o snapshot completo (nextOffset, segmentos, mapa de originSeqs).
5. **Recovery** – na inicialização carregamos o último checkpoint, aplicamos os commits seguintes e truncamos qualquer byte órfão dos segmentos antes de aceitar novas requisições.
6. **Conflitos** – se dois produtores disputam a mesma versão, o commit mais lento recebe `ErrTxnConflict` e o HTTP devolve `409`. Basta reenviar; os offsets serão recalculados.

## Endpoints Principais

| Método | Caminho | Descrição |
|--------|---------|-----------|
| `POST` | `/topics/{topic}/partitions/{p}` | Publica um registro e retorna o offset confirmado. |
| `POST` | `/topics/{topic}/partitions/{p}/batch` | Publica um lote **atômico** (corpo JSON com `records` base64) e retorna todos os offsets. |
| `GET`  | `/topics/{topic}/partitions/{p}/fetch?offset=X&maxBytes=Y` | Busca registros sequenciais; suporta `raw=1` e `minOffset`. |
| `GET`  | `/topics/{topic}/partitions/{p}/delta?fromVersion=A&toVersion=B` | Lê checkpoint + commits do `_delta_log` para auditoria ou debugging. |
| `GET`  | `/topics/{topic}/partitions/{p}/metrics` | Mostra `nextOffset` e a versão atual do delta log. |
| `GET`  | `/cluster/topics/{topic}/partitions/{p}` | Metadados de replicação (leader, epoch, réplicas). |
| `POST` | `/cluster/topics/{topic}/partitions/{p}/leader` | Atualiza liderança (failover/manual). |
| `POST` | `/cluster/topics/{topic}/partitions/{p}/replica` | Endpoint usado pelo replicador/fan-out para replays externos. |

## Execução

```bash
go run ./cmd/broker \
  -data-dir ./data \
  -topic demo
```

Variáveis de ambiente relevantes:

| Variável | Padrão | Uso |
|----------|--------|-----|
| `DATA_DIR` | `./data` | Diretório raiz das partições locais. |
| `TOPIC` | `demo` | Nome padrão do tópico criado no startup. |
| `ADDR` | `:8080` | Porta de escuta da API. |
| `ENABLE_TRANSACTIONS` | `1` (padrão habilitado) | Define se o broker usa `_delta_log`. Use `0` apenas para debugging legado. |

Exemplo de publicação:

```bash
curl -X POST http://localhost:8080/topics/demo/partitions/0 \
     --data-binary 'hello delta log'
```

Publicação em lote (payloads precisam estar em base64):

```bash
curl -X POST http://localhost:8080/topics/demo/partitions/0/batch \
     -H 'Content-Type: application/json' \
     -d '{"records":["aGVsbG8=", "ZGVsdGE="]}'
```

Consulta ao delta log:

```bash
curl http://localhost:8080/topics/demo/partitions/0/delta?fromVersion=0
```

## Recuperação e Replicação

- **Recovery local** – se o processo cair entre escrever os bytes e confirmar o commit, o replay remove qualquer sobra do `.log` antes de aceitar novos clientes.
- **Replicação** – permanecem disponíveis os modos single-leader, multi-líder e leaderless (hinted handoff/quorum). Réplicas seguem lendo apenas commits confirmados, portanto partilham o mesmo histórico ACID.
- **Leituras consistentes** – clientes podem usar `minOffset` ou o header `X-Read-Min-Offset` para exigir que o broker sirva dados somente após certo offset commitado; se o nó não puder, devolve `307` com a URL do líder.

## Estrutura do Repositório

```
cmd/
  broker/              # binário principal
  examples/replication # cluster de exemplo com 3 nós
internal/
  api/                 # camada HTTP
  broker/              # coordenação de partições e replicação
  deltalog/            # implementação Delta Lake-style
  logstore/            # segmentos (dados + índices) com API transacional
  types/               # tipos auxiliares
pkg/
  client/              # clientes de alto nível
docs/
  acid-delta-plan.md   # plano detalhado da evolução ACID
data/                  # diretório padrão criado em tempo de execução
```

## Desenvolvimento e Testes

```
go test ./...
```

Os testes cobrem:
- Detecção de conflito e replay do `_delta_log`.
- Checkpoints automáticos e recuperação com truncamento de bytes órfãos.
- Fluxos de replicação, hinted handoff e APIs HTTP (incluindo o modo batch/ACID).

Para inspecionar manualmente:
1. Publique mensagens.
2. Explore `data/topic=demo-part=0/_delta_log`.
3. Apague o processo e reinicie — os offsets continuam consistentes.

---

Com essa base, você pode experimentar políticas de replicação, ajustar o tamanho de segmento, alterar a cadência de checkpoints (`deltalog.WithCheckpointInterval`) ou expandir o protocolo para tombstones/compaction, mantendo sempre as garantias ACID via delta log padrão.
