# Pipeline IoT — Projeto Integrador (Lucas)

**Autor:** Lucas Gonçalves
**Disciplina:** Projeto Integrador — Big Data (Fatec)
**Professor:** [github.com/siriani](https://github.com/siriani)

## 1. Objetivo

Este projeto implementa um pipeline de ingestão e tratamento de dados de
sensores IoT (rede de nós Meshtastic/DHT11) que chegam brutos no MongoDB,
trata quatro problemas reais de qualidade de dado (duplicata, relógio de
nó não confiável, buraco de leitura e payload fora de esquema) e grava o
resultado tratado no MariaDB em formato de série temporal e agregados,
além de manter o último valor de cada métrica no Redis para consulta
rápida. O objetivo é ter uma base confiável para responder perguntas
operacionais simples sobre a malha de sensores: bateria carregando,
saturação de canal e nós caindo.

## 2. Arquitetura

```
MongoDB (pacotes_brutos / sensores)
        |
        v
  [decode.py]   extrai envelope (node_id, packet_id, received_at) e
        |       decodifica métricas do payload (env, telemetry, position,
        |       texto DHT11 disfarçado)
        v
  [pipeline.py] orquestra, em ordem, os 4 tratamentos:
        |         1) dedupe (node_id + packet_id)
        |         2) validação de relógio do nó (clock.py)
        |         3) detecção de buraco de silêncio (gaps.py)
        |         4) validação de faixa física (validate.py)
        |
        +--> OK  --> MariaDB
        |             lucas_serie          (uma linha por leitura)
        |             lucas_agregado_hora  (upsert incremental)
        |             lucas_agregado_dia   (upsert incremental)
        |             lucas_buracos        (um registro por silêncio)
        |
        +--> OK  --> Redis
        |             lucas:last:<node_id>:<metric>  (último valor, JSON)
        |             lucas:checkpoint               (último _id do Mongo lido)
        |             lucas:counters:processados             (contador)
        |             lucas:counters:duplicatas_descartadas  (contador)
        |             lucas:counters:timestamps_invalidos    (contador)
        |             lucas:counters:buracos_detectados      (contador)
        |             lucas:counters:quarentena              (hash, campo = motivo)
        |             lucas:gap:last                  (hash, campo = node_id,
        |                                               valor = último received_at ISO-8601)
        |
        +--> falhou --> MongoDB (lucas_quarentena, com _motivo_quarentena)
```

O loop principal (`__main__.py` / `Pipeline.run_forever`) faz polling no
Mongo, processa em lotes (`BATCH_SIZE`) e dorme `POLL_INTERVAL_SECONDS`
entre ciclos.

## 3. Os quatro problemas e como o código trata

1. **Duplicatas por nó** — o mesmo pacote pode chegar mais de uma vez
   (reenvio de rádio, reprocessamento). `Pipeline.process_document`
   descarta pela chave `(node_id, packet_id)` em memória durante o lote, e
   a tabela `lucas_serie` tem `UNIQUE KEY (node_id, packet_id, metric)`
   com `INSERT IGNORE` como segunda barreira — mesmo se o processo for
   reiniciado e reprocessar algo, não duplica linha.

2. **Relógio do nó não é confiável** — `node_timestamp` vem do próprio
   nó (pode estar com GPS não sincronizado, zerado ou no futuro).
   `clock.validate_node_timestamp` só classifica o motivo de problema
   (`timestamp_zero_ou_negativo`, `timestamp_epoca_1970`,
   `timestamp_futuro`, `timestamp_muito_antigo`) para contagem/log — a
   série **sempre** usa `received_at` (hora de chegada no gateway/Mongo)
   como relógio oficial, nunca o `node_timestamp`.

3. **Buracos não são interpolados** — quando um nó fica em silêncio por
   mais que `GAP_THRESHOLD_SECONDS`, `gaps.GapTracker` registra o
   intervalo `(silencio_inicio, silencio_fim, duracao_segundos)` em
   `lucas_buracos`. Não se inventa valor para o período sem dado; o
   buraco fica explícito para quem for analisar.

4. **Esquema inesperado vai para quarentena com motivo** —
   `decode.extract_envelope` levanta `DecodeError` para payload fora do
   formato esperado (`payload_data_ausente`, `sender_ausente`,
   `received_at_invalido`, `received_at_ausente`), e
   `validate.validate_reading` rejeita valor fora de faixa física ou não
   finito. Em qualquer um dos casos o documento original vai para a
   coleção de quarentena no Mongo com o campo `_motivo_quarentena`, sem
   parar o pipeline.

## 4. Modelo de dados

- **Métrica é linha, não coluna** (`lucas_serie`, formato longo: uma
  linha por `node_id + metric + received_at`). Nem todo pacote traz as
  mesmas métricas (telemetry traz bateria/tensão/canal, env traz
  temperatura/umidade, position traz lat/lon), então uma tabela larga
  teria colunas majoritariamente nulas e precisaria de migração de
  esquema cada vez que um novo tipo de sensor aparecesse. Em formato
  longo, uma métrica nova é só um novo valor de `metric`, sem alterar
  tabela.

- **Agregado por hora, não por 15 minutos** (`lucas_agregado_hora`). Os
  nós não têm um intervalo de envio fixo e confiável (dependem de rádio
  mesh, podem atrasar ou enviar em lote), então uma janela de 15 minutos
  frequentemente ficaria vazia ou com só uma leitura — não ganha
  granularidade de sinal, só multiplica linhas e upserts. Hora é
  granularidade suficiente para as perguntas operacionais deste PI
  (carregando a bateria? saturando a malha?) e ainda permite calcular o
  agregado diário a partir dela sem precisar reagregar o bruto.

- **Redis para o último valor, não `SELECT` no MariaDB**
  (`lucas:last:<node_id>:<metric>`). Esse dado é consultado toda vez que
  alguém quer saber o estado atual de um nó (ex.: um painel/dashboard
  olhando "tensão agora" de cada nó), e é sobrescrito a cada leitura
  nova — é natureza de cache de estado, não de histórico. Fazer isso
  com `SELECT ... ORDER BY received_at DESC LIMIT 1` no MariaDB
  funcionaria, mas exigiria scan ou índice dedicado por
  `(node_id, metric)` para cada consulta; no Redis é `GET` direto pela
  chave, O(1), e o pipeline já escreve lá em todo `insert_serie_batch`
  aceito.

## 5. Garantia de restart

O pipeline precisa poder cair e voltar sem perder nem duplicar dado:

- **Checkpoint no Redis** (`lucas:checkpoint`, `RedisCheckpoint` em
  `checkpoint.py`): guarda o `_id` do último documento do Mongo
  processado. `Pipeline.run_once` usa esse valor para filtrar
  `{"_id": {"$gt": ObjectId(last_id)}}` na próxima leitura — ao reiniciar,
  o pipeline retoma exatamente de onde parou, sem reler o Mongo inteiro.

- **`INSERT IGNORE` na unique key** (`lucas_serie.uq_dedupe` em
  `(node_id, packet_id, metric)`): mesmo que o checkpoint falhe e o
  mesmo lote seja reprocessado, a constraint de unicidade garante que a
  linha não entra duas vezes — o `INSERT IGNORE` simplesmente ignora a
  tentativa de duplicata em vez de dar erro e travar o lote.

Os dois mecanismos juntos cobrem o caso comum (checkpoint evita
reprocessar) e o caso de falha (constraint evita duplicar se
reprocessar mesmo assim).

- **Contadores persistidos no Redis** (`Counters.load_from_redis` /
  `Counters.sync_to_redis`, em `counters.py`): ao subir, o pipeline
  carrega os contadores das chaves `lucas:counters:*` em vez de começar
  do zero; `Pipeline.process_batch` escreve os valores atualizados de
  volta no Redis depois de cada lote. Um reinício não perde a contagem
  acumulada.

- **Última leitura por nó persistida no Redis** (`GapTracker`, em
  `gaps.py`): ao instanciar, o `GapTracker` restaura o último
  `received_at` de cada nó a partir do hash `lucas:gap:last` e grava de
  volta nessa mesma chave a cada chamada de `check()`. Assim, um
  silêncio que atravessa um restart do processo ainda é detectado como
  buraco — o `GapTracker` não "esquece" o último dado visto só porque o
  processo caiu.

## 6. Contadores

`Counters` (`src/lucas_pipeline/counters.py`) acompanha:

- `processados` — leituras aceitas e gravadas na série
- `duplicatas_descartadas` — pacotes repetidos (mesmo `node_id` + `packet_id`)
- `timestamps_invalidos` — `node_timestamp` de nó reprovado em `clock.py`
- `buracos_detectados` — intervalos de silêncio registrados em `lucas_buracos`
- `quarentena` — dicionário por motivo (`payload_data_ausente`, `sender_ausente`, `<metrica>_fora_de_faixa`, etc.)

Os contadores vivem em memória durante a execução, mas não são só "por
execução": a cada lote processado, `Pipeline.process_batch` sincroniza
os valores atuais para o Redis (`Counters.sync_to_redis`, chaves
`lucas:counters:*` descritas na seção 2); e ao subir, o pipeline chama
`Counters.load_from_redis` para retomar a contagem acumulada em vez de
zerar. Reiniciar o processo não reinicia os contadores.

Os valores reais desses contadores ainda não existem — vêm da corrida
contínua de 72h no ambiente da disciplina (ver seção 10).

## 7. Como rodar local

```bash
# 1. ambiente Python
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. variáveis de ambiente
cp .env.example .env
# ajuste MONGO_URI, MARIADB_*, REDIS_* conforme seu ambiente

# 3. infraestrutura local (Mongo + MariaDB + Redis)
docker-compose up -d mongo mariadb redis

# 4. criar o esquema no MariaDB na primeira vez
echo "ENSURE_SCHEMA=1" >> .env
# (ou aplique manualmente: mysql ... < sql/schema.sql)

# 5. testes unitários
pytest

# 6. rodar o pipeline
python -m lucas_pipeline
```

`ENSURE_SCHEMA=1` faz o pipeline aplicar `sql/schema.sql` (tabelas
`lucas_*`, todas `CREATE TABLE IF NOT EXISTS`) antes de começar o
polling — seguro de rodar repetido.

## 8. As três consultas

- [`queries/01_tensao_bateria_por_dia.sql`](queries/01_tensao_bateria_por_dia.sql) — o nó solar está carregando? Tensão média/mín/máx de bateria por nó e por dia.
- [`queries/02_utilizacao_canal_por_hora.sql`](queries/02_utilizacao_canal_por_hora.sql) — a malha está saturando? Utilização de canal média por nó e por hora.
- [`queries/03_silencio_por_no.sql`](queries/03_silencio_por_no.sql) — quem está caindo? Tempo total e quantidade de silêncios por nó, do maior para o menor.

## 9. Estrutura de pastas

```
src/lucas_pipeline/   código do pipeline (decode, validate, clock, gaps, checkpoint, store, counters, pipeline)
tests/                25 testes unitários + fixtures sintéticas
sql/schema.sql        esquema das tabelas lucas_* no MariaDB
queries/               as três consultas SQL deste PI
scripts/seed_synthetic.py  carga de dados sintéticos para teste local
docker-compose.yml    Mongo + MariaDB + Redis + pipeline para ambiente local
```

## 10. Resultados da corrida de 72h — PENDENTE

Sem credenciais de produção ainda. Contadores e resultados reais das
consultas serão preenchidos após 72h contínuas no ambiente da
disciplina. **Não inventar números.** Os dados em `tests/fixtures/` são
**sintéticos**, só para teste unitário (ver
`tests/fixtures/README.md`) — não podem ser citados como resultado real
da coleta.

## 11. Licença / uso acadêmico

Projeto desenvolvido para fins acadêmicos na disciplina de Projeto
Integrador — Big Data (Fatec). Uso e reprodução livres para fins de
estudo, com crédito ao autor.
