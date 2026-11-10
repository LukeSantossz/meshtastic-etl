-- Pergunta: a malha está saturando?
--
-- channel_utilization (metric = 'channel_utilization') mede o quanto do
-- canal de rádio da malha Meshtastic cada nó percebe ocupado. Média por
-- hora e por nó ajuda a ver se a ocupação sobe em horários específicos
-- (ex.: pico de tráfego) e se algum nó em particular está enxergando
-- utilização muito mais alta que os vizinhos, o que pode indicar
-- interferência local ou problema de posicionamento.
--
-- Fonte: lucas_serie (dado bruto por leitura), truncando received_at na
-- hora. O pipeline também mantém lucas_agregado_hora com o mesmo cálculo
-- já pronto (ver _update_aggregates em src/lucas_pipeline/pipeline.py);
-- a consulta alternativa no final usa essa tabela.
--
-- Como rodar (exemplo com mysql client, ajuste host/usuário/banco conforme seu .env):
--   mysql -h <MARIADB_HOST> -P <MARIADB_PORT> -u <MARIADB_USER> -p <MARIADB_DATABASE> < queries/02_utilizacao_canal_por_hora.sql

SELECT
    node_id,
    DATE_FORMAT(received_at, '%Y-%m-%d %H:00:00') AS hora,
    AVG(value) AS utilizacao_media,
    MIN(value) AS utilizacao_minima,
    MAX(value) AS utilizacao_maxima,
    COUNT(*) AS leituras
FROM lucas_serie
WHERE metric = 'channel_utilization'
    -- AND received_at >= '2026-01-01 00:00:00'
    -- AND received_at <  '2026-01-08 00:00:00'
GROUP BY node_id, hora
ORDER BY node_id, hora;

-- Alternativa: mesmo resultado, lendo direto da tabela já agregada por hora
-- (mantida pelo pipeline via upsert_agregado_hora em cada leitura aceita):
--
-- SELECT
--     node_id,
--     bucket_hora AS hora,
--     avg_value AS utilizacao_media,
--     min_value AS utilizacao_minima,
--     max_value AS utilizacao_maxima,
--     count_value AS leituras
-- FROM lucas_agregado_hora
-- WHERE metric = 'channel_utilization'
-- ORDER BY node_id, bucket_hora;
