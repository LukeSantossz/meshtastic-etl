-- Pergunta: o nó solar está carregando?
--
-- Tensão de bateria (metric = 'voltage') é a leitura que mostra se o painel
-- solar + bateria de cada nó estão dando conta da carga ao longo do dia.
-- Quedas sistemáticas na média diária, ou mínimas muito próximas do limite
-- de 2.5V (ver RANGES em src/lucas_pipeline/validate.py), indicam nó que não
-- está recarregando o suficiente durante o dia.
--
-- Fonte: lucas_serie (dado bruto por leitura). O agrupamento é feito aqui
-- em vez de usar lucas_agregado_dia só para deixar a consulta explícita;
-- a tabela agregada abaixo tem o mesmo resultado, só que já pré-calculado
-- pelo pipeline a cada leitura (ver seção "Modelo de dados" no README).
--
-- Como rodar (exemplo com mysql client, ajuste host/usuário/banco conforme seu .env):
--   mysql -h <MARIADB_HOST> -P <MARIADB_PORT> -u <MARIADB_USER> -p <MARIADB_DATABASE> < queries/01_tensao_bateria_por_dia.sql
--
-- Para restringir a um período, descomente e ajuste a cláusula de datas abaixo.

SELECT
    node_id,
    DATE(received_at) AS dia,
    AVG(value) AS tensao_media,
    MIN(value) AS tensao_minima,
    MAX(value) AS tensao_maxima,
    COUNT(*) AS leituras
FROM lucas_serie
WHERE metric = 'voltage'
    -- AND received_at >= '2026-01-01 00:00:00'
    -- AND received_at <  '2026-01-08 00:00:00'
GROUP BY node_id, DATE(received_at)
ORDER BY node_id, dia;

-- Alternativa: mesmo resultado, lendo direto da tabela já agregada por dia
-- (mantida pelo pipeline via upsert_agregado_dia em cada leitura aceita):
--
-- SELECT
--     node_id,
--     bucket_dia AS dia,
--     avg_value AS tensao_media,
--     min_value AS tensao_minima,
--     max_value AS tensao_maxima,
--     count_value AS leituras
-- FROM lucas_agregado_dia
-- WHERE metric = 'voltage'
-- ORDER BY node_id, bucket_dia;
