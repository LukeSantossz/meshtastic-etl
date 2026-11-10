-- Pergunta: quem está caindo?
--
-- lucas_buracos guarda um registro por intervalo de silêncio detectado
-- em cada nó (silêncio = tempo entre duas leituras do mesmo nó maior que
-- GAP_THRESHOLD_SECONDS, ver src/lucas_pipeline/gaps.py — sem interpolação,
-- só o registro do intervalo). Somar a duração desses intervalos por nó
-- mostra quais nós ficam mais tempo fora do ar, e quantas vezes isso
-- aconteceu, o que ajuda a priorizar manutenção de campo.
--
-- Como rodar (exemplo com mysql client, ajuste host/usuário/banco conforme seu .env):
--   mysql -h <MARIADB_HOST> -P <MARIADB_PORT> -u <MARIADB_USER> -p <MARIADB_DATABASE> < queries/03_silencio_por_no.sql

SELECT
    node_id,
    COUNT(*) AS qtd_buracos,
    SUM(duracao_segundos) AS silencio_total_segundos,
    ROUND(SUM(duracao_segundos) / 3600, 2) AS silencio_total_horas,
    MAX(duracao_segundos) AS maior_buraco_segundos,
    MAX(silencio_fim) AS ultimo_silencio_registrado
FROM lucas_buracos
GROUP BY node_id
ORDER BY silencio_total_segundos DESC;
