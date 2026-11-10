-- Schema MariaDB do pipeline do Lucas (prefixo lucas_).
-- Destino dos dados tratados a partir dos pacotes brutos do MongoDB.

CREATE TABLE IF NOT EXISTS lucas_serie (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    node_id VARCHAR(64) NOT NULL,
    packet_id VARCHAR(64) NOT NULL,
    received_at DATETIME(3) NOT NULL,
    metric VARCHAR(64) NOT NULL,
    value DOUBLE NOT NULL,
    unit VARCHAR(16) NULL,
    node_timestamp BIGINT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_dedupe (node_id, packet_id, metric),
    INDEX idx_node_time (node_id, received_at),
    INDEX idx_metric_time (metric, received_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS lucas_agregado_hora (
    node_id VARCHAR(64) NOT NULL,
    metric VARCHAR(64) NOT NULL,
    bucket_hora DATETIME NOT NULL,
    avg_value DOUBLE NOT NULL,
    min_value DOUBLE NOT NULL,
    max_value DOUBLE NOT NULL,
    count_value INT NOT NULL,
    PRIMARY KEY (node_id, metric, bucket_hora)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS lucas_agregado_dia (
    node_id VARCHAR(64) NOT NULL,
    metric VARCHAR(64) NOT NULL,
    bucket_dia DATE NOT NULL,
    avg_value DOUBLE NOT NULL,
    min_value DOUBLE NOT NULL,
    max_value DOUBLE NOT NULL,
    count_value INT NOT NULL,
    PRIMARY KEY (node_id, metric, bucket_dia)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS lucas_buracos (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    node_id VARCHAR(64) NOT NULL,
    silencio_inicio DATETIME(3) NOT NULL,
    silencio_fim DATETIME(3) NOT NULL,
    duracao_segundos INT NOT NULL,
    UNIQUE KEY uq_buraco (node_id, silencio_inicio)
) ENGINE=InnoDB;
