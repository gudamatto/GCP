-- ====================================================
--  Data generator for streaming data simulation (API Event: JSON) Bigquery - GCP
-- ================================================
DECLARE qtde_registros INT64 DEFAULT 100;

DECLARE project_id STRING DEFAULT 'Projeto_ID';

DECLARE dataset_event STRING DEFAULT 'Dataset';

DECLARE table_event STRING DEFAULT 'Table_name';

-- Colunas a serem criadas do JSON (aspas simples e vírgulas)
DECLARE nomes_colunas ARRAY<STRING> DEFAULT [
  'NOME',
  'CPF',
  'RG',
  'DATA_NASCIMENTO',
  'SEXO',
  'ESTADO_CIVIL',
  'NOME_MAE',
  'NOME_PAI',
  'EMAIL',
  'TELEFONE',
  'CELULAR',
  'ENDERECO',
  'NUMERO',
  'COMPLEMENTO',
  'BAIRRO',
  'CIDADE',
  'UF',
  'CEP',
  'PAIS',
  'NACIONALIDADE',
  'ESCOLARIDADE',
  'PROFISSAO',
  'EMPRESA',
  'RENDA_MENSAL',
  'SITUACAO_CADASTRAL',
  'DATA_CADASTRO'
];

-- =============================================
-- VARIÁVEIS DE CONSTRUÇÃO DINÂMICA
-- =============================================
DECLARE colunas_json STRING DEFAULT '';

DECLARE colunas_select STRING DEFAULT '';

DECLARE colunas_source STRING DEFAULT '';

DECLARE colunas_resultado STRING DEFAULT '';

DECLARE json_object_data STRING DEFAULT '';

DECLARE source_data_query STRING DEFAULT '';

DECLARE full_table_name STRING DEFAULT FORMAT('`%s.%s`', dataset_event, table_event);

-- =============================================
-- CONSTRUÇÃO DINÂMICA DE CAMPOS
-- =============================================
FOR coluna IN (
  SELECT
    nome
  FROM
    UNNEST (nomes_colunas) AS nome
) DO
SET
  colunas_json = CONCAT(
    colunas_json,
    FORMAT(
      "CAST(JSON_VALUE(data, '$.%s') AS STRING) AS %s,\n",
      coluna.nome,
      coluna.nome
    )
  );

SET
  colunas_select = CONCAT(
    colunas_select,
    FORMAT("'%s', %s,\n", coluna.nome, coluna.nome)
  );

SET
  colunas_resultado = CONCAT(
    colunas_resultado,
    FORMAT(
      "  JSON_VALUE(data, '$.%s') AS %s,\n",
      coluna.nome,
      coluna.nome
    )
  );

-- Verifica se o nome da coluna contém palavras-chave para gerar valor numérico
IF REGEXP_CONTAINS(
  LOWER(coluna.nome),
  r"id|number|type|tax|rate|currency|valeu|balance|numero|taxa|valor|cpf|cnpj|rg|pis|titulo|cns|renavam|certidao|total|unitario|bruto|liquido|desconto|multa|juros|pago|recebido|financiado|parcela|icms|ipi|ir|comissao|cambio|indice|agencia|conta|banco|operacao|cheque|documento|quantidade|estoque|fiscal|pedido|produto|contrato|percentual|digito|nota"
) THEN
SET
  colunas_source = CONCAT(
    colunas_source,
    FORMAT(
      "CAST(FLOOR(100000 + RAND()*999999) AS INT64) AS %s,\n",
      coluna.nome
    )
  );

ELSEIF REGEXP_CONTAINS(LOWER(coluna.nome), r"data|date") THEN
SET
  colunas_source = CONCAT(
    colunas_source,
    FORMAT(
      "TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL CAST(FLOOR(RAND()*30) AS INT64) DAY) AS %s,\n",
      coluna.nome
    )
  );

ELSE
SET
  colunas_source = CONCAT(
    colunas_source,
    FORMAT(
      "CONCAT('%s_', CAST(FLOOR(1000 + RAND()*9000) AS STRING)) AS %s,\n",
      coluna.nome,
      coluna.nome
    )
  );

END IF;

END
FOR;

-- Remove vírgulas finais
SET
  colunas_json = RTRIM(colunas_json, ',\n');

SET
  colunas_select = RTRIM(colunas_select, ',\n');

SET
  colunas_source = RTRIM(colunas_source, ',\n');

SET
  colunas_resultado = RTRIM(colunas_resultado, ',\n');

-- Gera o JSON_OBJECT dinâmico
SET
  json_object_data = FORMAT('JSON_OBJECT(\n%s\n)', colunas_select);

-- Gera o SELECT da source_data dinamicamente com quantidade controlada
SET
  source_data_query = FORMAT(
    'SELECT\n%s\nFROM UNNEST(GENERATE_ARRAY(1, %d))',
    colunas_source,
    qtde_registros
  );

-- =============================================
-- CRIAÇÃO DA TABELA DE DESTINO
-- =============================================
EXECUTE IMMEDIATE FORMAT(
  """
  CREATE TABLE IF NOT EXISTS %s (
    subscription_name STRING NOT NULL,
    message_id STRING NOT NULL,
    publish_time TIMESTAMP,
    data JSON,
    attributes JSON
  )
  PARTITION BY DATE(publish_time)
  OPTIONS(
    description='Eventos simulados - JSON dinâmico',
    labels=[
      ('frequency_ingestion', 'streaming'),
      ('origin_type', 'event_cloud_dynamic'),
      ('object_type', 'table'),
      ('table_has_pii', 'yes'),
      ('type_ingestion', 'streaming')
    ]
  );
""",
  full_table_name
);

-- =============================================
-- INSERÇÃO DE DADOS SIMULADOS
-- =============================================
EXECUTE IMMEDIATE FORMAT(
  """
  INSERT INTO %s (
    subscription_name,
    message_id,
    publish_time,
    data,
    attributes
  )
  WITH source_data AS (
    %s
  )
  SELECT
    'subscription-exchanges-operation' AS subscription_name,
    CONCAT('msg-', CAST(GENERATE_UUID() AS STRING)) AS message_id,
    CURRENT_TIMESTAMP() AS publish_time,
    %s AS data,
    JSON_OBJECT(
      'event_id', CONCAT('evt-', CAST(GENERATE_UUID() AS STRING)),
      'event_type', 'exchange.operation',
      'event_version', '1.0',
      'source', 'exchange-operation-service',
      'correlation_id', CONCAT('corr-', CAST(GENERATE_UUID() AS STRING)),
      'created_at', FORMAT_TIMESTAMP('%%Y-%%m-%%dT%%H:%%M:%%S-03:00', CURRENT_TIMESTAMP())
    ) AS attributes
  FROM source_data;
""",
  full_table_name,
  source_data_query,
  json_object_data
);

-- =============================================
-- VISUALIZAÇÃO DOS DADOS INSERIDOS
-- =============================================
EXECUTE IMMEDIATE FORMAT(
  """
  SELECT
%s
  FROM %s;
""",
  colunas_resultado,
  full_table_name
);