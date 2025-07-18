-- ====================================================
--  Job Export event Cloud Spanner (yaml) - GCP Google 
-- ================================================

-- Dados BIGQUERY Origem - Source:
DECLARE project_id STRING DEFAULT 'Project_ID';
DECLARE table_bq STRING DEFAULT ' DatasetBigqueryOrigem.Table_Bigquery';


-- Dados SPANNER Destino - Target:
DECLARE spanner_instance STRING DEFAULT 'Servidor_Spanner99';
DECLARE spanner_database STRING DEFAULT 'Database_Spanner';
DECLARE table_spanner STRING DEFAULT ' DatasetSpannerDestino.Table_Spanner';


-- ======================
-- CONSTANT - FIXED !
-- ======================
DECLARE bq_source STRING DEFAULT FORMAT('`%s.%s`', project_id, table_bq);

DECLARE temp_table STRING DEFAULT CONCAT('temp_export');

DECLARE full_table_bq STRING DEFAULT FORMAT('%s', table_bq);

DECLARE spanner_table STRING DEFAULT FORMAT('%s', table_spanner);

DECLARE spanner_options STRING DEFAULT FORMAT('{"table": "%s"}', spanner_table);

-- =============================================================
-- DEFINA AS COLUNAS PARA EXTRAÇÃO - AJUSTE PARA NOVAS COLUNAS;
-- =====================================================
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

DECLARE colunas_json STRING DEFAULT '';
DECLARE colunas_select STRING DEFAULT '';

-- ========================================
-- CONSTRUÇÃO DINÂMICA DOS CAMPOS (CAST...)
-- ========================================
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
  colunas_select = CONCAT(colunas_select, FORMAT("%s,\n", coluna.nome));

END
FOR;

-- Necessário remover a vírgula das colunas, não alterar; 
SET
  colunas_json = RTRIM(colunas_json, ',\n');
SET
  colunas_select = RTRIM(colunas_select, ',\n');

-- ====================================
-- CRIA TABELA TEMPORÁRIA COM OS CAMPOS
-- ====================================
EXECUTE IMMEDIATE FORMAT(
  """
  CREATE OR REPLACE TEMP TABLE %s AS
  SELECT
    %s
  FROM %s
""",
  temp_table,
  colunas_json,
  bq_source
);

-- =====================================
-- EXPORTAÇÃO PARA SPANNER 
-- =====================================
EXECUTE IMMEDIATE FORMAT(
  """
  EXPORT DATA OPTIONS (
    uri               = 'https://spanner.googleapis.com/projects/%s/instances/%s/databases/%s',
    format            = 'CLOUD_SPANNER',
    spanner_options   = '%s'
  )
  AS
  SELECT
    %s
  FROM %s
  WHERE TRUE
""",
  project_id,
  spanner_instance,
  spanner_database,
  spanner_options,
  colunas_select,
  temp_table
);