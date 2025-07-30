-- =======================================================================================================
-- A cláusula INTERLEAVE IN PARENT no Cloud Spanner define uma hierarquia física de tabelas (aninhamento),
-- otimizando leituras e escritas em dados relacionados fortemente.
--
-- Sugestoes uso:
-- Quando a relação é 1:N fortemente acoplada e você acessa sempre junto (Cliente + Telefone).
--
-- Ideal para manter proximidade física dos dados e melhorar performance.
--
-- Documentação Oficial:
-- https://cloud.google.com/spanner/docs/schema-and-data-model?hl=pt-br#parent-child
-- =====================================================================

-- ===============================================================
-- Tabelas a serem criadas:
--  1. Tabela pai: Cliente
-- ===============================================================

CREATE TABLE Cliente (
  ClienteId STRING(36) NOT NULL,
  Nome STRING(100),
  Email STRING(100)
) PRIMARY KEY (ClienteId);

-- ===============================================================
--  2. Tabela filha: Telefone, INTERLEAVE com Cliente:
-- ===============================================================
-- O que isso faz:
-- Os dados de Telefone são fisicamente armazenados junto dos seus respectivos Cliente.
-- A chave primária da tabela filha começa com a chave do pai.
--
-- ON DELETE CASCADE: se Cliente for Deletada, seus Telefones também são automaticamente removidos!
-- =================================================================================================

CREATE TABLE Telefone (
  ClienteId STRING(36) NOT NULL,
  TelefoneId STRING(36) NOT NULL,
  Numero STRING(20)
) PRIMARY KEY (ClienteId, TelefoneId),
  INTERLEAVE IN PARENT Cliente ON DELETE CASCADE;

-- ===============================================================
--  2. Tabela filha: Pedido, INTERLEAVE com Cliente:
-- ===============================================================

CREATE TABLE Pedido (
  ClienteId STRING(36) NOT NULL,
  PedidoId STRING(36) NOT NULL,
  DataPedido DATE,
  ValorTotal FLOAT64
) PRIMARY KEY (ClienteId, PedidoId),
  INTERLEAVE IN PARENT Cliente ON DELETE CASCADE;

-- ===============================================================
-- O que isso faz:
-- Os dados de Pedido são fisicamente armazenados junto dos seus respectivos Cliente.
-- A chave primária da tabela filha começa com a chave do pai.
--
-- ON DELETE CASCADE: se Cliente for Deletada, seus Pedido também são automaticamente removidos! É apenas um exemplo; ok
-- ======================================================================================================================

-- ================================================================================
-- Exemplo Select dos dados do cliente, seus telefones e seus pedidos relacionados;
-- ================================================================================

SELECT
  c.ClienteId,
  c.Nome,
  c.Email,
  t.Numero AS Telefone,
  p.PedidoId,
  p.DataPedido,
  p.ValorTotal
FROM 
  Cliente c
  LEFT JOIN Telefone t ON c.ClienteId = t.ClienteId
  LEFT JOIN Pedido p ON c.ClienteId = p.ClienteId
ORDER BY
  c.ClienteId;

-- ======================================================================================================================
