-- 💳 3. Tabla de Dimensión: Finanzas
CREATE TABLE silver_layer.dim_finance AS
SELECT 
  unique_id,
  iban
FROM bronze_layer.batch_first_load;
