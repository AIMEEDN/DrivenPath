-- 📅 2. Tabla de Dimensión: Fecha
CREATE TABLE silver_layer.dim_date AS
SELECT 
  unique_id,
  accessed_at
FROM bronze_layer.batch_first_load;