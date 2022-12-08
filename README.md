# BD_Proc_2
Segunda Entrega BD_Processing

En esta segunda entrega encontrarás los siguientes cambios: 
 - Incluye el Batch para alimentar 3 TABLAS. 
          + Estoy utilizando las dos tablas que se solicitan en la Práctica para el BATCH -> 
            CREATE TABLE bytes_hourly(timestamp TIMESTAMP, id TEXT, value BIGINT, type TEXT);
            CREATE TABLE user_quota_limit(email TEXT, usage BIGINT, quota BIGINT, timestamp TIMESTAMP);
          + Generé una NUEVA TABLA ADICIONAL para poder reportar los usuarios que han excedido su quota -> 
            CREATE TABLE IF NOT EXISTS exceed (email TEXT, usage BIGINT, quota BIGINT, timestamp TIMESTAMP, exceeded TEXT)
            
Vuelvo a incluir un powerpoint con las pantallas que demuestran las tablas que se han generado llamada "segunda_entrega_BDP.pptx"

Espero tus comentarios
