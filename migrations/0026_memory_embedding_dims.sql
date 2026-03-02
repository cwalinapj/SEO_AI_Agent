PRAGMA foreign_keys = ON;

ALTER TABLE memory_items ADD COLUMN embedding_dims INTEGER;

UPDATE memory_items
SET embedding_dims = CASE
  WHEN embedding_model = 'text-embedding-3-large' THEN 3072
  WHEN embedding_model = 'text-embedding-3-small' THEN 1536
  ELSE embedding_dims
END
WHERE embedding_dims IS NULL;
