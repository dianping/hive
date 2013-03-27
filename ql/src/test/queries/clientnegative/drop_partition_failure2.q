-- Verifies that dropping a partition from a table that doesn't exist fails appropriately

ALTER TABLE fake_table DROP PARTITION (part = '1');

