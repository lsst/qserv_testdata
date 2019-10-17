-- Tests that an ORDER BY column with SELECT * (without naming the order by column) works.

SELECT * FROM Object ORDER BY objectId;
