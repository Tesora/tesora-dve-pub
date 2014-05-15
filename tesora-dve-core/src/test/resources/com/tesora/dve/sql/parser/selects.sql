select 1 - 2 from comment;
select 1 + -2 from comment;
select 1 -2 from comment;
select 1 -2 -3 -4 from comment;
select 1 + 2 -2 + 3 * -2 from comment;
SELECT COUNT(*) AS count FROM  comment c1 INNER JOIN comment c2 ON c2.nid = c1.nid WHERE  (c2.cid = '1') AND (SUBSTRING(c1.thread, 1, (LENGTH(c1.thread) -1)) < SUBSTRING(c2.thread, 1, (LENGTH(c2.thread) -1)));
