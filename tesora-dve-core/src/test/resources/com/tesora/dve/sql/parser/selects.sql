---
-- #%L
-- Tesora Inc.
-- Database Virtualization Engine
-- %%
-- Copyright (C) 2011 - 2014 Tesora Inc.
-- %%
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License, version 3,
-- as published by the Free Software Foundation.
-- 
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
-- GNU Affero General Public License for more details.
-- 
-- You should have received a copy of the GNU Affero General Public License
-- along with this program. If not, see <http://www.gnu.org/licenses/>.
-- #L%
---
select 1 - 2 from comment;
select 1 + -2 from comment;
select 1 -2 from comment;
select 1 -2 -3 -4 from comment;
select 1 + 2 -2 + 3 * -2 from comment;
SELECT COUNT(*) AS count FROM  comment c1 INNER JOIN comment c2 ON c2.nid = c1.nid WHERE  (c2.cid = '1') AND (SUBSTRING(c1.thread, 1, (LENGTH(c1.thread) -1)) < SUBSTRING(c2.thread, 1, (LENGTH(c2.thread) -1)));
