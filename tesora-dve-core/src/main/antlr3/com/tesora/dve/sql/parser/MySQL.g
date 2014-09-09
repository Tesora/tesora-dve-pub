parser grammar MySQL;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */


options

{
    language=Java;
//    tokenVocab=sql2003Lexer;
    // TODO explore removing this option
    output=AST;
    k=3;
}

@members {

  public TranslatorUtils utils;

  public void setUtils(TranslatorUtils u) { utils = u; }

  public void error(String descrip, String text) {
    utils.error(Pass.FIRST, descrip, text);
  }

  public String getErrorMessage(RecognitionException e, String[] tokenNames) {
    List stack = getRuleInvocationStack(e, this.getClass().getName());
    String msg = null;
    if ( e instanceof NoViableAltException ) {
      NoViableAltException nvae = (NoViableAltException)e;
      msg = " no viable alt; token="+e.token+
            " (decision="+nvae.decisionNumber+
            " state "+nvae.stateNumber+")"+
            " decision=<<"+nvae.grammarDecisionDescription+">>";
    } else {
      msg = super.getErrorMessage(e, tokenNames);
    }
    String onLine = utils.extractLine(this, e);
    return utils.formatErrorMessage(stack,msg,onLine);
  }

  public String getTokenErrorDisplay(Token t) {
    return t.toString();
  }

  // override to collect errors
  public void emitErrorMessage(String s) {
    utils.collectError(Pass.FIRST, s);
  }

  // override to programmatically control tracing
  public void traceIn(String ruleName, int ruleIndex)  {
    if (utils.getOptions().isTraceParser())
      super.traceIn(ruleName, ruleIndex);
  }

  public void traceOut(String ruleName, int ruleIndex) {
    if (utils.getOptions().isTraceParser())
      super.traceOut(ruleName, ruleIndex);
  }
  
  public Object bki(Object ctoken) {
    return utils.buildKeywordEscapeIdentifier(this.adaptor,ctoken); 
  }

  public Name bkn(Object ctoken) {
    return utils.buildName(bki(ctoken));
  }

  public ExpressionNode bkexpr(Object ctoken) {
    return utils.buildIdentifierLiteral(bkn(ctoken));
  }

  public void unsupported(String desc) {
    utils.unsupported(desc);
  }
}


// incomplete
sql_data_statement returns [Statement s] options {k=1;}:
  select_statement { $s = $select_statement.s; }
  | insert_statement { $s = $insert_statement.s; }
  | update_statement { $s = $update_statement.s; }
  | delete_statement { $s = $delete_statement.s; }
  | truncate_statement { $s = $truncate_statement.s; }
  | load_data_infile_statement { $s = $load_data_infile_statement.s; }
  ;

select_statement returns [ProjectingStatement s] options{k=1;}:
  (nestable_select_statement { $s = $nestable_select_statement.s; })
  | (non_nestable_select_statement { $s = $non_nestable_select_statement.s; })
  ;

non_nestable_select_statement returns [ProjectingStatement s] options{k=1;}
  @init {
    List<ProjectingStatement> stmts = new ArrayList<ProjectingStatement>();
    List<Boolean> unionOps = new ArrayList<Boolean>();
    int scopeid = -1;
  }
  :
  (ls=parens_basic_toplevel_select { stmts.add($ls.s);  scopeid = utils.getLastPoppedScope(); }) 
  ((UNION ALL? { unionOps.add($ALL != null); }) rs=parens_basic_toplevel_select { stmts.add($rs.s); })*
  { utils.repushScope(scopeid); } 
  orderby? limit_specification?
  { utils.popScope();
    $s = utils.maybeBuildUnionStatement(stmts,unionOps);
    $s.setOrderBy($orderby.l);
    $s.setLimit($limit_specification.ls);
  }
  ;
  
nestable_select_statement returns [ProjectingStatement s] options{k=1;}
  @init {
    List<ProjectingStatement> stmts = new ArrayList<ProjectingStatement>();
    List<Boolean> unionOps = new ArrayList<Boolean>();
    List<Pair<LimitSpecification,List>> obls = new ArrayList<Pair<LimitSpecification,List>>();
    int scopeid = -1;
  }
  :
  (ls=basic_toplevel_select { stmts.add($ls.s); scopeid = utils.getLastPoppedScope(); })
  ((UNION ALL? { unionOps.add($ALL != null); })
   ((nps=basic_toplevel_select { stmts.add($nps.s); })
    |
    ((ps=parens_basic_toplevel_select { stmts.add($ps.s); })
     { utils.repushScope(scopeid); }
     orderby? limit_specification?
     { utils.popScope(); obls.add(new Pair($limit_specification.ls,$orderby.l)); }    
    )
   )
  )*
  { $s = utils.maybeBuildUnionStatement(stmts,unionOps); 
    if (!obls.isEmpty()) {
      $s.setOrderBy(obls.get(obls.size() - 1).getSecond());
      $s.setLimit(obls.get(obls.size() - 1).getFirst());
    }
  }
  ;
  
maybe_parens_basic_toplevel_select returns [ProjectingStatement s] options {k=1;}:
  (basic_toplevel_select { $s = $basic_toplevel_select.s; })
  | (parens_basic_toplevel_select { $s = $parens_basic_toplevel_select.s; })
  ;

parens_basic_toplevel_select returns [ProjectingStatement s] options{k=1;}:
  (Left_Paren g=basic_toplevel_select Right_Paren { $s = utils.addGrouping($g.s); })
  ;

basic_toplevel_select returns [ProjectingStatement s] options {k=1;}:
  SELECT { utils.pushDML("select"); }
  push_unresolving_scope
  // rolled set_quantifiers into the extension options
  mysql_select_extension_option_list
  // set_quantifier?
  select_list
  { utils.storeProjection($select_list.l); }
  simple_select_from
  { $s = utils.buildSelectStatement($simple_select_from.m, $select_list.l, $mysql_select_extension_option_list.l, $SELECT);  utils.popScope(); }
  ;

// this has been broken up in anticipation of further mods around query support - for instance for select ... from dual
simple_select_from returns [Map m] options{k=1;}:
  { $m = new HashMap(); }
  (FROM table_reference_list)?
  { utils.resolveProjection(); }
  where?
  { utils.setGroupByNamespace(); }
  groupby?
  { utils.setHavingNamespace(); }
  having?
  { utils.setTrailingNamespace(); }
  // missing: window_clause?
  orderby?
  limit_specification?
  select_lock_type?
  { $m.put(EdgeName.TABLES, $table_reference_list.l); 
    $m.put(EdgeName.WHERECLAUSE, $where.expr);
    $m.put(EdgeName.GROUPBY, $groupby.l);
    $m.put(EdgeName.ORDERBY, $orderby.l);
    $m.put(EdgeName.LIMIT, $limit_specification.ls);
    $m.put(Statement.SELECT_LOCK_ATTRIBUTE, $select_lock_type.b);
    $m.put(EdgeName.HAVING, $having.expr);
  }
  ;

// break out pieces of the select in order to reuse them
orderby returns [List l] options{k=1;}:
  ORDER BY sorting_spec_list
  { $l = $sorting_spec_list.l; }
  ;

having returns [ExpressionNode expr] options {k=1;}:
  HAVING value_expression
  { $expr = $value_expression.expr; }
  ; 

groupby returns [List l] options{k=1;}:
  GROUP BY sorting_spec_list
  { $l = $sorting_spec_list.l; }
  ;
  
where returns [ExpressionNode expr] options{k=1;}:
  WHERE value_expression
  { $expr = $value_expression.expr; }
  ;
  
select_lock_type returns [Boolean b] options{k=1;}:
  FOR UPDATE
  { $b = Boolean.TRUE; }
  ;
  
set_quantifier returns [SetQuantifier sq] options {k=1;}:
    DISTINCT { $sq = SetQuantifier.DISTINCT; }
    | ALL { $sq = SetQuantifier.ALL; }
    ;

mysql_select_extension_option_list returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  ((o=mysql_select_extension_option { $l.add($o.o); }) | (sq=set_quantifier { $l.add($sq.sq); }))*
  ;
  
mysql_select_extension_option returns [MysqlSelectOption o] options {k=1;}:
  SQL_CALC_FOUND_ROWS { $o = MysqlSelectOption.SQL_CALC_FOUND_ROWS; }
  | HIGH_PRIORITY { $o = MysqlSelectOption.HIGH_PRIORITY; }
  | STRAIGHT_JOIN { $o = MysqlSelectOption.STRAIGHT_JOIN; }
  | SQL_SMALL_RESULT { $o = MysqlSelectOption.SQL_SMALL_RESULT; }
  | SQL_BIG_RESULT { $o = MysqlSelectOption.SQL_BIG_RESULT; }
  | SQL_BUFFER_RESULT { $o = MysqlSelectOption.SQL_BUFFER_RESULT; }
  | SQL_CACHE { $o = MysqlSelectOption.SQL_CACHE; }
  | SQL_NO_CACHE { $o = MysqlSelectOption.SQL_NO_CACHE; }
  | SQL_NO_FCACHE { $o = MysqlSelectOption.SQL_NO_FCACHE; }
  ;

sorting_spec_list returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  lob=order_by_value { $l.add($lob.obs); } (Comma tob=order_by_value { $l.add($tob.obs); })*
  ;

order_by_value returns [SortingSpecification obs] options {k=1;}: 
  value_expression (a=ASC | d=DESC)? 
  { $obs = utils.buildSortingSpecification($value_expression.expr, $a.text, $d.text); }
  ;

limit_specification returns [LimitSpecification ls] options {k=1;}:
  LIMIT fl=limit_value ((OFFSET sl=limit_value)? | (Comma tl=limit_value))
  { ExpressionNode rc = null; ExpressionNode ov = null;
    if ($sl.expr != null) { ov = $sl.expr; rc = $fl.expr; }
    else if ($tl.expr != null) { rc = $tl.expr; ov = $fl.expr; }
    else { rc = $fl.expr; }
    $ls = utils.buildLimitSpecification(rc, ov);
  }    
  ;

// when we support stored procs, we'll have to support
// identifiers here, but not quite yet
limit_value returns [ExpressionNode expr] options {k=1;}:
  Question_Mark { $expr = utils.buildParameter($Question_Mark); }
  | unsigned_integral_literal { $expr = $unsigned_integral_literal.expr; }
  | rhs_variable_ref { $expr = $rhs_variable_ref.vi; }
  ;

table_reference_list returns [List l] options {k=1;}:
  { $l = new ArrayList(); } 
  lt=table_reference { $l.add($lt.ft); } (Comma tt=table_reference { $l.add($tt.ft); })* 
  ;

simple_table_factor returns [ExpressionNode expr] options {k=1;}:
  qualified_identifier (AS? unqualified_identifier)?  index_hints? { $expr = utils.buildTableInstance($qualified_identifier.n, $unqualified_identifier.n, $index_hints.l); }
  ;

index_hints returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  ( th=index_hint {$l.add($th.h);} )+
  ;

index_hint returns [IndexHint h] options {k=1;}:
  index_hint_type (INDEX | k=KEY) (FOR targ=index_hint_target)? 
  (Left_Paren unqualified_identifier_list Right_Paren)
  { $h = utils.buildIndexHint($index_hint_type.ht, ($k != null), $index_hint_target.ht, $unqualified_identifier_list.l); }
  ;

index_hint_type returns [HintType ht] options {k=1;}:
  USE { $ht = HintType.USE; }
  | IGNORE { $ht = HintType.IGNORE; }
  | FORCE { $ht = HintType.FORCE; }
  ;
  
index_hint_target returns [HintTarget ht] options {k=1;}:
  JOIN { $ht = HintTarget.JOIN; }
  | ORDER BY { $ht = HintTarget.ORDERBY; }
  | GROUP BY { $ht = HintTarget.GROUPBY; }
  ;

table_reference returns [FromTableReference ft] options {k=1;}:
  { ArrayList joins = new ArrayList(); }
  table_factor (jr=join_rhs { joins.add($jr.jt); })*
  { $ft = utils.buildFromTableReference($table_factor.expr, joins); }
  ;
  
table_factor returns [ExpressionNode expr] options {k=2;}:
  (simple_table_factor { $expr = $simple_table_factor.expr; })
// orig
//  | Left_Paren 
//    ((select_statement Right_Paren AS? unqualified_identifier { $expr = utils.buildSubquery($select_statement.s, $unqualified_identifier.n, null,true); })
//    |(joined_table Right_Paren { $expr = utils.addGrouping($joined_table.expr); }))
    | (Left_Paren joined_table Right_Paren { $expr = utils.addGrouping($joined_table.expr); })
    | (Left_Paren nestable_select_statement Right_Paren AS? unqualified_identifier { $expr = utils.buildSubquery($nestable_select_statement.s, $unqualified_identifier.n, null, true); })
  ;

joined_table returns [ExpressionNode expr] options {k=1;}:
  { ArrayList joins = new ArrayList(); }
  table_factor (jt=join_rhs { joins.add($jt.jt); })*
  { $expr = ((joins.size()==0) ? $table_factor.expr : utils.buildTableJoin($table_factor.expr, joins)); }
  ;

join_rhs returns [JoinedTable jt] options {k=1;}:
  join_type? JOIN table_factor join_specification?
  { $jt = utils.buildJoinedTable($table_factor.expr, $join_specification.expr, $join_type.js); }
  ;

join_specification returns [JoinClauseType expr] options {k=1;}: 
  ((ON value_expression) { $expr = utils.buildJoinClauseType($value_expression.expr, null); })
  | ((USING Left_Paren qualified_identifier_list Right_Paren) { $expr = utils.buildJoinClauseType(null, $qualified_identifier_list.l); })
  ;
  
join_type returns [JoinSpecification js] options {k=1;}:
  (ijt=(CROSS | INNER) { $js = utils.buildJoinType($ijt.text, null); })
  | outer_join_type OUTER? { $js = utils.buildJoinType($outer_join_type.text, $OUTER.text); }
  ;
outer_join_type options {k=1;}: 
  LEFT | RIGHT | FULL
  ;


select_list returns [List l] options {k=1;}
  @init{ $l = new ArrayList(); Token lhs = input.LT(1); Token rhs = null; }
  :
  (li=select_item { $l.add($li.expr); }
   (Comma ti=select_item { rhs = $Comma; utils.updateSourcePosition($l.get($l.size() - 1), lhs, rhs); lhs = rhs; $l.add($ti.expr); })*
  )
  { rhs = input.LT(1);
    utils.updateSourcePosition($l.get($l.size() - 1), lhs, rhs);
  }
  ;
  
select_item returns [ExpressionNode expr] options {k=1;}:
  Asterisk { $expr = utils.buildWildcard($Asterisk); }
  |
  // TODO - Character_String_Literal can match for a function call as well when AS is missing
  // so make AS be required for that case and come back to this later
  value_expression ((AS^)? (unqualified_identifier | Character_String_Literal))? 
  { $expr = utils.maybeBuildExprAlias($value_expression.expr, $unqualified_identifier.n, $Character_String_Literal.text, $AS.tree); }
  ;

insert_statement returns [Statement s] options{k=2;}:
  ((kw=((INSERT { utils.pushDML("insert"); })
        | 
        (r=REPLACE { utils.pushDML("replace");})
        ) (im=insert_statement_modifier)? IGNORE? INTO? push_scope resolvable_table_ref { utils.pushSkeletonInsert($resolvable_table_ref.expr, $r != null, $kw); })
   ( (SET update_list suldup=on_duplicate_key_update? { $s = utils.buildInsertIntoSetStatement($update_list.l, ($IGNORE != null),$suldup.l, $im.im); })
     | 
     (insert_field_specification?
      (nsv=simple_value_specification | ss=select_statement)
      (ndup=on_duplicate_key_update)?
      {  if ($nsv.l != null)
           $s = utils.buildInsertStatement($nsv.l, $ndup.l, $im.im, ($IGNORE != null));
         else
           $s = utils.buildInsertIntoSelectStatement(utils.buildSubquery($ss.s,null,null,false),($IGNORE != null),$ndup.l);
      }
      
      )))
    ;
   
fast_insert_statement returns [Statement s] options {k=1;}:
  INSERT { utils.pushDML("insert"); } insert_statement_modifier? IGNORE? push_scope INTO? resolvable_table_ref { utils.pushSkeletonInsert($resolvable_table_ref.expr,false,$INSERT); } fast_insert_field_specification? fast_simple_value_specification
  { $s = utils.buildInsertStatement($fast_simple_value_specification.l, null, null, ($IGNORE != null)); 
    utils.popScope(); } 
;

// for now eat the LOW/HIGH PRIORITY flags
insert_statement_modifier returns [InsertModifier im] options {k=1;}:
  DELAYED { $im = InsertModifier.DELAYED; }
  | LOW_PRIORITY { $im = null; }
  | HIGH_PRIORITY { $im = null; }
  ;

resolvable_table_ref returns [ExpressionNode expr] options {k=1;}:
  qualified_identifier { $expr = utils.buildTableInstance($qualified_identifier.n, null); }
  ;

insert_column_ref returns [ExpressionNode expr] options {k=1;}:
  qualified_identifier { $expr = utils.pushInsertSkeletonField(utils.buildColumnReference($qualified_identifier.n)); }
  ;

insert_field_specification_body returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  (lrc=insert_column_ref { $l.add($lrc.expr); }(Comma trc=insert_column_ref { $l.add($trc.expr); })*)
  ;
  
insert_field_specification returns [List l] options {k=1;}:
  Left_Paren insert_field_specification_body? Right_Paren
  { $l = $insert_field_specification_body.l; } 
  ;

simple_value_specification returns [List l] options {k=1;}:
  VALUES insert_value_list
  { $l = $insert_value_list.l; } 
  ;

insert_value_list returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  liv=insert_value_specification_body { $l.add($liv.l); } 
  (Comma tiv=insert_value_specification_body { $l.add($tiv.l); utils.enough(this,$l); })*
  ;

continuation_insert_value_list returns [List l] options {k=1;}:
   insert_value_list? on_duplicate_key_update?
   { if ($on_duplicate_key_update.l != null) utils.reportContinuationOnDupKey(); $l = $insert_value_list.l; }
  ;

on_duplicate_key_update returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  ON DUPLICATE KEY UPDATE dk=value_expression { $l.add($dk.expr); } (Comma dk2=value_expression { $l.add($dk2.expr); })*
  ;

insert_value_specification_body returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  Left_Paren (leod=expression_or_default { $l.add($leod.expr); }(Comma teod=expression_or_default { $l.add($teod.expr); })*)? Right_Paren 
  ;

expression_or_default returns [ExpressionNode expr] options {k=1;}: 
  value_expression { $expr = $value_expression.expr; } 
  | DEFAULT { $expr = utils.buildDefaultKeywordExpr($DEFAULT); }
  ;

fast_insert_field_specification returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  Left_Paren (lrc=insert_column_ref { $l.add($lrc.expr); }(Comma trc=insert_column_ref { $l.add($trc.expr); })*) Right_Paren 
  ;


fast_simple_value_specification returns [List l] options {k=1;}:
  VALUES fast_insert_value_list
  { $l = $fast_insert_value_list.l; } 
  ;

fast_insert_value_list returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  liv=fast_insert_value_specification_body { $l.add($liv.l); } 
  (Comma tiv=fast_insert_value_specification_body { $l.add($tiv.l); utils.enough(this,$l); })*
  ;

fast_continuation_insert_value_list returns [List l] options {k=1;}:
  fast_insert_value_list? on_duplicate_key_update?
  { if ($on_duplicate_key_update.l != null) utils.reportContinuationOnDupKey(); $l = $fast_insert_value_list.l; }
  ;

fast_insert_value_specification_body returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  Left_Paren (leod=fast_insert_value { $l.add($leod.expr); }(Comma teod=fast_insert_value { $l.add($teod.expr); })*) Right_Paren 
  ;

fast_insert_value returns [ExpressionNode expr] options {k=1;}:
  DEFAULT { $expr = utils.buildDefaultKeywordExpr($DEFAULT); }
  | NULL { $expr = utils.buildLiteral($NULL); }
  | fast_literal { $expr = $fast_literal.expr; }
  ;

fast_literal returns [ExpressionNode expr] options {k=1;}:
  string_literal { $expr = $string_literal.expr; }
  | unsigned_numeric_literal { $expr = $unsigned_numeric_literal.expr; }
  | signed_numeric_literal { $expr = $signed_numeric_literal.expr; }
  | Bit_String_Literal { $expr = utils.buildLiteral($Bit_String_Literal); }
  | Hex_String_Literal { $expr = utils.buildLiteral($Hex_String_Literal); }
  ;


// the rewrite rule for update looks a bit like select to aid with the translation
update_statement returns [Statement s] options {k=1;}:
  UPDATE IGNORE? push_scope { utils.pushDML("update"); }
  table_reference_list
  SET update_list
  where?
  { utils.setTrailingNamespace(); }
  orderby?
  limit_specification? 
  { $s = utils.buildUpdateStatement($table_reference_list.l, $update_list.l, $where.expr, $orderby.l, 
      $limit_specification.ls, ($IGNORE != null), $UPDATE);
    utils.popScope(); 
  } 
  ;

update_list returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  lu=update_item { $l.add($lu.expr); }(Comma tu=update_item { $l.add($tu.expr); })* 
  ;

update_item returns [ExpressionNode expr] options {k=1;}:
  qualified_identifier^ Equals_Operator expression_or_default
  { $expr = utils.buildUpdateExpression(new FunctionName($Equals_Operator.text, $Equals_Operator.type,true), $qualified_identifier.n, $expression_or_default.expr, $qualified_identifier.tree); }
  ;

delete_statement returns [Statement s] options {k=1;}:
  DELETE push_scope { utils.pushDML("delete"); }
  LOW_PRIORITY? QUICK?
  qualified_identifier_list?
  (FROM table_reference_list)
  where?
  orderby?
  limit_specification?
  { $s = utils.buildDeleteStatement($table_reference_list.l, $qualified_identifier_list.l, 
     $where.expr, $orderby.l, $limit_specification.ls, $DELETE);
    utils.popScope(); 
  }
  ;


truncate_statement returns [Statement s] options {k=1;}:
  TRUNCATE TABLE? push_scope { utils.pushDML("truncate"); } qualified_identifier
  { $s = utils.buildTruncateStatement($qualified_identifier.n, $TRUNCATE); utils.popScope(); }
  ;

load_data_infile_statement returns [Statement s] options {k=1;}:
  LOAD DATA load_data_infile_statement_modifier? LOCAL? INFILE Character_String_Literal
  (rep=REPLACE | ig=IGNORE)?
  INTO TABLE tbl=qualified_identifier
  (CHARACTER SET cs=unqualified_identifier)?
  load_data_infile_col_option?
  load_data_infile_line_option?
  load_data_infile_ignore_line_option?
  load_data_infile_col_or_var_list?
  (SET update_list)?
  { $s = utils.buildLoadDataInfileStatement($load_data_infile_statement_modifier.ldim, ($LOCAL != null), $Character_String_Literal.text, ($rep != null), ($ig != null), $tbl.n, $cs.n, $load_data_infile_col_option.co, $load_data_infile_line_option.lo, $load_data_infile_ignore_line_option.expr, $load_data_infile_col_or_var_list.l, $update_list.l); }
  ;
    
load_data_infile_statement_modifier returns [LoadDataInfileModifier ldim] options {k=1;}:
  LOW_PRIORITY { $ldim = LoadDataInfileModifier.LOW_PRIORITY; }
  | CONCURRENT { $ldim = LoadDataInfileModifier.CONCURRENT; }
  ;
    
load_data_infile_col_option returns [LoadDataInfileColOption co] options {k=1;}:
  (FIELDS | COLUMNS) (TERMINATED BY term=Character_String_Literal)? (OPTIONALLY? ENCLOSED BY enc=Character_String_Literal)? (ESCAPED BY esc=Character_String_Literal)?
  { $co = utils.buildLoadDataInfileColOption($term.text,$enc.text,$esc.text); }
  ;

load_data_infile_line_option returns [LoadDataInfileLineOption lo] options {k=1;}:
  LINES (STARTING BY st=Character_String_Literal)? (TERMINATED BY term=Character_String_Literal)?
  { $lo = utils.buildLoadDataInfileLineOption($st.text,$term.text); }
  ;

load_data_infile_ignore_line_option returns [ExpressionNode expr] options {k=1;}: 
  IGNORE unsigned_integral_literal (LINES | ROWS) { $expr = $unsigned_integral_literal.expr; }
  ;

load_data_infile_col_or_var_list returns [List l] options {k=1;}:  
  { $l = new ArrayList(); }
  Left_Paren id1=qualified_identifier { $l.add($id1.n); }(Comma id2=qualified_identifier { $l.add($id2.n); })* Right_Paren
  ;
    
comment returns [String str] options {k=1;}:
  FIELD_COMMENT Character_String_Literal 
  { $str = $Character_String_Literal.text; }
  ;


value_expression returns [ExpressionNode expr] options {k=1;}
  @init { PrecedenceCollector acc = utils.buildPrecedenceCollector(this.adaptor); }
  :
  (lhs=boolean_expr { acc.addExpr($lhs.expr); })
  (o=boolean_operator rhs=boolean_expr { acc.addOp($o.tree); acc.addExpr($rhs.expr); })*
  { $expr = acc.build(); }
  ;
  
//expression_set returns [ExpressionNode expr] options {k=1;}
//  @init { List values = new ArrayList(); }
//  :
//  e1=basic_expr { values.add($e1.expr); }(Comma e2=basic_expr { values.add($e2.expr); })+ 
//  { $expr = utils.buildExpressionSet(values); }
//  ;
  
boolean_operator options {k=1;}:
  OR
  | AND
  | XOR
  | Double_Ampersand
  ;
  

boolean_expr returns [ExpressionNode expr] options{k=1;}:
  ln=multi_not_expr? predicate (i=IS in=NOT? is_parameters)?
  { $expr = utils.buildBooleanExpr(this.adaptor, $ln.t, $predicate.expr, $i, $in, $is_parameters.l); }
  ;
  
multi_not_expr returns [Token t] options {k=1;}
  @init { List l = new ArrayList(); }
  :
  (nl=NOT { l.add($nl); } (nr=NOT { l.add($nr); })*)
  { $t = ((l.size() \% 2) == 0) ? null : $nl; }
  ;

predicate returns [ExpressionNode expr] options {k=1;}:
  lhs=math_binop_expr
  (
    ((n=NOT)?
     ((i=IN in_parameters (op=relational_binop orhs=predicate)?)
     |(b=BETWEEN blhs=math_binop_expr AND brhs=predicate)
     |(l=LIKE like_parameters)
     |((bre=REGEXP | rl=RLIKE) string_literal)
     )
    )
    |
    (op=relational_binop orhs=predicate)
  )?
  { $expr = utils.buildPredicate(this.adaptor,$lhs.expr,$n,
      $i,$in_parameters.l,
      $b,$blhs.expr,$brhs.expr,
      $l,$like_parameters.l,
      $bre,$rl, $string_literal.expr,
      $op.tree, $orhs.expr); }
  ;

relational_binop options {k=1;}:
  Equals_Operator
  | Not_Equals_Operator
  | Less_Than_Operator
  | Greater_Than_Operator
  | Less_Or_Equals_Operator
  | Greater_Or_Equals_Operator
  ;


// relational & math ops
math_binop_expr returns [ExpressionNode expr] options {k=1;}
  @init { PrecedenceCollector acc = utils.buildPrecedenceCollector(this.adaptor); }
  :
  (lhs=basic_expr { acc.addExpr($lhs.expr); })
  ((o=simple_binop rhs=basic_expr { acc.addOp($o.tree); acc.addExpr($rhs.expr); })
  |(signed_numeric_literal { acc.collectSignedNumericLiteral($signed_numeric_literal.expr); })
   )*
  { $expr = acc.build(); }
  ;
  
simple_binop options{k=1;}:
  Vertical_Bar
  | Ampersand
  | Plus_Sign
  | Minus_Sign
  | Asterisk
  | Slash
  | Percent
  | Concatenation_Operator
  ;
  
is_parameters returns [List l] options {k=1;}
  @init { $l = new ArrayList(); }
  : 
  TRUE { $l.add(utils.buildLiteral($TRUE)); }
  | FALSE { $l.add(utils.buildLiteral($FALSE)); }
  | UNKNOWN { $l.add(utils.buildLiteral($UNKNOWN)); } 
  | NULL { $l.add(utils.buildLiteral($NULL)); }
  ;

in_parameters returns [List l] options {k=1;}
  @init { $l = new ArrayList(); }
  :
  Left_Paren
  (( f=value_expression { $l.add($f.expr); }(Comma t=value_expression { $l.add($t.expr); })* ) 
   | nestable_select_statement { $l.add(utils.buildSubquery($nestable_select_statement.s,null,$nestable_select_statement.tree,false)); })
  Right_Paren 
  ; 

like_parameters returns [List l] options {k=1;}:
  (a=Character_String_Literal | p=Question_Mark) (ESCAPE b=Character_String_Literal)? 
  { $l = new ArrayList();
    if ($a != null) $l.add(utils.buildLiteral($a));
    else $l.add(utils.buildParameter($p));
    if ($b != null)
       $l.add(utils.buildLiteral($b));
  }
  | function_call_or_identifier { $l = new ArrayList(); $l.add($function_call_or_identifier.expr); }
  ;

// between_parameters returns [List l] options {k=1;}:
//   f=basic_expr AND s=basic_expr 
//   { $l = new ArrayList(); $l.add($f.expr); $l.add($s.expr); }
//   ;

// binary, collate
// collate is possibly on a separate rule, but let's see if we can get this to work well enough
basic_expr returns [ExpressionNode expr] options {k=1;}:
  (ps=Plus_Sign|ms=Minus_Sign|b=BINARY|t=Tilde)? be=negatable_basic_expr (c=COLLATE cn=collation_identifier)?
  { $expr = utils.applyExprFlags($ps,$ms,$b,$t,$be.expr,$c, $cn.n); }
  ;

negatable_basic_expr returns [ExpressionNode expr] options {k=1;}:
  literal { $expr = $literal.expr; }
  | Question_Mark { $expr = utils.buildParameter($Question_Mark); }
  | (Left_Paren ((lav=value_expression { List values = new ArrayList(); values.add($lav.expr); } (Comma tav=value_expression { values.add($tav.expr); })* { $expr = utils.buildMultivalueExpression(values); }) | (nestable_select_statement { $expr = utils.buildSubquery($nestable_select_statement.s, null, null, false); })) Right_Paren)
  | case_expression { $expr = $case_expression.expr; }
  | rhs_variable_ref { $expr = $rhs_variable_ref.vi; }
  | function_call_or_identifier { $expr = $function_call_or_identifier.expr; }
  | interval_expression { $expr = $interval_expression.expr; }
  ;

function_call_or_identifier returns [ExpressionNode expr] options {k=1;}:
  explicit_function_call { $expr = $explicit_function_call.expr; }
  | maybe_function_call { $expr = $maybe_function_call.expr; }
  ;  

explicit_function_call returns [ExpressionNode expr] options {k=1;}:
  CAST^ Left_Paren carg=value_expression AS cast_to_type Right_Paren // -> ^(FUNCALL ^(SCOMP_NAME CAST) value_expression type_name)
    { $expr = utils.buildCastCall($CAST.text, $carg.expr, $AS.text, $cast_to_type.n); }
  | CHAR^ Left_Paren function_call_params (USING unqualified_identifier)? Right_Paren
  { $expr = utils.buildCharCall($function_call_params.l, $unqualified_identifier.n); }
  | CONVERT^ Left_Paren convarg=value_expression ((USING transname=unqualified_identifier) | (Comma convert_type_description)) Right_Paren 
  { $expr = utils.buildConvertCall($convarg.expr, $transname.n, $convert_type_description.type); }
  | TIMESTAMPDIFF^ Left_Paren timestampdiff_constant_value Comma p1=basic_expr Comma p2=basic_expr Right_Paren
	{ $expr = utils.buildTimestampDiffCall($timestampdiff_constant_value.text, $p1.expr, $p2.expr); }
//  | set_function_name^ Left_Paren set_quantifier? value_expression Right_Paren 
//    { $expr = utils.buildFunctionCall($set_function_name.fn, Collections.singletonList($value_expression.expr), $set_quantifier.sq, $set_function_name.tree); }
  | keyword_function_name^ (Left_Paren Right_Paren)? 
    { $expr = utils.buildFunctionCall($keyword_function_name.fn, null, null, $keyword_function_name.tree); }
  | known_function_name^ Left_Paren function_call_params? Right_Paren 
    { $expr = utils.buildFunctionCall($known_function_name.fn, $function_call_params.l, null, $known_function_name.tree); }
//  | BINARY^ string_literal
//    { if ($string_literal.expr == null)
//        $expr = bkexpr($BINARY); 
//     else 
//        $expr = utils.buildFunctionCall(utils.buildFunctionName($BINARY,true), Collections.singletonList($string_literal.expr), null, $BINARY);
//    }
  | EXISTS Left_Paren nestable_select_statement Right_Paren { $expr = utils.buildExists($EXISTS,$nestable_select_statement.s, $nestable_select_statement.tree); }
  | group_concat_call { $expr = $group_concat_call.expr; }  
  ;  

group_concat_call returns [ExpressionNode expr] options {k=1;}:
  g=GROUP_CONCAT Left_Paren (d=DISTINCT)? function_call_params
  orderby?
  (SEPARATOR s=Character_String_Literal)? Right_Paren
  { $expr = utils.buildGroupConcat(utils.buildFunctionName($g, false),$function_call_params.l, ($d != null), $s.text); }
  ;
  
// if we ever need the actual types - can turn this into a Type object I guess
cast_to_type returns [Name n] options {k=1;}:
  (stn=(DATE | DATETIME | TIME) { $n = bkn($stn.text); })
  |
  (it=(SIGNED | UNSIGNED) i=INTEGER? { $n = utils.buildCastToIntegralType($it.text,$i.text); })
  | 
  (st=(BINARY | CHAR) 
    ((Left_Paren sa=Unsigned_Integer Right_Paren) | (CHARSET csn=unqualified_identifier))?
    { $n = utils.buildCastToBinaryOrChar($st.text, $sa.text, $csn.n); }
  )
  |
  (DECIMAL (Left_Paren fv=Unsigned_Integer (Comma sv=Unsigned_Integer)? Right_Paren)? { $n = utils.buildCastToSizedType($DECIMAL.text,$fv.text,$sv.text); })
  ;

timestampdiff_constant_value options {k=1;}:
  MICROSECOND | SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR 
  ;

interval_expression returns [ExpressionNode expr] options {k=1;}:
  INTERVAL value_expression date_sub_constant_value
  { $expr = utils.buildInterval($value_expression.expr, $date_sub_constant_value.text); }
  ;

date_sub_constant_value options {k=1;}:
  timestampdiff_constant_value
  | SECOND_MICROSECOND | MINUTE_MICROSECOND | MINUTE_SECOND
  | HOUR_MICROSECOND | HOUR_SECOND | HOUR_MINUTE
  | DAY_MICROSECOND | DAY_SECOND | DAY_MINUTE | DAY_HOUR
  | YEAR_MONTH 
  ;

maybe_function_call returns [ExpressionNode expr] options {k=1;}:
  qualified_identifier^ (Left_Paren (sq=set_quantifier)? (fcp=function_call_params | a=Asterisk)? Right_Paren)?
  { $expr = utils.buildFunctionOrIdentifier($qualified_identifier.n, $sq.sq, $fcp.l, $a,$qualified_identifier.tree, $Left_Paren, $Right_Paren); }
  ;

function_call_params returns [List l] options {k=1;}:
  { $l = new ArrayList(); } 
  lve=value_expression { $l.add($lve.expr); } (Comma fve=value_expression { $l.add($fve.expr); })* 
  ;

//// ( avg | max | min | sum | every | any | some | count | stddev_pop | stddev_samp | var_samp | var_pop | collect | fusion| intersection )  
//set_function_name returns [FunctionName fn] options {k=1;}:
//  f=(AVG | MAX | MIN | SUM)
//  { $fn = utils.buildFunctionName($f, false); }
//  ;

known_function_name returns [FunctionName fn] options {k=1;}:
  f=(SUBSTRING
  | COALESCE
  | DATABASE
  | FOUND_ROWS
  | IF
  | IFNULL
  | LOWER
  | NULLIF
  | RAND
  | CURDATE
  | CURTIME
  | UNIX_TIMESTAMP
  | UTC_TIMESTAMP
  | BIT_AND
  | BIT_OR
  | BIT_XOR
  | STD
  | STDDEV_POP
  | STDDEV_SAMP
  | STDDEV
  | VAR_POP
  | VAR_SAMP
  | VARIANCE
  | LEFT
  | REPLACE
  )
  { $fn = utils.buildFunctionName($f, false); }
  ;

keyword_function_name returns [FunctionName fn] options {k=1;}:
  CURRENT_DATE { $fn = utils.buildFunctionName($CURRENT_DATE, false); }
  | CURRENT_TIMESTAMP { $fn = utils.buildFunctionName($CURRENT_TIMESTAMP, false); }
  | CURRENT_TIME { $fn = utils.buildFunctionName($CURRENT_TIME, false); }
  | CURRENT_USER { $fn = utils.buildFunctionName($CURRENT_USER, false); }
  | NOW { $fn = utils.buildFunctionName($NOW, false); }
  ;
  
case_expression returns [ExpressionNode expr] options {k=1;}:
  CASE^ (t=basic_expr)? simple_when_clause_list (ELSE e=value_expression)? END 
  { $expr = utils.buildCaseExpression($t.expr, $e.expr, $simple_when_clause_list.l, $CASE.tree); }
  ;
  
simple_when_clause_list returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  (swc=simple_when_clause { $l.add($swc.wc); })+;
  
simple_when_clause returns [WhenClause wc] options {k=1;}:
  WHEN^ t=value_expression THEN r=value_expression  
  { $wc = utils.buildWhenClause($t.expr, $r.expr, $WHEN.tree); }
  ;



literal_sans_signed returns [ExpressionNode expr] options {k=1;}:
  non_null_literal { $expr = $non_null_literal.expr; }
  | null_literal { $expr = $null_literal.expr; }
  ;

non_null_literal returns [ExpressionNode expr] options {k=1;}:
  unsigned_numeric_literal { $expr = $unsigned_numeric_literal.expr; }
  | string_literal { $expr = $string_literal.expr; }
  | Bit_String_Literal { $expr = utils.buildLiteral($Bit_String_Literal); }
  | Hex_String_Literal { $expr = utils.buildLiteral($Hex_String_Literal); }
  | boolean_literal { $expr = $boolean_literal.expr; }
//  | day_time_literal { $expr = null; unsupported("day_time_literal"); }
//  | interval_literal { $expr = null; unsupported("interval_literal"); }
  ;

null_literal returns [ExpressionNode expr] options {k=1;}:
  NULL { $expr = utils.buildLiteral($NULL); }
  ;

literal returns [ExpressionNode expr] options {k=1;}:
  literal_sans_signed { $expr = $literal_sans_signed.expr; }
  | signed_numeric_literal { $expr = $signed_numeric_literal.expr; }
  ;

string_literal returns [ExpressionNode expr] options {k=1;}:
    Character_String_Literal { $expr = utils.buildLiteral($Character_String_Literal); }
    | National_Character_String_Literal { $expr = utils.buildLiteral($National_Character_String_Literal); }
    ;

boolean_literal returns [ExpressionNode expr] options {k=1;}:
  TRUE { $expr = utils.buildBooleanLiteral(Boolean.TRUE, $TRUE); } 
  | FALSE { $expr = utils.buildBooleanLiteral(Boolean.FALSE, $FALSE); } 
  | UNKNOWN { $expr = utils.buildBooleanLiteral(null, $UNKNOWN); }
  ;
  
signed_numeric_literal returns [ExpressionNode expr] options {k=1;}: 
  signed_integral_literal { $expr = $signed_integral_literal.expr; } 
  | Signed_Float { $expr = utils.buildLiteral($Signed_Float); }
  ;
unsigned_numeric_literal returns [ExpressionNode expr] options {k=1;}: 
  unsigned_integral_literal { $expr = $unsigned_integral_literal.expr; } 
  | Unsigned_Float { $expr = utils.buildLiteral($Unsigned_Float); }
  ;

unsigned_integral_literal returns [ExpressionNode expr] options {k=1;}: 
  Unsigned_Integer { $expr = utils.buildLiteral($Unsigned_Integer); }
  | Unsigned_Large_Integer { $expr = utils.buildLiteral($Unsigned_Large_Integer); }
  ;
signed_integral_literal returns [ExpressionNode expr] options {k=1;}: 
  Signed_Integer { $expr = utils.buildLiteral($Signed_Integer); }
  | Signed_Large_Integer { $expr = utils.buildLiteral($Signed_Large_Integer); }
  ;

sign options {k=1;}: 
  Plus_Sign | Minus_Sign
  ;

date_literal_fragment options {k=1;}: 
  Unsigned_Integer Minus_Sign Unsigned_Integer Minus_Sign Unsigned_Integer { unsupported("date_literal_fragment"); }
  ;
date_literal options {k=1;}: 
  DATE Quote date_literal_fragment Quote { unsupported("date_literal"); }
  ;

time_literal_fragment options {k=1;}: 
  Unsigned_Integer Colon Unsigned_Integer Colon seconds_value { unsupported("time_literal_fragment"); }
  ;
timezone_literal_fragment options {k=1;}: 
  sign Unsigned_Integer Colon Unsigned_Integer { unsupported("timezone_literal_fragment"); }
  ;
seconds_value options {k=1;}: 
  Unsigned_Integer ( Period ( Unsigned_Integer )? )? { unsupported("seconds_value"); }
  ;

time_literal options {k=1;}: 
  TIME Quote time_literal_fragment timezone_literal_fragment? Quote { unsupported("time_literal"); }
  ;

timestamp_literal options {k=1;}:
  TIMESTAMP Quote date_literal_fragment Space time_literal_fragment timezone_literal_fragment? Quote { unsupported("timestamp_literal"); }
  ;

unqualified_identifier returns [Name n] options {k=1;}: 
  keyword_simple_identifier { $n = $keyword_simple_identifier.n; } 
  ;

simple_unqualified_identifier returns [Name n] options {k=1;}: 
  Regular_Identifier { $n = utils.buildIdentifier($Regular_Identifier); }
  ;

qualified_identifier returns [Name n] options {k=1;}:
  { List parts = new ArrayList(); }
  ksi=keyword_simple_identifier^ { parts.add($ksi.n); } (Period tqi=trailing_qualified_identifier { parts.add($tqi.n); })*
  { $n = utils.buildQualifiedName(parts); } 
  ;

trailing_qualified_identifier returns [Name n] options {k=1;}:
  keyword_simple_identifier { $n = $keyword_simple_identifier.n; }
  | Asterisk { $n = utils.buildName($Asterisk); }
  ;

unqualified_identifier_list returns [List l] options {k=1;}:
  { $l = new ArrayList(); } 
  ui=unqualified_identifier { $l.add($ui.n); } ( Comma fui=unqualified_identifier { $l.add($fui.n); })* 
  ;

qualified_identifier_list returns [List l] options {k=1;}: 
  { $l = new ArrayList(); }
  qi=qualified_identifier { $l.add($qi.n); }( Comma fqi=qualified_identifier { $l.add($fqi.n); } )* 
  ;

keyword_escape_identifier_value2 options {k=1;}:
  TRANSACTION
  ;
  
keyword_escape_identifier_value3 options {k=1;}:
  NAMES | CHARSET | PASSWORD | DVE | GLOBAL | LOCAL | SESSION
  ;

keyword_escape_identifier_value1 options {k=1;}:
  TYPE | COUNT | TIMESTAMP | FIELD_COMMENT | STATIC | DESC | ASC | VARIABLES
  | TABLES | STATUS | ROLLBACK | SITE | COMMENT | USER | COLUMNS | TENANT | EXTERNAL
  | NO | INSTANCE | LENGTH | SECOND | ENGINE | ENGINES | TIME | DATE | YEAR | MONTH | QUARTER | WEEK | CONTAINER
  | RANGE | CHARACTERS | LEVEL | DAYS | SERVICE | REF | BIN | START | ACTION | END | ABS | EXP | HOUR
  | SIGN | ANY | ARE | AT | BOOLEAN | CASCADED | CLOB | CLOSE | COMMIT | DAY | DYNAMIC | EXCEPT
  | INTERSECT | ISOLATION | MERGE | MINUTE | MOD | NCHAR | NCLOB | NONE | OF | OLD | ONLY
  | ROWS | ROW | SCOPE | SOME | BEGIN | AUTO | COLLATION | COMMITTED | CONST | DEFINER | EXTERN | HOURS
  | INCREMENT | INTERSECTION | INVOKER | KIND | LEN | NULLABLE | PARTIAL | PRIOR | PRIVILEGES
  | REPEATABLE | SECURITY | SERIALIZABLE | SETS | SIGNED | SIMPLE | SIZE  | TEMPORARY | UNCOMMITTED 
  | VIEW | OFFSET  | DUPLICATE | FIELDS | TRIGGERS | IDENTIFIED
  | PROCESSLIST | FLUSH | ENUM | ERRORS | WARNINGS | MICROSECOND | INDEXES | PLUGINS | LOGS | GRANTS
  | SLAVE | ENABLE | DISABLE | ALGORITHM | UNDEFINED | TEMPTABLE | CONSISTENT | SNAPSHOT
  | STORAGE | DISTRIBUTE | SERIAL | AVG | MAX | MIN | SUM | VALUES | OPTION | QUERY
  | JDBC | PERSISTENT | RANDOM | BROADCAST | GROUPS
  | SITES | RANGES | GENERATION | GENERATIONS | OPTIONAL | REQUIRED | STRICT | TEMPLATE | TEMPLATES | RELOAD | TEMP
  | TENANTS | SUSPEND | RESUME | MULTITENANT | PROVIDER | PROVIDERS | POLICY | POLICIES 
  | STANDARD | ADAPTIVE | RELAXED | INSTANCES | SERVICES | STOP | SINGLE | MASTERMASTER
  | MASTER | CONTAINERS | CONTAINER_TENANT | CONTAINER_TENANTS | DISCRIMINATE | RAW
  | LOGGING | DATA | INFILE | LINES | TERMINATED | ENCLOSED | ESCAPED | STARTING | FUNCTION
  | HASH | BTREE | RTREE | MEMORY | ROW_FORMAT | FIRST | AFTER | UUID | LAST_INSERT_ID | POOL
  | DATETIME | XML | SERVER | SERVERS | EVENTS | PLAN | PLANS | PARTITIONS | OR | ENABLED 
  | MODE | PREPARE | EXECUTE | DEALLOCATE | STATISTICS | CACHE | RECOVER | ONE | XA | CARDINALITY
  | PHASE | VARIABLE
  
// The keywords below do not work in DVE, however it's not just a simple case of making them identifiers,
// so they are listed here for reference
//  | FULL | CATALOG | SAVEPOINT | RAND | TRUNCATE | CAST | CURDATE | CURTIME | ESCAPE | UNKNOWN
//  | COALESCE | LOWER | NULLIF | STDDEV_POP | STDDEV_SAMP | SUBSTRING
//  | VAR_POP | VAR_SAMP | TRUNCATE | RAND| TIMESTAMPDIFF | DATE_SUB | UNIX_TIMESTAMP | NOW
//  | DATE_ADD | FOUND_ROWS | SQL_BUFFER_RESULT | SQL_CACHE | SQL_NO_CACHE | SQL_NO_FCACHE | STD
//  | STDDEV | VARIANCE | GROUP_CONCAT | BIT_AND | BIT_OR | BIT_XOR | LOAD
  ;  

// a simple identifier is all identifiers (no exclusions)
keyword_simple_identifier returns [Name n] options {k=1;}:
    keyword_escape_identifier_value1 { $n = utils.buildIdentifier($keyword_escape_identifier_value1.text, $keyword_escape_identifier_value1.tree); }
  | keyword_escape_identifier_value2 { $n = utils.buildIdentifier($keyword_escape_identifier_value2.text, $keyword_escape_identifier_value2.tree); }
  | keyword_escape_identifier_value3 { $n = utils.buildIdentifier($keyword_escape_identifier_value3.text, $keyword_escape_identifier_value3.tree); }
  | Regular_Identifier { $n = utils.buildIdentifier($Regular_Identifier); }
  ;

// simple identifier1 is all identifiers except transaction
keyword_simple_identifier1 returns [Name n] options {k=1;}:
    keyword_escape_identifier_value1 { $n = utils.buildIdentifier($keyword_escape_identifier_value1.text, $keyword_escape_identifier_value1.tree); }
  | keyword_escape_identifier_value3 { $n = utils.buildIdentifier($keyword_escape_identifier_value3.text, $keyword_escape_identifier_value3.tree); }
  | Regular_Identifier { $n = utils.buildIdentifier($Regular_Identifier); }
  ;

// simple identifier2 is all identifiers except transaction, charset, password, names, dve, global, session, local
keyword_simple_identifier2 returns [Name n] options {k=1;}:
    keyword_escape_identifier_value1 { $n = utils.buildIdentifier($keyword_escape_identifier_value1.text, $keyword_escape_identifier_value1.tree); }
  | Regular_Identifier { $n = utils.buildIdentifier($Regular_Identifier); }
  ;
 
collation_identifier returns [Name n] options {k=1;}:
  unqualified_identifier { $n = $unqualified_identifier.n; }
  | Character_String_Literal { $n = utils.buildNameFromStringLiteral($Character_String_Literal.tree); }
  ;
  
push_scope:
  { utils.pushScope(); }
  ;
  
push_unresolving_scope:
  { utils.pushUnresolvingScope(); }
  ;

