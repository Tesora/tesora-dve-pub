parser grammar PE;

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
    tokenVocab=sql2003Lexer;
    // TODO explore removing this option
    output=AST;
    k=3;
}

import MySQL;

tokens {
  NOTIN;
  NOTIS;
  NOTLIKE;
  NOTBETWEEN;
}


@header {
package com.tesora.dve.sql.parser;

import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

import com.tesora.dve.common.catalog.*;
import com.tesora.dve.server.connectionmanager.UserXid;
import com.tesora.dve.sql.expression.*;
import com.tesora.dve.sql.node.*;
import com.tesora.dve.sql.node.structural.*;
import com.tesora.dve.sql.node.expression.*;
import com.tesora.dve.sql.node.expression.IndexHint.*;
import com.tesora.dve.sql.util.*;
import com.tesora.dve.sql.parser.Utils.PrecedenceCollector;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.*;
import com.tesora.dve.sql.schema.types.*;
import com.tesora.dve.sql.schema.modifiers.*;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.MysqlSelectOption;
import com.tesora.dve.sql.statement.session.*;
import com.tesora.dve.worker.*;
}




@members {

  public TranslatorUtils utils;

  public void setUtils(TranslatorUtils u) { 
    utils = u;
    gMySQL.setUtils(u);
  }

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

sql_statements returns [List stmts] options {k=1;}
  @init { List buf = new ArrayList(); }
  :
  (
    (explainable_executable_statement { buf.add($explainable_executable_statement.s); } Semicolon? empty_statement)
    |((le=executable_statement { buf.add($le.s); })? (Semicolon (te=executable_statement { buf.add($te.s); })?)* empty_statement)
  )
  { $stmts = buf; }
  ;

// this arranges for explain to work for everything EXCEPT an empty statement
explainable_executable_statement returns [Statement s] options {k=1;}:
  EXPLAIN config_options? sql_data_statement { $s = utils.setExplainFlag($sql_data_statement.s,$config_options.l); }
  ;

executable_statement returns [Statement s] options {k=1;}:
  sql_schema_statement { $s = $sql_schema_statement.s; }
  | sql_data_statement { $s = $sql_data_statement.s; } 
  | sql_session_statement { $s = $sql_session_statement.s; }
  | start_statement { $s = $start_statement.s; }
  | stop_statement { $s = $stop_statement.s; }
  | kill_statement { $s = $kill_statement.s; }
  ;

sql_schema_statement returns [Statement s] options {k=1;}:
  sql_schema_definition_statement { $s = $sql_schema_definition_statement.s; }
  | sql_schema_destruction_statement { $s = $sql_schema_destruction_statement.s; }
  | sql_schema_alter_statement { $s = $sql_schema_alter_statement.s; }
  | sql_schema_query_statement { $s = $sql_schema_query_statement.s; }
  | sql_schema_maintenance_statement { $s = $sql_schema_maintenance_statement.s; }
  | sql_rename_statement { $s = $sql_rename_statement.s; }
  ;
  
sql_rename_statement returns [Statement s] options {k=1;}:
  RENAME ddl
  ((TABLE rename_table_name_list
  { $s = utils.buildRenameTableStatement($rename_table_name_list.l); })
  | ((DATABASE | SCHEMA) source = qualified_identifier TO target = qualified_identifier
  { $s = utils.buildRenameDatabaseStatement(new Pair($source.n, $target.n)); }))
;

rename_table_name_list returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  first = rename_table_name_pair { $l.add($first.p); } (Comma next = rename_table_name_pair { $l.add($next.p); })*
;

rename_table_name_pair returns [Pair p] options {k=1;}:
  source = qualified_identifier TO target = qualified_identifier
  { $p = new Pair($source.n, $target.n); }
;

sql_schema_definition_statement returns [Statement s] options {k=1;}:
  CREATE ddl creatable_target { $s = $creatable_target.s; }
  | GRANT ddl grantable_target { $s = $grantable_target.s; }
  ;

creatable_target returns [Statement s] options {k=1;}:
  table_definition { $s = $table_definition.s; }
  | USER user_spec_list { $s = utils.buildCreateUserStatement($user_spec_list.l); }
  | pe_definition_target { $s = $pe_definition_target.s; }
  | (u=UNIQUE | f=FULLTEXT)? INDEX indname=unqualified_identifier key_index_option? ON altered_table key_list all_key_option_list
  { $s = utils.buildAlterTableStatement($altered_table.tk,utils.buildAddIndexAction(
      utils.buildKey(($f != null ? IndexType.FULLTEXT : $key_index_option.it), $indname.n, $key_list.l, $all_key_option_list.l)
      )); utils.popScope();}
  | view_definition { $s = $view_definition.s; }
  ;

grantable_target returns [Statement s] options {k=1;}:
  (ALL PRIVILEGES? ON grant_scope TO user_spec { $s = utils.buildGrant($grant_scope.gs, $user_spec.us, $ALL.text); })
  | (PROCESS ON grant_scope TO user_spec { $s = utils.buildEmptyStatement("grant process"); } )
  ;

grant_scope returns [GrantScope gs] options {k=1;}:
  (Asterisk | unqualified_identifier) Period Asterisk
  { if ($unqualified_identifier.n == null) $gs = utils.buildGrantScope(null); 
    else $gs = utils.buildGrantScope($unqualified_identifier.n);
  }
  ;

sql_schema_destruction_statement returns [Statement s] options {k=1;}:
  DROP ddl droppable_target { $s = $droppable_target.s; }
  ;

droppable_target returns [Statement s] options {k=1;}:
  drop_statement { $s = $drop_statement.s; }
  | pe_drop_target { $s = $pe_drop_target.s; }
  ;  

sql_schema_alter_statement returns [Statement s] options {k=1;}:
  ALTER ddl alterable_target { $s = $alterable_target.s; }
  | (d=SUSPEND | r=RESUME) ddl TENANT ui=unqualified_identifier 
  { $s = ($d == null) ? utils.buildResumeTenantStatement($ui.n) : utils.buildSuspendTenantStatement($ui.n); }  
  ;

alterable_target returns [Statement s] options {k=1;}:
  pe_alter_target { $s = $pe_alter_target.s; }
  | database_tag dbn=unqualified_identifier (
    (DEFAULT? (ch=create_db_charset_expr? co=create_db_collate_expr?) { $s = utils.buildAlterDatabaseStatement($dbn.n, $ch.n, $co.n); })
    | (templ=template_declaration_kern { $s = utils.buildAlterDatabaseStatement($dbn.n, $templ.p); })
  )
  | (TABLE altered_table { ArrayList acts = new ArrayList(); Name tableName = $altered_table.tk.getTable().getName(); } 
      ((lata=alter_table_action[tableName] { acts.addAll($lata.l); }) (Comma tata=alter_table_action[tableName] { acts.addAll($tata.l); })*)
      { $s = utils.buildAlterTableStatement($altered_table.tk, acts); utils.popScope(); })       
  ;
  
altered_table returns [TableKey tk] options {k=1;}:
  qualified_identifier { $tk = utils.lookupAlteredTable($qualified_identifier.n); }
  ;

alter_table_action [Name tableName] returns [List l] options {k=1;}:
  (RENAME TO? ntn=qualified_identifier { $l = utils.buildRenameTableAction($ntn.n); })
  | (CONVERT TO
    ((charset_expr_tag cs=charset_type) (COLLATE cn=collate_type)?
    ) { $l = utils.buildTableConvertToAction($cs.n, $cn.n); })
  | (CHANGE COLUMN? ccn=unqualified_identifier cfc=field_specification add_col_first_or_after_spec? { $l = utils.buildChangeColumnAction($ccn.n, $cfc.l, $add_col_first_or_after_spec.p); })
  | (ALTER COLUMN? acn=unqualified_identifier table_alter_target_change_column { $l = utils.buildAlterColumnAction($acn.n,$table_alter_target_change_column.expr); })
  | (ADD (
       (koid=key_definition { $l = utils.buildAddIndexAction($koid.tc); })
       | (COLUMN? 
          ((afc=field_specification add_col_first_or_after_spec? { $l = utils.buildAddColumnAction($afc.l, $add_col_first_or_after_spec.p); })
           |(add_multi_column_spec { $l = utils.buildAddColumnAction($add_multi_column_spec.l); })))
       ))
  | (DROP drop_target_action { $l = $drop_target_action.l; }) 
  | (DISABLE KEYS { $l = utils.buildDisableKeysAction(); })
  | (ENABLE KEYS { $l = utils.buildEnableKeysAction(); })
  | (mysql_table_option[tableName] { $l = utils.buildTableOptionAction($mysql_table_option.t); })
  | (MODIFY COLUMN? mfs=field_specification add_col_first_or_after_spec? { $l = utils.buildModifyColumnAction($mfs.l, $add_col_first_or_after_spec.p); })
  | distribution_declaration_target { $l = utils.buildModifyDistributionAction($distribution_declaration_target.dv); }
  ;  

add_col_first_or_after_spec returns [Pair p] options {k=1;}:
  (FIRST { $p = new Pair($FIRST.text, null); })
  | (AFTER unqualified_identifier { $p = new Pair($AFTER.text, $unqualified_identifier.n); })
  ;

add_multi_column_spec returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  Left_Paren lfs=field_specification { $l.addAll($lfs.l); } (Comma tfs=field_specification { $l.addAll($tfs.l); })* Right_Paren
  ;

drop_target_action returns [List l] options {k=1;}:
  ((in=(INDEX | KEY) | (f=FOREIGN KEY) | (c=COLUMN))? dt=unqualified_identifier
  { if ($in.text == null && $f.text == null) $l = utils.buildDropColumnAction($dt.n);
    else if ($f.text != null) $l = utils.buildDropIndexAction(ConstraintType.FOREIGN,$dt.n);
    else $l = utils.buildDropIndexAction(null,$dt.n);
  })
  | (PRIMARY KEY 
    { $l = utils.buildDropIndexAction(ConstraintType.PRIMARY,utils.buildName("PRIMARY")); }
  )
  ;
            

sql_schema_query_statement returns [Statement s] options {k=1;}:
  sql_schema_show_statement { $s = $sql_schema_show_statement.s; }
  | sql_schema_describe_statement { $s = $sql_schema_describe_statement.s; }
  ;

// table <if not exists> name
// [1] (fields & keys) <table_opts> select_statement
// [2] | like name
// [3] | (like name)
// [4] | () <table_opts> select_statement
// [5] | <table_opts> select statement

table_definition returns [Statement s] options {k=1;}:
  (temptab=TEMPORARY { utils.notddl(); })? TABLE push_scope if_not_exists tn=qualified_identifier { Name tableName = $tn.n; }
  (
    // [2]
    ( LIKE npon=qualified_identifier 
      { $s = utils.buildCreateTable($tn.n, $npon.n, $if_not_exists.b); utils.popScope(); } )
    |
    // [5]
    ( (cta_dto=db_table_options[tableName])? partition?
      (PERSISTENT GROUP ctasgn=unqualified_identifier)?
      ((ctadist=distribution_declaration) | (ctacont=container_discriminator))?
      AS? (ctasel=select_statement)
      { $s = utils.buildCreateTable($tn.n, Collections.EMPTY_LIST, $ctadist.dv, $ctasgn.n, $cta_dto.l,
                                $if_not_exists.b, $ctacont.p, $ctasel.s, $temptab != null); utils.popScope(); }
     )
     |
     ( Left_Paren
        (
          // [3]
          ( LIKE pon=qualified_identifier Right_Paren 
            { $s = utils.buildCreateTable($tn.n, $pon.n, $if_not_exists.b); utils.popScope(); } )
          |
          // [1] & [4]
          ( table_define_fields? Right_Paren (nt_dto=db_table_options[tableName])? partition? 
            (PERSISTENT GROUP sgn=unqualified_identifier)?
            ((ntdist=distribution_declaration) | (ntcont=container_discriminator))?
            (AS? ntsel=select_statement)?
            { $s = utils.buildCreateTable($tn.n, $table_define_fields.l, $ntdist.dv, $sgn.n, $nt_dto.l, 
                                    $if_not_exists.b, $ntcont.p, $ntsel.s, $temptab != null); utils.popScope(); }
           )
        )
      )
   )
   ;

container_discriminator returns [Pair p] options {k=1;}:
  DISCRIMINATE distribution_columns USING CONTAINER unqualified_identifier
  { $p = new Pair($unqualified_identifier.n, $distribution_columns.l); }
  ;

table_define_fields returns [List l] options {k=1;}: 
  { $l = new ArrayList(); }
  ltf=table_define_field { $l.addAll($ltf.l); } (Comma ttf=table_define_field { $l.addAll($ttf.l); })* 
  ;

table_define_field returns [List l] options {k=1;}
    @init { $l = new ArrayList(); }
    :
    column_definition { $l.addAll($column_definition.l); } 
    | key_definition { $l.add($key_definition.tc); }
    ;

key_definition returns [PEKey tc] options {k=1;}:
  (fulltext_key_def { $tc = $fulltext_key_def.tc; })
  | ((INDEX | KEY) regular_key_def { $tc = $regular_key_def.tc; })
  | (constraint_definition { $tc = $constraint_definition.tc; })
  ;

constraint_definition returns [PEKey tc] options {k=1;}:
  (CONSTRAINT cn=keyword_simple_identifier1)?
  anonymous_constraint_definition
  { $tc = $anonymous_constraint_definition.tc;
    if ($cn.n != null)
      $tc.setSymbol($cn.n); 
  }
  ;
  
anonymous_constraint_definition returns [PEKey tc] options {k=1;}:
  ((PRIMARY KEY (io=key_index_option)? kl=key_list akl=all_key_option_list)
   { $tc = utils.withConstraint(ConstraintType.PRIMARY, utils.buildName("PRIMARY"), utils.buildKey($io.it, utils.buildName("PRIMARY"), $kl.l, $akl.l)); })
  |((UNIQUE (INDEX | KEY)? regular_key_def)
    { $tc = utils.withConstraint(ConstraintType.UNIQUE, null, $regular_key_def.tc); })
  |(foreign_key_def { $tc = utils.withConstraint(ConstraintType.FOREIGN, null, $foreign_key_def.tc); }) 
  ;

fulltext_key_def returns [PEKey tc] options {k=1;}:
  FULLTEXT (KEY | INDEX)? (n=unqualified_identifier)? kl=key_list akl=all_key_option_list
  { $tc = utils.buildKey(IndexType.FULLTEXT, $n.n, $kl.l, $akl.l); }
  ;

regular_key_def returns [PEKey tc] options {k=1;}:
  (n=unqualified_identifier)? (io=key_index_option)? kl=key_list akl=all_key_option_list
  { $tc = utils.buildKey($io.it, $n.n, $kl.l, $akl.l); }
  ;

foreign_key_def returns [PEKey tc] options {k=1;}:
  FOREIGN KEY (fkin=unqualified_identifier)? fkl=key_list
  REFERENCES ttn=qualified_identifier Left_Paren (tfkl=unqualified_identifier_list) Right_Paren
  (ON ((DELETE dra=fk_ref_option) | (UPDATE ura=fk_ref_option)))*
  { $tc = utils.buildForeignKey($fkin.n,$fkl.l,$ttn.n,$tfkl.l,$dra.fka,$ura.fka);}
  ;


fk_ref_option returns [ForeignKeyAction fka] options {k=1;}:
  RESTRICT { $fka = ForeignKeyAction.RESTRICT; }
  | CASCADE { $fka = ForeignKeyAction.CASCADE; }
  | SET NULL { $fka = ForeignKeyAction.SET_NULL; }
  | NO ACTION { $fka = ForeignKeyAction.NO_ACTION; }
  ;

column_definition returns [List l] options {k=1;}:
   ((field_specification { $l = $field_specification.l; })
   | (enum_field_specification { $l = $enum_field_specification.l; }))
   // opt_check_constraint | references
// field_specification opt_check_constraint?
// | field_specification references
   ;

field_specification returns [List l] options {k=1;}:
  { ArrayList modifiers = new ArrayList(); }
   unqualified_identifier type_description (field_attribute { modifiers.add($field_attribute.cm); })* comment?
   { $l = utils.buildFieldDefinition($unqualified_identifier.n, $type_description.type, modifiers, $comment.str); }
   ;
   
enum_field_specification returns [List l] options {k=1;}:
  { ArrayList modifiers = new ArrayList(); }
	unqualified_identifier enum_type_description (enum_field_attribute[$enum_type_description.type] { modifiers.add($enum_field_attribute.cm); })* comment?
   { $l = utils.buildFieldDefinition($unqualified_identifier.n, $enum_type_description.type, modifiers, $comment.str); }
   ;
   
enum_field_attribute [Type type] returns [ColumnModifier cm] options {k=1;}: 
  ((field_attribute_no_default { $cm = $field_attribute_no_default.cm; } )
  | (DEFAULT enum_default_spec { $cm = utils.buildEnumDefaultValue($type, $enum_default_spec.expr); }))
  ;
  
enum_default_spec returns [ExpressionNode expr]  options {k=1;}:
  char_or_hex_string_literal { $expr = $char_or_hex_string_literal.expr; }
  | null_literal { $expr = $null_literal.expr; }
  ; 

type_description returns [Type type] options {k=1;}:
  (type_name field_width? field_modifiers
  { $type = utils.buildType($type_name.l,$field_width.ts, $field_modifiers.l); })
  ;
  
any_type_description returns [Type type] options {k=1;}:
  (type_description { $type = $type_description.type; }) | (enum_type_description { $type = $enum_type_description.type; })
  ;
  
enum_type_description returns [Type type] options {k=1;}:
  (((e=ENUM) |(s=SET)) { List evals = new ArrayList(); } Left_Paren (lev=char_or_hex_string_literal { evals.add($lev.expr); }) 
     (Comma (tev=char_or_hex_string_literal { evals.add($tev.expr); }))* Right_Paren field_modifiers
     { $type = utils.buildEnum(($e == null),evals,$field_modifiers.l); } 
  	)
  ;
  
char_or_hex_string_literal returns [ExpressionNode expr] options {k=1;}:
  v=(Character_String_Literal | Unsigned_Integer | Hex_String_Literal) { $expr = utils.buildLiteral($v); }
  ;
  
field_modifiers returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  (field_modifier { $l.add($field_modifier.tm); })*
  ; 

// todo: maybe move types into MySQL
type_name returns [List l]
  options {k=1;}
  @init{ $l = new ArrayList(); }
  :
  type_value_keyword { $l.add($type_value_keyword.n); } 
  | LONG type_value_keyword { $l.add(bkn($LONG)); $l.add($type_value_keyword.n); } 
  | DOUBLE PRECISION? { $l.add(bkn($DOUBLE)); if ($PRECISION != null) $l.add(bkn($PRECISION)); }
  | CHARACTER VARYING? { $l.add(bkn($CHARACTER)); if ($VARYING != null) $l.add(bkn($VARYING)); }
  ;

type_value_keyword returns [Name n] options {k=1;}:
  builtin_type_value_keyword { $n = bkn($builtin_type_value_keyword.tree); }
  | simple_unqualified_identifier { $n = $simple_unqualified_identifier.n; }
  ;

builtin_type_value_keyword options {k=1;}:
  VARCHAR | PRECISION | SMALLINT | BLOB | CHAR
  | NUMERIC | DECIMAL | DEC | FIXED | INTEGER | INT | FLOAT
  | REAL | DATE | TIME | DATETIME | TIMESTAMP | NCHAR | YEAR
  | BINARY | UNSIGNED | SERIAL | BOOLEAN
  ;  

field_width returns [SizeTypeAttribute ts] options {k=1;}:
  Left_Paren a=unsigned_integral_literal (Comma b=unsigned_integral_literal)? Right_Paren 
  { $ts = utils.buildSizeTypeAttribute($a.expr,$b.expr); }
  ;

field_modifier returns [TypeModifier tm] options {k=1;}:
//  simple_unqualified_identifier { $cm = utils.buildColumnModifier($simple_unqualified_identifier.n); }
//  | 
  UNSIGNED { $tm = utils.buildTypeModifier(TypeModifierKind.UNSIGNED); }
  | SIGNED { $tm = utils.buildTypeModifier(TypeModifierKind.SIGNED); }
  | ZEROFILL { $tm = utils.buildTypeModifier(TypeModifierKind.ZEROFILL); }
  | USING Character_String_Literal { $tm = utils.buildComparisonModifier($Character_String_Literal.text); }
  | COLLATE unqualified_identifier { $tm = utils.buildCollationSpec($unqualified_identifier.n); }
  | ((CHARACTER SET) | CHARSET) unqualified_identifier { $tm = utils.buildCharsetSpec($unqualified_identifier.n); } 
  | BINARY { $tm = utils.buildTypeModifier(TypeModifierKind.BINARY); }
  ;

convert_type_description returns [Type type] options {k=1;}:
  (cst=(BINARY | CHAR | DECIMAL) field_width? { $type = utils.buildType(Collections.singletonList(bkn($cst)),$field_width.ts,null); })
  | (cust=(DATE | DATETIME | TIME) { $type = utils.buildType(Collections.singletonList(bkn($cust)), null, null); })
  | ((s=SIGNED | UNSIGNED) INTEGER?  
    { ArrayList l = new ArrayList();
      l.add($s == null ? utils.buildTypeModifier(TypeModifierKind.UNSIGNED) : utils.buildTypeModifier(TypeModifierKind.SIGNED));
      $type = utils.buildType(Collections.singletonList(utils.buildName("INTEGER")),null, l); })
  ; 

field_attribute returns [ColumnModifier cm] options {k=1;}: 
  (field_attribute_no_default { $cm = $field_attribute_no_default.cm; } )
  | (field_default_value { $cm = utils.buildDefaultValue($field_default_value.expr); })
  ;
  
field_attribute_no_default returns [ColumnModifier cm] options {k=1;}: 
  ((n=NOT? NULL) { $cm = utils.buildColumnModifier($n != null ? ColumnModifierKind.NOT_NULLABLE : ColumnModifierKind.NULLABLE); }) 
  | (AUTOINCREMENT { $cm = utils.buildColumnModifier(ColumnModifierKind.AUTOINCREMENT); })
  | (ON UPDATE CURRENT_TIMESTAMP { $cm = utils.buildOnUpdate(); })
  | (PRIMARY KEY { $cm = utils.buildInlineKeyModifier(ConstraintType.PRIMARY); })  
  | (UNIQUE { $cm = utils.buildInlineKeyModifier(ConstraintType.UNIQUE); })
  | (KEY { $cm = utils.buildInlineKeyModifier(null); })
  ;

field_default_value returns [ExpressionNode expr] options {k=1;}:
   DEFAULT ( literal { $expr = $literal.expr; }
    | CURRENT_TIMESTAMP { $expr = utils.buildIdentifierLiteral("CURRENT_TIMESTAMP"); }
    )
    ;

db_table_options [Name tableName] returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  lto=mysql_table_option[tableName] { $l.add($lto.t); } (Comma? tto=mysql_table_option[tableName] { $l.add($tto.t);} )* 
  ;

mysql_table_option [Name tableName] returns [TableModifier t] options {k=1;}:
  engine_modifier { $t = $engine_modifier.t; }
  | (FIELD_COMMENT Equals_Operator? Character_String_Literal { $t = utils.buildCommentTableModifier($tableName, $Character_String_Literal.text); })
  | (DEFAULT? 
    ((COLLATE Equals_Operator? cn=unqualified_identifier { $t = utils.buildCollationTableModifier($cn.n); })
    |(charset_expr_tag (cseq=Equals_Operator)? charset_type { $t = utils.buildCharsetTableModifier($charset_type.n); })
    ))
  | (AUTOINCREMENT Equals_Operator ai=unsigned_integral_literal { $t = utils.buildAutoincTableModifier($ai.expr); })
  | (ROW_FORMAT Equals_Operator? fn=unqualified_identifier { $t = utils.buildRowFormatTableModifier($fn.n); })
  | (MAX_ROWS Equals_Operator? mr=unsigned_integral_literal { $t = utils.buildMaxRowsModifier($mr.expr); })
  | (CHECKSUM Equals_Operator? cs=unsigned_integral_literal { $t = utils.buildChecksumModifier($cs.expr); })
  | (DELAY_KEY_WRITE Equals_Operator? dkw=unsigned_integral_literal { $t = utils.buildDelayKeyWriteTableModifier($dkw.expr); })
  ;

engine_modifier returns [TableModifier t] options {k=1;}:
  ENGINE Equals_Operator? unqualified_identifier
  { $t = utils.buildEngineTableModifier($unqualified_identifier.n); }
  ;

partition options {k=1;}:
  { utils.pushUnresolving(); }
  // add more later, right now we only do partition by range
  PARTITION BY partition_decl
  { utils.popUnresolving(); }
  ;

partition_decl options {k=1;}:
  partition_range_decl
  | partition_list_decl
  | partition_key_decl
  | partition_hash_decl
  ;
  

partition_range_decl options {k=1;}:
  RANGE COLUMNS? Left_Paren value_expression (Comma value_expression)* Right_Paren
  Left_Paren range_partition_decl (Comma range_partition_decl)* Right_Paren
  ;

// there is still one case we don't support - multiple values
// hopefully we won't see that for a while
range_partition_decl options {k=1;}:
  PARTITION unqualified_identifier VALUES LESS THAN Left_Paren? range_partition_value (Comma range_partition_value)* Right_Paren? 
  ; 

range_partition_value options {k=1;}:
  (literal | function_call_or_identifier | MAXVALUE)
  ;

partition_list_decl options {k=1;}:
  LIST COLUMNS? Left_Paren value_expression Right_Paren
  Left_Paren list_partition_decl (Comma list_partition_decl)* Right_Paren
  ;
  
list_partition_decl options {k=1;}:
  PARTITION unqualified_identifier VALUES IN Left_Paren value_expression (Comma value_expression)* Right_Paren
  ;

partition_key_decl options {k=1;}:
  LINEAR? KEY Left_Paren unqualified_identifier? Right_Paren (PARTITIONS Unsigned_Integer)?
  ;
  
partition_hash_decl options {k=1;}:
  HASH value_expression (PARTITIONS Unsigned_Integer)?
  ;

// range_partition_def options {k=1;}:
//  PARTITION unqualified_identifier VALUES LESS THAN
//  (value_expression | (Left_Paren? MAXVALUE Right_Paren?)) 
//  (STORAGE? engine_modifier) 
//  ;
  
key_index_impl returns [IndexType it] options {k=1;}: 
  (BTREE { $it = IndexType.BTREE; })
  | (RTREE { $it = IndexType.RTREE; })
  | (HASH { $it = IndexType.HASH; })
  ;
  
key_index_option returns [IndexType it] options {k=1;}:
  USING key_index_impl { $it = $key_index_impl.it; }
  ;

// dve extension - specify the cardinality 
key_cardinality returns [ExpressionNode expr] options {k=1;}:
  CARDINALITY unsigned_integral_literal
  { $expr = $unsigned_integral_literal.expr; }
  ;
  
all_key_option_list returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  (ako=all_key_option { $l.add($ako.o); })*
  ;

all_key_option returns [Object o] options {k=1;}: 
  key_index_option { $o = $key_index_option.it; }
  | comment { $o = $comment.str; }
  ;

key_list returns [List l] options {k=1;}:  
  { $l = new ArrayList(); }
  Left_Paren lkps=key_part_spec { $l.add($lkps.c); }(Comma tkps=key_part_spec { $l.add($tkps.c); })* Right_Paren
  ;
  
key_part_spec returns [PEKeyColumnBase c] options {k=1;}:  
  unqualified_identifier (Left_Paren unsigned_integral_literal Right_Paren)? (ASC | DESC)? key_cardinality?
  { $c = utils.buildPEKeyColumn($unqualified_identifier.n, $unsigned_integral_literal.expr, $key_cardinality.expr); }
  ;

view_definition returns [Statement s] options {k=1;}:
  (OR (r=REPLACE))? algorithm_clause? definer_clause? security_clause? 
  VIEW qualified_identifier (Left_Paren unqualified_identifier_list Right_Paren)? AS select_statement with_clause?
  (TABLE push_scope Left_Paren table_define_fields Right_Paren)?
  { $s = utils.buildCreateViewStatement($qualified_identifier.n, $select_statement.s, 
       $definer_clause.us, $unqualified_identifier_list.l, 
       $r != null, $algorithm_clause.a, $security_clause.s, $with_clause.s, $table_define_fields.l); 
       if ($table_define_fields.l != null) utils.popScope(); }
  ;
  
algorithm_clause returns [String a] options {k=1;}:
  ALGORITHM Equals_Operator algorithm_type { $a = $algorithm_type.t; }
  ;
  
algorithm_type returns [String t] options {k=1;}:
  UNDEFINED { $t = "UNDEFINED"; }
  | MERGE { $t = "MERGE"; }
  | TEMPTABLE { $t = "TEMPTABLE"; }
  ;

definer_clause returns [UserScope us] options {k=1;}:
  DEFINER Equals_Operator ((userid { $us = $userid.us; }) | (CURRENT_USER { $us = utils.findCurrentUser(); }))
  ; 

security_clause returns [String s] options {k=1;}:
  SQL SECURITY ((DEFINER { $s = "DEFINER"; }) | (INVOKER { $s = "INVOKER"; }))
  ;

with_clause returns [String s] options {k=1;}:
  WITH (c=CASCADED | l=LOCAL)? CHECK OPTION
  { if ($c == null && $l == null) $s = "UNSPECIFIED";
    else if ($c != null) $s = "CASCADED";
    else $s = "LOCAL";
  }
  ;

sql_session_statement returns [Statement s] options {k=1;}:
  sql_transaction_statement { $s = $sql_transaction_statement.s; }
  | sql_use_statement { $s = $sql_use_statement.s; }
  | sql_set_statement { $s = $sql_set_statement.s; }
  | sql_pe_session_statement { $s = $sql_pe_session_statement.s; }
  | sql_lock_statement { $s = $sql_lock_statement.s; }
  | FLUSH PRIVILEGES { $s = utils.buildFlushPrivileges(); }
  | sql_text_pstmt_prepare_statement { $s = $sql_text_pstmt_prepare_statement.s; }
  | sql_text_pstmt_execute_statement { $s = $sql_text_pstmt_execute_statement.s; }
  | sql_text_pstmt_deallocate_statement { $s = $sql_text_pstmt_deallocate_statement.s; }
  ;

sql_transaction_statement returns [Statement s] options {k=1;}:
  COMMIT WORK? { $s = utils.buildCommitTransactionStatement(); }
  | BEGIN WORK? { $s = utils.buildStartTransactionStatement(false); }
  | ROLLBACK WORK? (TO SAVEPOINT? unqualified_identifier)? { $s = utils.buildRollbackTransactionStatement($unqualified_identifier.n); }
  | SAVEPOINT unqualified_identifier { $s = utils.buildSavepointTransactionStatement($unqualified_identifier.n); }
  | RELEASE SAVEPOINT unqualified_identifier { $s = utils.buildReleaseSavepointTransactionStatement($unqualified_identifier.n); }
  | sql_xa_transaction_statement { $s = $sql_xa_transaction_statement.s; }
  ;

sql_xa_transaction_statement returns [Statement s] options {k=1;}:
  XA 
  ( (xa_start { $s = $xa_start.s; })
  | (xa_end { $s = $xa_end.s; }) 
  | (xa_prepare { $s = $xa_prepare.s; })
  | (xa_commit { $s = $xa_commit.s; })
  | (xa_rollback { $s = $xa_rollback.s; })
  | (xa_recover { $s = $xa_recover.s; })
  )
  ;

xa_start returns [Statement s] options {k=1;}:
  (START | BEGIN) xa_id (j=JOIN | r=RESUME)?
  { $s = utils.buildXAStart($xa_id.id, $j, $r); }
  ;

xa_end returns [Statement s] options {k=1;}:
  END xa_id (sus=SUSPEND (FOR m=MIGRATE)?)?
  { $s = utils.buildXAEnd($xa_id.id, $sus, $m); }
  ;

xa_prepare returns [Statement s] options {k=1;}:
  PREPARE xa_id
  { $s = utils.buildXAPrepare($xa_id.id); }
  ;
  
xa_commit returns [Statement s] options {k=1;}:
  COMMIT xa_id (ONE p=PHASE)?
  { $s = utils.buildXACommit($xa_id.id, $p); }
  ;
  
xa_rollback returns [Statement s] options {k=1;}:
  ROLLBACK xa_id
  { $s = utils.buildXARollback($xa_id.id); }
  ;

xa_recover returns [Statement s] options {k=1;}:
  RECOVER
  { $s = utils.buildXARecover(); }
  ;
  
  
xa_id returns [UserXid id] options {k=1;}:
  gtrid=xa_id_str (Comma bqual=xa_id_str (Comma format=xa_id_str)?)?
  { $id = utils.buildXAXid($gtrid.s, $bqual.s, $format.s); }
  ;

xa_id_str returns [String s] options {k=1;}:
  v=(Unsigned_Integer | Character_String_Literal | Bit_String_Literal | Hex_String_Literal)
  { $s = $v.text; }
  ;

start_statement returns [Statement s] options {k=1;}:
  START ((TRANSACTION (WITH c=CONSISTENT SNAPSHOT)? { $s = utils.buildStartTransactionStatement($c != null); } )
        |(EXTERNAL SERVICE unqualified_identifier { $s = utils.buildStartExternalServiceStatement($unqualified_identifier.n); }))
  ;

stop_statement returns [Statement s] options {k=1;}:
  STOP EXTERNAL SERVICE unqualified_identifier { $s = utils.buildStopExternalServiceStatement($unqualified_identifier.n); }
  ;

sql_use_statement returns [Statement s] options {k=1;}:
  (USE fui=unqualified_identifier { $s = utils.buildUseDatabaseStatement($fui.n); })
  | (USING CONTAINER sui=unqualified_identifier container_dist_vect?  
     { $s = utils.buildUseContainerStatement($sui.n,$container_dist_vect.l); })
  ;

container_dist_vect returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  Left_Paren ldv=container_dist_value { $l.add($ldv.p); } (Comma tdv=container_dist_value { $l.add($tdv.p); })* Right_Paren
  ;    

container_dist_value returns [Pair p] options {k=1;}:
  GLOBAL { $p = new Pair(utils.buildLiteral($GLOBAL), null); }
  | simple_unqualified_identifier Equals_Operator literal { $p = new Pair($literal.expr, $simple_unqualified_identifier.n); }
  | literal { $p = new Pair($literal.expr,null); }
  ;

sql_lock_statement returns [Statement s] options {k=1;}:
  LOCK TABLES push_scope table_lock_specs { $s = utils.buildLockTablesStatement($table_lock_specs.l); utils.popScope();  }
  | UNLOCK TABLES { $s = utils.buildUnlockTablesStatement(); }
  ;
  
table_lock_specs returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  lt=table_lock_spec { $l.add($lt.p); }(Comma tt=table_lock_spec { $l.add($tt.p); })* 
  ;
  
table_lock_spec returns [Pair p] options {k=1;}:
  simple_table_factor lock_type 
  { $p = new Pair($simple_table_factor.expr, $lock_type.lt); }
  ;

lock_type returns [LockType lt] options {k=1;}:
  READ LOCAL? { $lt = utils.lookupLockType($READ.text, $LOCAL.text); }
  | LOW_PRIORITY? WRITE { $lt = utils.lookupLockType($LOW_PRIORITY.text, $WRITE.text); }
  ;

sql_text_pstmt_prepare_statement returns [Statement s] options {k=1;}:
  PREPARE unqualified_identifier FROM Character_String_Literal
  { $s = utils.buildPreparePreparedStatement($unqualified_identifier.n, $Character_String_Literal.text); }
  ;
  
sql_text_pstmt_execute_statement returns [Statement s] options {k=1;}:
  { ArrayList vars = new ArrayList(); }
  EXECUTE unqualified_identifier (USING f=rhs_variable_ref { vars.add($f.vi); } (Comma t=rhs_variable_ref { vars.add($t.vi); })*)?
  { $s = utils.buildExecutePreparedStatement($unqualified_identifier.n, vars); }
  ;
  
sql_text_pstmt_deallocate_statement returns [Statement s] options {k=1;}:
  DEALLOCATE PREPARE unqualified_identifier
  { $s = utils.buildDeallocatePreparedStatement($unqualified_identifier.n); }
  ;

sql_set_statement returns [Statement s] options {k=1;}:
  SET ((set_sess_vars { $s = $set_sess_vars.s; }) 
        | (ddl set_user_password { $s = $set_user_password.s; }))
  ;
  
set_sess_vars returns [Statement s] options {k=1;}:
  { ArrayList parts = new ArrayList(); utils.startSessionVarsScope(); }
  OPTION? lov=option_value { parts.add($lov.sve); } (Comma tov=option_value { parts.add($tov.sve); })* 
  { $s = utils.buildSessionSetVarStatement(parts); utils.endSessionVarsScope(); }
  ;

set_user_password returns [Statement s] options {k=1;}:
  PASSWORD (FOR userid)? Equals_Operator PASSWORD Left_Paren Character_String_Literal Right_Paren 
  { $s = utils.buildSetPasswordStatement($userid.us, $Character_String_Literal.text); }
  ;

// we support global variables now.  persistent values are modified via alter dve.
// the syntax we seek to support is:
// [1] (rhs_variable_ref Equals_Operator set_expr_or_default) // user variable
// [2] ((GLOBAL | SESSION)? unqualified_identifier Equas_Operator set_expr_or_default) // session/global variable variant 1
// [3] ((@@global. | @@session. | @@)unqualified_identifier Equals_Operator set_expr_or_default) // session/global variable variant 2
// @@session.<name>, @@<name>, session <name>, local <name>, @@local.<name> all mean session vars 

option_value returns [SetExpression sve] options {k=1;}:
  // TRANSACTION is an unqualified_identifier, so we have a special rule just so we can catch that case
  // handle the exceptions first
  (NAMES^ charset_type collate_spec? 
  { ArrayList rhs = new ArrayList(); rhs.add(utils.buildIdentifierLiteral($charset_type.n)); if ($collate_spec.n != null) rhs.add(utils.buildIdentifierLiteral($collate_spec.n));
    $sve = utils.buildSetVariableExpression(utils.buildLHSVariableInstance(utils.buildVariableScope(VariableScopeKind.SESSION),bkn($NAMES),$NAMES.tree),rhs); })
  | 
  (charset_expr_tag cst=charset_type 
  { $sve = utils.buildSetVariableExpression(utils.buildLHSVariableInstance(utils.buildVariableScope(VariableScopeKind.SESSION),$charset_expr_tag.n,$charset_expr_tag.tree), 
    utils.buildIdentifierLiteral($cst.n)); })
  |
  // this branch handles everything of the form @... = foo
  (rhs_variable_ref Equals_Operator c=set_expr_or_default
   { $sve = utils.buildSetVariableExpression($rhs_variable_ref.vi,$c.expr); })
  |
  // handle (global|session) (transaction isolation_level .. | unqualified_identifier = expr)
  (lvsk=variable_scope_kind 
    (
      (TRANSACTION ISOLATION LEVEL il=isolation_level 
       { $sve = utils.buildSetTransactionIsolation($il.il, $lvsk.vsk); }) 
      | 
      (keyword_simple_identifier1 Equals_Operator nsve=set_expr_or_default 
       { $sve = utils.buildSetVariableExpression(utils.buildLHSVariableInstance(utils.buildVariableScope($lvsk.vsk),$keyword_simple_identifier1.n, $keyword_simple_identifier1.tree),$nsve.expr);})
    )
  )
  |
  // handle (transaction isolation level) | 
  (TRANSACTION ISOLATION LEVEL il=isolation_level { $sve = utils.buildSetTransactionIsolation($il.il, VariableScopeKind.SESSION); } )
  |
  // handle keyword_simple_identifier2 = expr
  (keyword_simple_identifier2 Equals_Operator ssve=set_expr_or_default
   { $sve = utils.buildSetVariableExpression(utils.buildLHSVariableInstance(utils.buildVariableScope(VariableScopeKind.SESSION), $keyword_simple_identifier2.n, $keyword_simple_identifier2.tree),$ssve.expr); }
  )
   ;
  
lhs_variable_ref returns [VariableInstance vi] options {k=1;}:
  (rhs_variable_ref { $vi = $rhs_variable_ref.vi; })
  | (variable_scope_kind? unqualified_identifier
     {$vi = utils.buildLHSVariableInstance(utils.buildVariableScope($variable_scope_kind.vsk),$unqualified_identifier.n,$unqualified_identifier.tree); })
  ;
  
rhs_variable_ref returns [VariableInstance vi] options {k=1;}:
  (f=AT_Sign (t=AT_Sign (variable_scope_kind Period)?)? unqualified_identifier
  { $vi = utils.buildRHSVariableInstance($f,$t,$variable_scope_kind.vsk, $unqualified_identifier.n, $unqualified_identifier.tree); })
  ;

variable_scope_kind returns [VariableScopeKind vsk] options {k=1;}:
  (GLOBAL { $vsk = VariableScopeKind.GLOBAL; })
  | (SESSION { $vsk = VariableScopeKind.SESSION; })
  | (DVE { $vsk = VariableScopeKind.SCOPED; })
  | (LOCAL { $vsk = VariableScopeKind.SESSION; })
  ;
  
charset_type returns [Name n] options {k=1;}:
  unqualified_identifier { $n = $unqualified_identifier.n; }
  | BINARY { $n = bkn($BINARY); } 
  | DEFAULT { $n = bkn($DEFAULT); } 
  | string_literal { $n = utils.buildNameFromStringLiteral($string_literal.tree); }
  ;

charset_expr_tag returns [Name n] options {k=1;}:
  CHAR SET { $n = utils.buildName("CHARSET"); }
  | CHARSET { $n = utils.buildName("CHARSET"); }
  | CHARACTER SET { $n = utils.buildName("CHARSET"); } 
  ;

collate_spec returns [Name n] options {k=1;}:
  COLLATE collate_type 
  { $n = $collate_type.n; }
  ;

collate_type returns [Name n] options {k=1;}: 
  collation_identifier { $n = $collation_identifier.n; } 
  | DEFAULT { $n = bkn($DEFAULT); }
  ;

set_expr_or_default returns [ExpressionNode expr] options {k=1;}:
  value_expression { $expr = $value_expression.expr; }
  | DEFAULT { $expr = bkexpr($DEFAULT); }
  | ON { $expr = bkexpr($ON); }
  | OFF { $expr = bkexpr($OFF); }
  | ALL { $expr = bkexpr($ALL); }
  ;


isolation_level returns [SetTransactionIsolationExpression.IsolationLevel il] options {k=1;}:
  READ (UNCOMMITTED { $il = SetTransactionIsolationExpression.IsolationLevel.READ_UNCOMMITTED; }
        | COMMITTED { $il = SetTransactionIsolationExpression.IsolationLevel.READ_COMMITTED; })
  | REPEATABLE READ { $il = SetTransactionIsolationExpression.IsolationLevel.REPEATABLE_READ; }
  | SERIALIZABLE { $il = SetTransactionIsolationExpression.IsolationLevel.SERIALIZABLE; }
  ;

multitenant_mode returns [MultitenantMode mode] options {k=1;}:
  MULTITENANT
  { $mode = MultitenantMode.ADAPTIVE; }
  ;

pe_definition_target returns [Statement s] options {k=1;}:
  database_definition { $s = $database_definition.s; }
//  | PROJECT project_sub1 { $s = $project_sub1.s; }
  | PERSISTENT (
    INSTANCE simple_unqualified_identifier config_options { $s = utils.buildCreatePersistentInstance($simple_unqualified_identifier.n, $config_options.l); }
    | persistent_sub1 { $s = $persistent_sub1.s; }
    )
  | RANGE if_not_exists rn=unqualified_identifier type_list PERSISTENT GROUP sg=unqualified_identifier { $s = utils.buildCreateRangeStatement($rn.n,$sg.n,$type_list.l, $if_not_exists.b); }
  | TENANT tn=unqualified_identifier Character_String_Literal? (ON ondb=unqualified_identifier)? { $s = utils.buildCreateTenantStatement($tn.n, $ondb.n, $Character_String_Literal.text); }
  | DYNAMIC SITE ((policy_definition_target { $s = $policy_definition_target.s; })| (provider_definition_target { $s = $provider_definition_target.s; }))
  | EXTERNAL SERVICE unqualified_identifier USING config_options { $s = utils.buildCreateExternalServiceStatement($unqualified_identifier.n, $config_options.l); }
  | CONTAINER if_not_exists cn=unqualified_identifier PERSISTENT GROUP csg=unqualified_identifier cdd=container_distribution_declaration 
  { $s = utils.buildCreateContainerStatement($cn.n, $csg.n, $cdd.p, $if_not_exists.b); } 
  | TEMPLATE if_not_exists tempn=unqualified_identifier template_parameters
  { $s = utils.buildCreateTemplateStatement($tempn.n, $if_not_exists.b, $template_parameters.l); }
  | RAW PLAN if_not_exists rpn=unqualified_identifier DATABASE dbn=unqualified_identifier rawplan_parameters
  { $s = utils.buildCreateRawPlanStatement($rpn.n, $if_not_exists.b, $dbn.n, $rawplan_parameters.l); }
  ;

database_definition returns [Statement s] options {k=1;}:
  multitenant_mode? database_tag if_not_exists dbn=unqualified_identifier
  consolidated_db_defs 
  { $s = utils.buildCreateDatabase($dbn.n, $if_not_exists.b, $database_tag.text, $multitenant_mode.mode, $consolidated_db_defs.l); }
  ;

policy_definition_target returns [Statement s] options {k=1;}:
  POLICY unqualified_identifier Left_Paren class_definition_list? Right_Paren (STRICT Equals_Operator (o=ON | OFF))
  { $s = utils.buildCreateDynamicSitePolicyStatement($unqualified_identifier.n,($o != null),$class_definition_list.l); }
  ;

class_definition_list returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  lcdl=policy_class_definition { $l.add($lcdl.cd); } (Comma tcdl=policy_class_definition { $l.add($tcdl.cd); })*
  ;

policy_class_definition returns [PEPolicyClassConfig cd] options {k=1;}:
  unqualified_identifier ((COUNT c=unsigned_integral_literal) | (PROVIDER prov=string_literal) | (POOL po=string_literal))*
  { $cd = utils.buildClassConfig($unqualified_identifier.n,$c.expr,$prov.expr,$po.expr); }
  ;
  

provider_definition_target returns [Statement s] options {k=1;}:
  PROVIDER unqualified_identifier USING config_options
  { $s = utils.buildCreateDynamicSiteProvider($unqualified_identifier.n, $config_options.l); }
  ;
if_not_exists returns [Boolean b] options {k=1;}
  @init { $b = Boolean.FALSE; }
  :
  (IF NOT EXISTS { $b = Boolean.TRUE; })?
  ;

if_exists returns [Boolean b] options {k=1;}:
  (IF EXISTS { $b = Boolean.TRUE; })
  ;

consolidated_db_defs returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  ((td=template_declaration_kern { $l.add($td.p); })
   |(fkm=fkmode_declaration_kern { $l.add($fkm.p); })
   |(DEFAULT?
     ((ch=create_db_charset_expr { $l.add(new Pair("charset",$ch.n)); })
     |(co=create_db_collate_expr { $l.add(new Pair("collate",$co.n)); })
     |(pg=create_db_pg { $l.add(new Pair("pers_group",$pg.n)); }))))*
  ;

template_declaration_kern returns [Pair p] options {k=1;}:
  USING TEMPLATE tn=unqualified_identifier tm=template_mode? { $p = utils.buildTemplateDeclaration($tn.n, $tm.mode); }
  ;

template_mode returns [TemplateMode mode] options {k=1;}:
  (OPTIONAL { $mode = TemplateMode.OPTIONAL; })
  |(REQUIRED { $mode = TemplateMode.REQUIRED; })
  |(STRICT { $mode = TemplateMode.STRICT; })
  ;

fkmode_declaration_kern returns [Pair p] options {k=1;}:
  FOREIGN KEY MODE fkmode_value
  { $p = new Pair("fkmode", $fkmode_value.fkm); }
  ;
  
fkmode_value returns [FKMode fkm] options {k=1;}:  
  (STRICT { $fkm = FKMode.STRICT; })
  |(IGNORE { $fkm = FKMode.IGNORE; })
  |(EMULATE { $fkm = FKMode.EMULATE; })
  ;
  
create_db_pg returns [Name n] options {k=1;}:
  PERSISTENT GROUP unqualified_identifier { $n = $unqualified_identifier.n; }
  ;
  
create_db_charset_expr returns [Name n] options {k=1;}:
  charset_expr_tag Equals_Operator? charset_type { $n = $charset_type.n; }
  ;

create_db_collate_expr returns [Name n] options {k=1;}:
  COLLATE Equals_Operator? charset_type { $n = $charset_type.n; }
  ;

// project_sub1 returns [Statement s] options {k=1;}:
//  CATALOG Character_String_Literal { $s = utils.buildCreateCatalog($Character_String_Literal); }
//  | pn=unqualified_identifier DEFAULT PERSISTENT GROUP gn=unqualified_identifier { $s = utils.buildCreateProject($pn.n,$gn.n); }
//  ;

persistent_sub1 returns [Statement s] options {k=1;}:
  (SITE sn=unqualified_identifier 
   ((config_options { $s = utils.buildCreatePersistentSite($sn.n, $config_options.l); })
    | 
    (OF TYPE ha_definition_tag SET MASTER mn=unqualified_identifier ADD? unqualified_identifier_list?
     { $s = utils.buildCreatePersistentSite($sn.n,$ha_definition_tag.s,$mn.n,$unqualified_identifier_list.l); })))  
  | GROUP unqualified_identifier (ADD unqualified_identifier_list)? { $s = utils.buildCreatePersistentGroup($unqualified_identifier.n, $unqualified_identifier_list.l); } 
  ;

ha_definition_tag returns [String s] options {k=1;}:
  SINGLE { $s = Worker.SINGLE_DIRECT_HA_TYPE; }
  | MASTERMASTER { $s = Worker.MASTER_MASTER_HA_TYPE; }
  ;
  
sql_pe_session_statement returns [Statement s] options {k=1;}:
  RELOAD LOGGING { $s = utils.buildReloadLogging(); } 
  ;

database_tag options {k=1;}: 
  DATABASE // -> ^(DATABASE DATABASE)
  | SCHEMA // -> ^(DATABASE SCHEMA)
  ;

default_persistent_group options {k=1;}:
  { unsupported("default_persistent_group");}
  DEFAULT PERSISTENT GROUP unqualified_identifier // -> ^(DEFAULT PERSISTENT GROUP identifier)
  ;

distribution_declaration_target returns [UnresolvedDistributionVector dv] options {k=1;}:
  BROADCAST DISTRIBUTE { $dv = utils.buildDistributionVector(DistributionVector.Model.BROADCAST,null,null); }
  | RANDOM DISTRIBUTE { $dv = utils.buildDistributionVector(DistributionVector.Model.RANDOM,null,null); }
  | STATIC DISTRIBUTE distribution_columns { $dv = utils.buildDistributionVector(DistributionVector.Model.STATIC, $distribution_columns.l, null); } 
  | RANGE DISTRIBUTE distribution_columns USING rn=unqualified_identifier 
  { $dv = utils.buildDistributionVector(DistributionVector.Model.RANGE, $distribution_columns.l, $rn.n); }
  | CONTAINER DISTRIBUTE cn=unqualified_identifier { $dv = utils.buildDistributionVector(DistributionVector.Model.CONTAINER, null, $cn.n); }
  ;

container_distribution_declaration returns [Pair p] options {k=1;}:
  BROADCAST DISTRIBUTE { $p = new Pair(DistributionVector.Model.BROADCAST,null); }
  | RANDOM DISTRIBUTE { $p = new Pair(DistributionVector.Model.RANDOM,null); }
  | STATIC DISTRIBUTE { $p = new Pair(DistributionVector.Model.STATIC,null); }
  | RANGE DISTRIBUTE USING unqualified_identifier { $p = new Pair(DistributionVector.Model.RANGE,$unqualified_identifier.n); }
  ;

distribution_declaration returns [UnresolvedDistributionVector dv] options {k=1;}:
  distribution_declaration_target { $dv = $distribution_declaration_target.dv; }
  ;

distribution_columns returns [List l] options {k=1;}: 
  ON Left_Paren unqualified_identifier_list Right_Paren 
  { $l = $unqualified_identifier_list.l; }
  ;

type_list returns [List l] options {k=1;}: 
  { $l = new ArrayList(); }
  Left_Paren ltd=any_type_description { $l.add($ltd.type); }(Comma ttd=any_type_description { $l.add($ttd.type); })* Right_Paren 
//  Left_Paren firstrt=range_type { $l.add($firstrt.p); }(Comma additionalrt=range_type { $l.add($additionalrt.p); })* Right_Paren 
  ;

user_spec_list returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  lu=user_spec { $l.add($lu.us); } (Comma tu=user_spec { $l.add($tu.us); })* 
  ;

user_spec returns [PEUser us] options {k=1;}:
  userid (IDENTIFIED BY PASSWORD? Character_String_Literal)? 
  { $us = utils.buildUserSpec($userid.us, $Character_String_Literal.text); }
  ;

userid returns [UserScope us] options {k=1;}:
  un=userid_part (AT_Sign mn=host_spec)? 
  { $us = utils.buildUserScope($un.text, $mn.text); }    
  ;

userid_part returns [String text] options {k=1;}:
  un=Character_String_Literal { $text = $un.text; }
  | Single_Or_Double_Or_Back_Quote uq1=unqualified_identifier Single_Or_Double_Or_Back_Quote { $text = $uq1.n.get(); }
  | uq2=unqualified_identifier { $text = $uq2.n.get(); }
  ;

// i.e. `192.168.%` or 'localhost' or 'a.b.c.d' or "c.d.%"
host_spec returns [String text] options {k=1;}:
  Back_Quoted_String_Literal { $text = utils.massageHostSpec($Back_Quoted_String_Literal.text); }
  | Regular_Identifier { $text = utils.massageHostSpec($Regular_Identifier.text); }
  | Character_String_Literal { $text = $Character_String_Literal.text; }
  ;

config_options returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  lgp=config_option { $l.add($lgp.p); }(tgp=config_option { $l.add($tgp.p); })* 
  ;
  
// we're going to use qualified_identifier here, but in the translator we'll use a special rule that handles
// arbitrary nesting (qualified_identifier in translator only handles 3 levels)
config_option returns [Pair p] options {k=1;}:
  qualified_identifier Equals_Operator literal 
  { $p = utils.buildConfigOption($qualified_identifier.n, $literal.expr); }
  ;


drop_statement returns [Statement s] options {k=1;}:
  (TEMPORARY { utils.notddl(); })? TABLE (IF e=EXISTS)? qualified_identifier_list { $s = utils.buildDropTableStatement($qualified_identifier_list.l, ($e != null), $TEMPORARY != null); }
  | MULTITENANT? database_tag (IF e=EXISTS)? unqualified_identifier { $s = utils.buildDropDatabaseStatement($unqualified_identifier.n,($e != null), ($MULTITENANT != null), $database_tag.text); } 
  | USER (IF e=EXISTS)? userid { $s = utils.buildDropUserStatement($userid.us, $e != null); }
  | VIEW (IF ve=EXISTS)? qualified_identifier { $s = utils.buildDropViewStatement($qualified_identifier.n,$ve != null); }
  | INDEX in=unqualified_identifier ON tn=qualified_identifier { $s = utils.buildExternalDropIndexStatement($in.n,$tn.n); }
  ;

pe_drop_target returns [Statement s] options {k=1;}:
  TENANT unqualified_identifier { $s = utils.buildDropTenantStatement($unqualified_identifier.n); }
  | DYNAMIC SITE (p=PROVIDER | POLICY) unqualified_identifier config_options? {
      if ($p != null) $s = utils.buildDropDynamicSiteProviderStatement($unqualified_identifier.n, $config_options.l);
      else $s = utils.buildDropDynamicSitePolicyStatement($unqualified_identifier.n);
  } 
  | RANGE if_exists? rn=unqualified_identifier (PERSISTENT GROUP sg=unqualified_identifier)? { $s = utils.buildDropRangeStatement($rn.n, ($if_exists.b != null), $sg.n); }
  | PERSISTENT (
    (g=GROUP | SITE) gsui=unqualified_identifier {
        if ($g != null) $s = utils.buildDropPersistentGroupStatement($gsui.n); 
        else $s = utils.buildDropPersistentSiteStatement($gsui.n);
    }
    | INSTANCE ui=unqualified_identifier { $s = utils.buildDropPersistentInstanceStatement($ui.n); }
  )
  | EXTERNAL SERVICE unqualified_identifier { $s = utils.buildDropExternalServiceStatement($unqualified_identifier.n); }
  | CONTAINER if_exists? unqualified_identifier { $s = utils.buildDropContainerStatement($unqualified_identifier.n, ($if_exists.b != null)); }
  | TEMPLATE if_exists? unqualified_identifier { $s = utils.buildDropTemplateStatement($unqualified_identifier.n, ($if_exists.b != null)); }
  | RAW PLAN if_exists? unqualified_identifier { $s = utils.buildDropRawPlanStatement($unqualified_identifier.n, ($if_exists.b != null)); }
  ;

pe_alter_target returns [Statement s] options {k=1;}:
  PERSISTENT (
    alter_persistent_sub1 { $s = $alter_persistent_sub1.s; } 
    | INSTANCE unqualified_identifier config_options { $s = utils.buildAlterPersistentInstanceStatement($unqualified_identifier.n, $config_options.l); }
  )
  | (DVE
     ( 
     (SET vn=unqualified_identifier Equals_Operator set_expr_or_default 
     { $s = utils.buildAlterPersistentVariable(utils.buildLHSVariableInstance(utils.buildVariableScope(VariableScopeKind.PERSISTENT),$vn.n,$DVE.tree), $set_expr_or_default.expr); } )
     |
     (ADD VARIABLE nvn=unqualified_identifier config_options { $s = utils.buildAddVariable($nvn.n, $config_options.l); })
     )
    )
  | DYNAMIC SITE alter_dynamic_site_target { $s = $alter_dynamic_site_target.s; }
  | EXTERNAL SERVICE unqualified_identifier SET config_options { $s = utils.buildAlterExternalServiceStatement($unqualified_identifier.n, $config_options.l); }
  | TEMPLATE tempn=unqualified_identifier SET template_parameters
  { $s = utils.buildAlterTemplateStatement($tempn.n, $template_parameters.l); }
  | RAW PLAN rpn=unqualified_identifier SET rawplan_parameters
  { $s = utils.buildAlterRawPlanStatement($rpn.n, $rawplan_parameters.l); }
  ;
    
alter_dynamic_site_target returns [Statement s] options {k=1;}:
  (POLICY alter_dynamic_policy_target { $s = $alter_dynamic_policy_target.s; })  
  | (PROVIDER alter_dynamic_site_provider_target { $s = $alter_dynamic_site_provider_target.s; })
  ;

alter_dynamic_policy_target returns [Statement s] options {k=1;}:
  pn=unqualified_identifier (SET STRICT Equals_Operator ((y=ON) | (n=OFF)))? (CHANGE class_definition_list)?
  { Boolean ns = ($y == null && $n == null ? null : $y != null);
    $s = utils.buildAlterDynamicSitePolicyStatement($pn.n, null, ns, $class_definition_list.l); }
  ;

rawplan_parameters returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  (tp=rawplan_parameter { $l.add($tp.p); })*
  ;

rawplan_parameter returns [Pair p] options {k=1;}:
  (
  xmlbody { $p = $xmlbody.p; }
  | xmlcomment { $p = $xmlcomment.p; }
  | (ENABLED Equals_Operator (t=TRUE | FALSE) { $p = new Pair("enabled",($t != null) ? "true" : "false");} )
  )
  ;

template_parameter returns [Pair p] options {k=1;}:
  (
  xmlbody { $p = $xmlbody.p; }
  | (MATCH Equals_Operator matchbody=Character_String_Literal {$p = new Pair("match",$matchbody.text);})
  | xmlcomment { $p = $xmlcomment.p; }
  )
  ;

xmlbody returns [Pair p] options {k=1;}:
  (XML Equals_Operator body=Character_String_Literal { $p = new Pair("xml", $body.text); })
  ;
  
xmlcomment returns [Pair p] options {k=1;}:
  (FIELD_COMMENT Equals_Operator? commentbody=Character_String_Literal { $p = new Pair("comment", $commentbody.text); })
  ;

template_parameters returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  (tp=template_parameter { $l.add($tp.p); })*
  ;
  
alter_dynamic_site_provider_target returns [Statement s] options {k=1;}:
  ln=unqualified_identifier ((SET vn=unqualified_identifier Equals_Operator set_expr_or_default) | config_options) 
  { if ($config_options.l != null) $s = utils.buildAlterDynamicSiteProviderStatement($ln.n,$config_options.l);
    else {
    VariableScope vs = utils.buildVariableScope($ln.n);
    VariableInstance vi = utils.buildLHSVariableInstance(vs,$vn.n,$vn.tree);
    $s = utils.buildAlterPersistentVariable(vi,$set_expr_or_default.expr);
    }
  }  
  ;

alter_persistent_sub1 returns [Statement s] options {k=1;}:
  (GROUP sg=unqualified_identifier ADD GENERATION sn=unqualified_identifier_list { $s = utils.buildAddPersistentSiteStatement($sg.n, $sn.l); })
  |
  (SITE ssn=unqualified_identifier
    (
      (SET 
        ((TYPE ha_definition_tag { $s = utils.buildAlterPersistentSite($ssn.n, $ha_definition_tag.s); })
        |(MASTER mn=unqualified_identifier { $s = utils.buildAlterPersistentSite($ssn.n, $mn.n); })
        )
      )
      |
      ((a=ADD | DROP) unqualified_identifier_list { $s = utils.buildAlterPersistentSite($ssn.n, ($a.text != null), $unqualified_identifier_list.l); })
    )
  )
  ;

table_alter_target_change_column returns [ExpressionNode expr] options {k=1;}:
  SET DEFAULT literal { $expr = $literal.expr; }
  | DROP DEFAULT { $expr = null; }
  ;

sql_schema_maintenance_statement returns [Statement s] options {k=1;}:
  (oper=(OPTIMIZE | CHECK) (opt=(NO_WRITE_TO_BINLOG | LOCAL))? TABLE qualified_identifier_list { $s = utils.buildMaintenanceQuery($oper.text, $opt.text, $qualified_identifier_list.l); })
  | (ANALYZE (aopt=(NO_WRITE_TO_BINLOG | LOCAL))? (t=TABLE | k=KEYS) aqi=qualified_identifier_list 
     { if ($t == null)
         $s = utils.buildAnalyzeKeys($aqi.l);
       else
         $s = utils.buildMaintenanceQuery("ANALYZE", $aopt.text, $aqi.l);  
     })
  ;

sql_schema_show_statement returns [Statement s] options {k=1;}:
  SHOW sql_schema_show_statement_target 
  { $s = $sql_schema_show_statement_target.s; }
  ;

sql_schema_show_statement_target returns [Statement s] options {k=1;}:
  (mt=MULTITENANT? (SCHEMAS | DATABASES | l=LOCK) sql_schema_like_or_where?  
   { if ($l == null)
      $s = utils.buildShowPluralQuery(($mt==null ? "DATABASE" : "MULTITENANT DATABASE"), null, $sql_schema_like_or_where.pair);
     else
       $s = utils.buildShowMultitenantLocks(); 
   }
   ) 
  | RANGES sql_schema_like_or_where? { $s = utils.buildShowPluralQuery("RANGE",null,$sql_schema_like_or_where.pair); }
//  | PROJECTS sql_schema_like_or_where? { $s = utils.buildShowPluralQuery("PROJECT", null, $sql_schema_like_or_where.pair); }
  | TENANTS sql_schema_like_or_where? { $s = utils.buildShowPluralQuery("TENANT",null, $sql_schema_like_or_where.pair); }
  | a=(TENANT | DATABASE | RANGE 
  //| PROJECT
  ) unqualified_identifier { $s = utils.buildShowSingularQuery($a.text, $unqualified_identifier.n); }
  | SCHEMA unqualified_identifier { $s = utils.buildShowSingularQuery("DATABASE",$unqualified_identifier.n); }
  | CREATE (
  	(TABLE tui=qualified_identifier) { $s = utils.buildShowSingularQuery("CREATE TABLE",$tui.n); }
  	| ((DATABASE | SCHEMA) if_not_exists dsui=unqualified_identifier) { $s = utils.buildShowCreateDatabaseQuery("CREATE DATABASE",$dsui.n, $if_not_exists.b); }
  	)
  | EVENTS sql_schema_show_scoping? sql_schema_like_or_where? { $s = utils.buildShowPluralQuery("EVENTS",$sql_schema_show_scoping.l,$sql_schema_like_or_where.pair); }
  | ENGINES sql_schema_like_or_where? { $s = utils.buildShowPluralQuery("ENGINES",null,null); }
  | TRIGGERS sql_schema_show_scoping? sql_schema_like_or_where? { $s = utils.buildShowPluralQuery("TRIGGER",$sql_schema_show_scoping.l,$sql_schema_like_or_where.pair); }
  | TABLE lui=unqualified_identifier { utils.push_info_schema_scope("TABLE STATUS"); } sql_schema_show_scoping? tsw=sql_schema_like_or_where?
  { if ($lui.n.get().toUpperCase().equals("STATUS")) {
       $s = utils.buildShowPluralQuery("TABLE STATUS",$sql_schema_show_scoping.l, $tsw.pair);        
    } else {
       $s = utils.buildShowSingularQuery("TABLE",$lui.n);
    }}
  | PERSISTENT (
    (GROUPS gw=sql_schema_like_or_where?) { $s = utils.buildShowPluralQuery("PERSISTENT GROUP",null,$gw.pair); }    
    | (w=(SITE | GROUP) unqualified_identifier)  { $s = utils.buildShowSingularQuery("PERSISTENT " + $w.text, $unqualified_identifier.n); } 
    | (SITES sw=sql_schema_like_or_where?) { $s = utils.buildShowPluralQuery("PERSISTENT SITE",null,$sw.pair); }
    | INSTANCES { utils.push_info_schema_scope("PERSISTENT INSTANCES"); } sis=sql_schema_like_or_where? { $s = utils.buildShowPluralQuery("PERSISTENT INSTANCE", null, $sis.pair); }
    )
  | DYNAMIC SITE (
    (PROVIDERS ((gps=SITES) | ( { utils.push_info_schema_scope("DYNAMIC SITE PROVIDERS"); } gpw=sql_schema_like_or_where))?) {
      if ($gps == null)  $s = utils.buildShowPluralQuery("DYNAMIC SITE PROVIDER",null,$gpw.pair);
      else $s = utils.buildShowDynamicSiteProvidersSites(); }
    | (POLICY gpn=unqualified_identifier)  { $s = utils.buildShowSingularQuery("DYNAMIC SITE POLICY",$gpn.n); }// -> POLICY identifier
    | (POLICIES { utils.push_info_schema_scope("DYNAMIC SITE POLICIES"); } gpw=sql_schema_like_or_where?) { $s = utils.buildShowPluralQuery("DYNAMIC SITE POLICY",null,$gpw.pair); }
    | (PROVIDER show_dynamic_site_provider_target) { $s = $show_dynamic_site_provider_target.s; }
    )
  | SITES STATUS { $s = utils.buildShowSitesStatus(); } 
  | regular_variable_scope? (
  	(VARIABLES { utils.push_info_schema_scope("VARIABLES"); } v=sql_schema_like_or_where?) { $s = utils.buildShowVariables($regular_variable_scope.vs, $v.pair); }
  	| (STATUS stats=sql_schema_like_or_where?) { $s = utils.buildShowStatus($stats.pair); }
  	)
  | (GENERATIONS genw=sql_schema_like_or_where?) { $s = utils.buildShowPluralQuery("GENERATION",null,$genw.pair); }
  | GENERATION (
    SITES { utils.push_info_schema_scope("GENERATION SITES"); } sql_schema_like_or_where?  { $s = utils.buildShowPluralQuery("GENERATION SITE",null,$sql_schema_like_or_where.pair); }
    | unsigned_integral_literal { unsupported("show unique generation"); }// -> GENERATION { utils.buildIdentifier(this.adaptor, $unsigned_integral_literal.text) }
    )
  | FULL? (
    (FIELDS | COLUMNS) sfcscp=sql_schema_show_scoping { utils.push_info_schema_scope("COLUMNS"); } sfclw=sql_schema_like_or_where? { $s = utils.buildShowColumns("COLUMN", $sfcscp.l, $sfclw.pair, $FULL); }
    | PROCESSLIST { $s = utils.buildShowProcesslistStatement($FULL != null); }
    | TABLES { utils.push_info_schema_scope("TABLE"); } stblscp=sql_schema_show_scoping? stbllw=sql_schema_like_or_where?  { $s = utils.buildShowPluralQuery("TABLE", $stblscp.l, $stbllw.pair, $FULL); }
    )
  | (w=WARNINGS | e=ERRORS) limit_specification? { $s = utils.buildShowErrorsOrWarnings($w.text == null ? $e.text : $w.text, $limit_specification.ls); }
  | COLLATION sql_schema_like_or_where? { $s = utils.buildShowPluralQuery("COLLATION",null, $sql_schema_like_or_where.pair); } 
  | EXTERNAL ((SERVICES { utils.push_info_schema_scope("EXTERNAL SERVICE");} esw=sql_schema_like_or_where? { $s = utils.buildShowPluralQuery("EXTERNAL SERVICE", null, $esw.pair); })
              | (SERVICE sn=unqualified_identifier { $s = utils.buildShowSingularQuery("EXTERNAL SERVICE", $sn.n); }))
  | (KEYS | INDEXES | INDEX ) { utils.push_info_schema_scope("KEY"); } sks=sql_schema_show_scoping? skw=sql_schema_like_or_where? { $s = utils.buildShowPluralQuery("KEY", $sks.l, $skw.pair); }
  | ((CHARACTER SET) | CHARSET ) sql_schema_like_or_where? { $s = utils.buildShowPluralQuery("CHARSET",null, $sql_schema_like_or_where.pair); } 
  | PLUGINS { $s = utils.buildShowPassthroughStatement("PLUGINS"); }
  | MASTER  (
    LOGS { $s = utils.buildShowPassthroughStatement("MASTER LOGS"); }
    | STATUS { $s = utils.buildShowPassthroughStatement("MASTER STATUS"); }
    )
  | SLAVE STATUS { $s = utils.buildShowPassthroughStatement("SLAVE STATUS"); }
  | GRANTS { $s = utils.buildShowPassthroughStatement("GRANTS"); }
  | CONTAINER unqualified_identifier  { $s = utils.buildShowSingularQuery("CONTAINER", $unqualified_identifier.n); } 
  | CONTAINERS { utils.push_info_schema_scope("CONTAINER");} sql_schema_show_scoping? sql_schema_like_or_where?  { $s = utils.buildShowPluralQuery("CONTAINER", $sql_schema_show_scoping.l, $sql_schema_like_or_where.pair); }
  | CONTAINER_TENANTS { utils.push_info_schema_scope("CONTAINER_TENANT"); } sql_schema_show_scoping? sql_schema_like_or_where? { $s = utils.buildShowPluralQuery("CONTAINER_TENANT", $sql_schema_show_scoping.l, $sql_schema_like_or_where.pair); }
  | PROCEDURE STATUS { utils.push_info_schema_scope("PROCEDURE STATUS"); } sql_schema_like_or_where?  { $s = utils.buildShowPluralQuery("PROCEDURE STATUS", null, $sql_schema_like_or_where.pair); }
  | FUNCTION STATUS { utils.push_info_schema_scope("FUNCTION STATUS"); } sql_schema_like_or_where?  { $s = utils.buildShowPluralQuery("FUNCTION STATUS", null, $sql_schema_like_or_where.pair); }
  | TEMPLATES { utils.push_info_schema_scope("TEMPLATE"); } (tkw=sql_schema_like_or_where)? { $s = utils.buildShowPluralQuery("TEMPLATE", null, $tkw.pair); }
  | TEMPLATE ON ((DATABASE | SCHEMA) if_not_exists dsui=unqualified_identifier) { $s = utils.buildShowSingularQuery("TEMPLATE ON DATABASE",$dsui.n); }
  | SERVERS { utils.push_info_schema_scope("SERVER"); } (tkw=sql_schema_like_or_where)? { $s = utils.buildShowPluralQuery("SERVER", null, $tkw.pair); }
  | RAW PLANS { utils.push_info_schema_scope("RAWPLAN"); } (rpw=sql_schema_like_or_where)? { $s = utils.buildShowPluralQuery("RAWPLAN", null, $rpw.pair); }
  | PLAN CACHE STATISTICS? { $s = utils.buildShowPlanCache($STATISTICS != null); }
  ;

show_dynamic_site_provider_target returns [Statement s] options {k=1;}:
  (simple_unqualified_identifier config_options { $s = utils.buildShowDynamicSiteProvider($simple_unqualified_identifier.n, $config_options.l); }) 
  | (VARIABLES simple_unqualified_identifier? sql_schema_like_or_where? {
    VariableScope vs = utils.buildVariableScope($simple_unqualified_identifier.n);
    $s = utils.buildShowVariables(vs, $sql_schema_like_or_where.pair);
  }) 
  ;

regular_variable_scope returns [VariableScope vs] options {k=1;}:
  SESSION { $vs = utils.buildVariableScope(VariableScopeKind.SESSION); }
  | GLOBAL { $vs = utils.buildVariableScope(VariableScopeKind.GLOBAL); }
  | DVE simple_unqualified_identifier? {
     if ($simple_unqualified_identifier.n == null) $vs = utils.buildVariableScope(VariableScopeKind.GLOBAL); 
     else $vs = utils.buildVariableScope($simple_unqualified_identifier.n); 
   }
  ;

sql_schema_show_scoping returns [List l] options {k=1;}:
  { $l = new ArrayList(); }
  ((FROM | IN) qualified_identifier { $l.add($qualified_identifier.n); })+
  ;

sql_schema_describe_statement returns [Statement s] options {k=1;}:
  (DESC | DESCRIBE) qualified_identifier 
  { ArrayList scoping = new ArrayList();
    scoping.add($qualified_identifier.n);
    $s = utils.buildShowColumns("COLUMN", scoping, null, null);
  }
  ;

sql_schema_like_or_where returns [Pair pair] options {k=1;}:
  LIKE like_literal_expr { $pair = new Pair($like_literal_expr.expr,null); }
  | where { $pair = new Pair(null,$where.expr); } 
  ;

like_literal_expr returns [ExpressionNode expr] options {k=1;}:
  ((Left_Paren sl1=string_literal Right_Paren) | sl2=string_literal) { $expr = ((sl1 == null) ? $sl2.expr : $sl1.expr); };

kill_statement returns [Statement s] options {k=1;}:
  KILL (CONNECTION | q=QUERY)? unsigned_numeric_literal { $s = utils.buildKillStatement($unsigned_numeric_literal.expr, $q == null ? Boolean.TRUE : Boolean.FALSE); }
  ;

ddl:
  { utils.ddl(); }
  ;

empty_statement options {k=1;}:
  // COMMENT | LINE_COMMENT;
  EOF 
  ;
