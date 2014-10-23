lexer grammar sql2003Lexer;

options
{
    language=Java;
}

@header {
package com.tesora.dve.sql.parser;
import com.tesora.dve.sql.ParserException.Pass;
}

@members {
  private Utils utils;
  
  public void setUtils(Utils u) { utils = u; }

  private static final String UNKNOWN_CHARACTER = "Unknown Character";

  public void error(String descrip, String text) {
    utils.error(Pass.LEXER, descrip, text);
  }

  public void emitErrorMessage(String s) {
    utils.collectError(Pass.LEXER, s);
  }

  // override to programmatically control tracing
  public void traceIn(String ruleName, int ruleIndex)  {
    if (utils.getOptions().isTraceLexer())
      super.traceIn(ruleName, ruleIndex);
  }
  
  public void traceOut(String ruleName, int ruleIndex) {
    if (utils.getOptions().isTraceLexer())
      super.traceOut(ruleName, ruleIndex);
  }

  public void matchRange(int a, int b) throws MismatchedRangeException
  {
    int t = input.LA(1);
    if ( t<a || t>b ) {
      if ( state.backtracking>0 ) {
        state.failed = true;
        return;
      }
      MismatchedRangeException mre =
        new MismatchedRangeException(a,b,input);
      recover(mre);
      throw mre;
    }
    input.consume();
    state.failed = false;
  }
}

/* 
=====================================================================================

BNF Grammar for ISO/IEC 9075-2:2003 - Database Language SQL (SQL-2003) SQL/Foundation

4/27/2011 - Lexer updated:
			1) Support case insensitive keyword tokens.
			2) Improve parsing of Numeric tokens
			3) Improve parsing of string constant tokens			

=====================================================================================

This software is made available under the BSD License:

Copyright (c) 2011, Mage Systems
Portions Copyright (c)  Jonathan Leffler 2004-2009'
Portions Copyright (c) 2003, ISO - International Organization for Standardization
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, 
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice, this 
      list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice, 
      this list of conditions and the following disclaimer in the documentation 
      and/or other materials provided with the distribution.
    * Neither the name of the "Mage Systems" nor the names of its contributors 
      may be used to endorse or promote products derived from this software 
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, 
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF 
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE 
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED 
OF THE POSSIBILITY OF SUCH DAMAGE.


Information taken from the Final Committee Draft (FCD) of ISO/IEC 9075-2:2003.
However, the page numbers and some section titles (9.14 through 9.23,
for example) are from the final standard.
This means there could be other as yet undiagnosed differences between
the final standard and the notation in this document; you were warned!


	5 Lexical Elements


Basic definitions of characters used, tokens, symbols, etc.
Most of this section would normally be handled within the lexical
analyzer rather than in the grammar proper.
Further, the original document does not quote the various single
characters, which makes it hard to process automatically.
*/

//-------------------------------------------------------------------------
//
//  Lexer Rules
//
//-------------------------------------------------------------------------


// C o m m e n t   T o k e n s
fragment
Start_Comment   : '/*' ~('!' | '#');

End_Comment     : '*/' 
    {
      $channel=HIDDEN;
    }
    ;

fragment
Line_Comment    : '//';

COMMENT
    :   (   Start_Comment ( options {greedy=false;} : . )* End_Comment )+ 
    {
			$channel=HIDDEN;
		} 
		;

LINE_COMMENT
    :   (   ( Line_Comment | '--' ) ~('\n'|'\r')* ('\r'? '\n')?) 
		{
  		$channel=HIDDEN;
		} 
    ;

MYSQL_EXTENSION_COMMENT  : '/*!' Digit*
    { 
       $channel = HIDDEN; 
    }
    ;
DVE_EXTENSION_COMMENT     : '/*#' D V E
    { 
       $channel = HIDDEN; 
    }
    ;
    
fragment
Latin_Letter  :  Latin_Upper_Case_Letter | Latin_Lower_Case_Letter;

fragment A
	:	'A' | 'a';

fragment B 
	:	'B' | 'b';

fragment C 
	:	'C' | 'c';
	
fragment D 
	:	'D' | 'd';
	
fragment E
	:	'E' | 'e';

fragment F
	:	'F' | 'f';

fragment G
	:	'G' | 'g';

fragment H
	:	'H' | 'h';
	
fragment I
	:	'I' | 'i';
	
fragment J
	:	'J' | 'j';
	
fragment K
	:	'K' | 'k';
	
fragment L
	:	'L' | 'l';
	
fragment M 
	:	'M' | 'm';
	
fragment N
	:	'N' | 'n';
	
fragment O
	:	'O' | 'o';
	
fragment P
	:	'P' | 'p';
	
fragment Q
	:	'Q' | 'q';
	
fragment R
	:	'R' | 'r';
	
fragment S
	:	'S' | 's';
	
fragment T
	:	'T' | 't';
	
fragment U
	:	'U' | 'u';
	
fragment V
	:	'V' | 'v';
	
fragment W
	:	'W' | 'w';
	
fragment X
	:	'X' | 'x';
	
fragment Y
	:	'Y' | 'y';
	
fragment Z
	:	'Z' | 'z';

fragment
Latin_Upper_Case_Letter :
		'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 
		'N' | 'O' | 'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z';

fragment
Latin_Lower_Case_Letter :
		'a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 
		'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z';

fragment
Digit  :  '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9';

fragment
OctalDigit  :  '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7';

fragment
Hexit   :  Digit | A | B | C | D | E | F;

fragment
Unsigned_Large_Integer
	:	;
fragment
Signed_Large_Integer
	:	;
fragment
Unsigned_Float
	:	;
fragment
Signed_Float
	:	;

//  Keyword Tokens

//  Reserved Keyword Tokens

	ADD                         :   A D D;
	ALL                         :   A L L;
	ALTER                       :   A L T E R;
	AND                         :   A N D;
	ANY                         :   A N Y;
	ARE                         :   A R E;
	AS                          :   A S;
	AT                          :   A T;
	BLOB                        :   B L O B;
	BOOLEAN                     :   B O O L E A N;
	BY                          :   B Y;
	CALL                        :   C A L L;
	CASCADED                    :   C A S C A D E D;
	CASE                        :   C A S E;
	CAST                        :   C A S T;
	CHARACTER                   :   C H A R A C T E R;
	CHAR                        :   C H A R;
	CHECK                       :   C H E C K;
	CLOB                        :   C L O B;
	CLOSE                       :   C L O S E;
	COLLATE                     :   C O L L A T E;
	COLUMN                      :   C O L U M N;
	COMMIT                      :   C O M M I T;
  CONNECTION                  :   C O N N E C T I O N;
  CONSTRAINT                  :   C O N S T R A I N T;
	CREATE                      :   C R E A T E;
	CROSS                       :   C R O S S;
	CURDATE						          :	  C U R D A T E;
	CURRENT_DATE                :   C U R R E N T Underscore D A T E;
	CURRENT_TIMESTAMP           :   C U R R E N T Underscore T I M E S T A M P;     
	CURTIME						          : 	C U R T I M E;
	CURRENT_TIME                :   C U R R E N T Underscore T I M E;
	CURRENT_USER                :   C U R R E N T Underscore U S E R;
	DATE                        :   D A T E;
	DAY                         :   D A Y;
	DEALLOCATE                  :   D E A L L O C A T E;
	DECIMAL                     :   D E C I M A L;
	DEC                         :   D E C;
	DEFAULT                     :   D E F A U L T;
	DELETE                      :   D E L E T E;
	DESCRIBE                    :   D E S C R I B E;
	DISTINCT                    :   D I S T I N C T;
	DOUBLE                      :   D O U B L E;
	DROP                        :   D R O P;
	DYNAMIC                     :   D Y N A M I C;
	ELSE                        :   E L S E;
	END                         :   E N D;
	ESCAPE                      :   E S C A P E;
	EXCEPT                      :   E X C E P T;
	EXECUTE                     :   E X E C U T E;
	EXISTS                      :   E X I S T S;
	EXTERNAL                    :   E X T E R N A L;
	FALSE                       :   F A L S E;
	FOREIGN                     :   F O R E I G N;
	FOR                         :   F O R;
	FROM                        :   F R O M;
	FULL                        :   F U L L;
	FUNCTION                    :   F U N C T I O N;
	GLOBAL                      :   G L O B A L;
	GRANT                       :   G R A N T;
	GROUP                       :   G R O U P;
	HAVING                      :   H A V I N G;
	HOUR                        :   H O U R;
	INNER                       :   I N N E R;
	INSERT                      :   I N S E R T;
	INTEGER                     :   I N T E G E R;
	INTERSECT                   :   I N T E R S E C T;
	INTERVAL                    :   I N T E R V A L;
	INTO                        :   I N T O;
	IN                          :   I N;
	ISOLATION                   :   I S O L A T I O N;
	IS                          :   I S;
	JOIN                        :   J O I N;
	KILL                        :   K I L L;
	LEFT                        :   L E F T;
	LIKE                        :   L I K E;
	LOCAL                       :   L O C A L;
	MATCH                       :   M A T C H;
	MERGE                       :   M E R G E;
	MINUTE                      :   M I N U T E;
	MONTH                       :   M O N T H;
	NATURAL                     :   N A T U R A L;
	NCHAR                       :   N C H A R;
	NCLOB                       :   N C L O B;
	NONE                        :   N O N E;
	NOT                         :   N O T;
	NO                          :   N O;
	NULL                        :   N U L L;
	NUMERIC                     :   N U M E R I C;
	OF                          :   O F;
	OLD                         :   O L D;
	ONLY                        :   O N L Y;
	ON                          :   O N;
	ORDER                       :   O R D E R;
	OR                          :   O R;
	OUTER                       :   O U T E R;
	OUT                         :   O U T;
	PARTITION                   :   P A R T I T I O N;
	PRECISION                   :   P R E C I S I O N;
	PREPARE                     :   P R E P A R E;
	PRIMARY                     :   P R I M A R Y;
	PROCEDURE                   :   P R O C E D U R E;
	RANGE                       :   R A N G E;
	REAL                        :   R E A L;
	REFERENCES                  :   R E F E R E N C E S;
	REF                         :   R E F;
	RELEASE                     :   R E L E A S E;
	RIGHT                       :   R I G H T;
	ROLLBACK                    :   R O L L B A C K;
	ROWS                        :   R O W S;
	ROW                         :   R O W;
	SAVEPOINT                   :   S A V E P O I N T;
	SCOPE                       :   S C O P E;
	SECOND                      :   S E C O N D;
	SELECT                      :   S E L E C T;
	SET                         :   S E T;
	SMALLINT                    :   S M A L L I N T;
	SOME                        :   S O M E;
	SPECIFIC                    :   S P E C I F I C;
	SQL                         :   S Q L;
	START                       :   S T A R T;
	STATIC                      :   S T A T I C;
	TABLE                       :   T A B L E;
	THEN                        :   T H E N;
	TIMESTAMP                   :   T I M E S T A M P;
	TIME                        :   T I M E;
	TO                          :   T O;
	TRIGGER                     :   T R I G G E R;
	TRUE                        :   T R U E;
	UNION                       :   U N I O N;
	UNIQUE                      :   U N I Q U E;
	UNKNOWN                     :   U N K N O W N;
	UPDATE                      :   U P D A T E;
	USER                        :   U S E R;
	USING                       :   U S I N G;
	VALUES                      :   V A L U E S;
	VARCHAR                     :   V A R C H A R;
	VARYING                     :   V A R Y I N G;
	WHEN                        :   W H E N;
	WHERE                       :   W H E R E;
	XOR                         :   X O R;
	YEAR                        :   Y E A R;


//  Non-Reserved Keyword Tokens

	ABS                         :   A B S;
	ACTION                      :   A C T I O N;
	AFTER                       :   A F T E R;
	ASC                         :   A S C;
	AUTO                        :   A U T O;
	AVG                         :   A V G;
	BEGIN	                      :   B E G I N;
	BETWEEN                     :   B E T W E E N;
	BIN                         :   B I N;
	CARDINALITY                 :   C A R D I N A L I T Y;
	CASCADE                     :   C A S C A D E;
	CATALOG                     :   C A T A L O G;
	CHARACTERS                  :   C H A R A C T E R S;
	COALESCE                    :   C O A L E S C E;
	COLLATION                   :   C O L L A T I O N;
	COMMITTED                   :   C O M M I T T E D;
	CONST                       :   C O N S T;
	CONVERT                     :   C O N V E R T;
	COUNT                       :   C O U N T;
	DATA                        :   D A T A;
	DAYS                        :   D A Y S;
	DEFINER                     :   D E F I N E R;
	DESC                        :   D E S C;
	EVENTS                      :   E V E N T S;
	EXP                         :   E X P;
	EXTERN                      :   E X T E R N;
	FIRST                       :   F I R S T;
	FLOAT                       :   F L O A T;
	HOURS                       :   H O U R S;
	INCREMENT                   :   I N C R E M E N T;
	INSTANCE                    :   I N S T A N C E;
	INTERSECTION                :   I N T E R S E C T I O N;
	INVOKER                     :   I N V O K E R;
	KEY                         :   K E Y;
	KIND                        :   K I N D;
	LENGTH                      :   L E N G T H;
	LEN                         :   L E N;
	LEVEL                       :   L E V E L;
	LONG                        :   L O N G;
	LOWER                       :   L O W E R;
	MAX                         :   M A X;
	MIN                         :   M I N;
	MOD                         :   M O D;
	NAMES                       :   N A M E S;
	NULLABLE                    :   N U L L A B L E;
	NULLIF                      :   N U L L I F;
	OPTION                      :   O P T I O N;
	PARTIAL                     :   P A R T I A L;
	PRIOR                       :   P R I O R;
	PRIVILEGES                  :   P R I V I L E G E S;
	QUERY                       :   Q U E R Y;
	READ                        :   R E A D;
	REPEATABLE                  :   R E P E A T A B L E;
	RESTRICT                    :   R E S T R I C T;
	SCHEMA                      :   S C H E M A;
	SECURITY                    :   S E C U R I T Y;
	SERIALIZABLE                :   S E R I A L I Z A B L E;
	SESSION                     :   S E S S I O N;
	SETS                        :   S E T S;
	SIGNED                      :   S I G N E D;
	SIGN                        :   S I G N;
	SIMPLE                      :   S I M P L E;
	SIZE                        :   S I Z E;
	STDDEV_POP                  :   S T D D E V Underscore P O P;
	STDDEV_SAMP                 :   S T D D E V Underscore S A M P;
	SUBSTRING                   :   S U B S T R I N G;
	SUM                         :   S U M;
	TEMPORARY                   :   T E M P O R A R Y;
	TRANSACTION                 :   T R A N S A C T I O N;
	TYPE                        :   T Y P E;
	UNCOMMITTED                 :   U N C O M M I T T E D;
	UNSIGNED                    :   U N S I G N E D;
	VAR_POP                     :   V A R Underscore P O P;
	VAR_SAMP                    :   V A R Underscore S A M P;
	VIEW                        :   V I E W;
	WORK                        :   W O R K;
	WRITE                       :   W R I T E;

// mysql specific stuff
  ALGORITHM                   :   A L G O R I T H M;
  ANALYZE                     :   A N A L Y Z E;
  AUTOINCREMENT               :   A U T O Underscore I N C R E M E N T;
  BEFORE                      :   B E F O R E;
  BINARY                      :   B I N A R Y;
  BIT_AND                     :   B I T Underscore A N D;
  BIT_OR                      :   B I T Underscore O R;
  BIT_XOR                     :   B I T Underscore X O R;
  BTREE                       :   B T R E E;
  CHANGE                      :   C H A N G E;
  CHARSET                     :   C H A R S E T;
  CHECKSUM                    :   C H E C K S U M;          
  COLUMNS                     :   C O L U M N S;
  CONCURRENT                  :   C O N C U R R E N T;
  CONSISTENT                  :   C O N S I S T E N T;
  DATABASES                   :   D A T A B A S E S;
  DATETIME                    :   D A T E T I M E;
  DAY_HOUR                    :   D A Y Underscore H O U R;
  DAY_MICROSECOND             :   D A Y Underscore M I C R O S E C O N D;
  DAY_MINUTE                  :   D A Y Underscore M I N U T E;
  DAY_SECOND                  :   D A Y Underscore S E C O N D;
  DELAYED                     :   D E L A Y E D;  
  DELAY_KEY_WRITE             :   D E L A Y Underscore K E Y Underscore W R I T E;
  DISABLE                     :   D I S A B L E;
  DUAL                        :   D U A L;
  DUPLICATE                   :   D U P L I C A T E;
  EACH                        :   E A C H;
  ENABLE                      :   E N A B L E;
  ENCLOSED                    :   E N C L O S E D;
  ENGINE                      :   E N G I N E;
  ENGINES                     :   E N G I N E S;
  ENUM                        :   E N U M;
  ERRORS                      :   E R R O R S;
  ESCAPED                     :   E S C A P E D;
  EXPLAIN                     :   E X P L A I N;
  FIELDS                      :   F I E L D S;
  FIELD_COMMENT               :   C O M M E N T;
  FLUSH                       :   F L U S H;
  FORCE                       :   F O R C E;
  FOUND_ROWS                  :   F O U N D Underscore R O W S;
  FULLTEXT                    :   F U L L T E X T;
  GRANTS                      :   G R A N T S;
  GROUP_CONCAT                :   G R O U P Underscore C O N C A T;
  HASH                        :   H A S H;
  HIGH_PRIORITY               :   H I G H Underscore P R I O R I T Y;
  HOUR_MICROSECOND            :   H O U R Underscore M I C R O S E C O N D;
  HOUR_MINUTE                 :   H O U R Underscore M I N U T E;
  HOUR_SECOND                 :   H O U R Underscore S E C O N D;
  IDENTIFIED                  :   I D E N T I F I E D;
  IF                          :   I F;
  IFNULL                      :   I F N U L L;
  IGNORE                      :   I G N O R E;
  INDEX                       :   I N D E X;
  INDEXES                     :   I N D E X E S;
  INFILE                      :   I N F I L E;
  INT                         :   I N T;
  KEYS                        :   K E Y S;
  LAST_INSERT_ID              :   L A S T Underscore I N S E R T Underscore I D;
  LESS                        :   L E S S;
  LIMIT                       :   L I M I T;
  LINEAR                      :   L I N E A R;
  LINES                       :   L I N E S;
  LIST                        :   L I S T;         
  LOAD                        :   L O A D;
  LOCK                        :   L O C K;
  LOGS                        :   L O G S;
  LOW_PRIORITY                :   L O W Underscore P R I O R I T Y;
  MAX_ROWS                    :   M A X Underscore R O W S;
  MEMORY                      :   M E M O R Y;
  MICROSECOND                 :   M I C R O S E C O N D;
  MINUTE_MICROSECOND          :   M I N U T E Underscore M I C R O S E C O N D;
  MINUTE_SECOND               :   M I N U T E Underscore S E C O N D;
  MODIFY                      :   M O D I F Y;
  NOW                         :   N O W;
  NO_WRITE_TO_BINLOG          :   N O Underscore W R I T E Underscore T O Underscore B I N L O G;
  OFFSET                      :   O F F S E T;
  ONE                         :   O N E;
  OPTIMIZE                    :   O P T I M I Z E;
  OPTIONALLY                  :   O P T I O N A L L Y;
  PARTITIONS                  :   P A R T I T I O N S;
  PASSWORD                    :   P A S S W O R D;
  PHASE                       :   P H A S E;
  PLUGINS                     :   P L U G I N S;
  PROCESS                     :   P R O C E S S;
  PROCESSLIST                 :   P R O C E S S L I S T;
  QUARTER                     :   Q U A R T E R;
  QUICK                       :   Q U I C K;
  RAND                        :   R A N D;
  RECOVER                     :   R E C O V E R;
  REGEXP                      :   R E G E X P;
  RENAME                      :   R E N A M E;
  REPLACE                     :   R E P L A C E;
  RLIKE                       :   R L I K E;
  ROW_FORMAT                  :   R O W Underscore F O R M A T;  
  RTREE                       :   R T R E E;
  SCHEMAS                     :   S C H E M A S;
  SECOND_MICROSECOND          :   S E C O N D Underscore M I C R O S E C O N D;
  SEPARATOR                   :   S E P A R A T O R;
  SERIAL                      :   S E R I A L;
  SHOW                        :   S H O W;
  SLAVE                       :   S L A V E;
  SNAPSHOT                    :   S N A P S H O T;
  SQL_BIG_RESULT              :   S Q L Underscore B I G Underscore R E S U L T;
  SQL_BUFFER_RESULT           :   S Q L Underscore B U F F E R Underscore R E S U L T;
  SQL_CACHE                   :   S Q L Underscore C A C H E;
  SQL_CALC_FOUND_ROWS         :   S Q L Underscore C A L C Underscore F O U N D Underscore R O W S;
  SQL_NO_CACHE                :   S Q L Underscore N O Underscore C A C H E; 
  SQL_NO_FCACHE               :   S Q L Underscore N O Underscore F C A C H E;
  SQL_SMALL_RESULT            :   S Q L Underscore S M A L L Underscore R E S U L T;
  STARTING                    :   S T A R T I N G;
  STATUS                      :   S T A T U S;
  STD                         :   S T D;
  STDDEV                      :   S T D D E V;
  STRAIGHT_JOIN               :   S T R A I G H T Underscore J O I N;
  TABLES                      :   T A B L E S;
  TEMPTABLE                   :   T E M P T A B L E;
  TERMINATED                  :   T E R M I N A T E D;
  THAN                        :   T H A N;
  TIMESTAMPDIFF               :   T I M E S T A M P D I F F;
  TRIGGERS                    :   T R I G G E R S;
  TRUNCATE                    :   T R U N C A T E;
  UNDEFINED                   :   U N D E F I N E D;
  UNIX_TIMESTAMP              :   U N I X Underscore T I M E S T A M P;
  UNLOCK                      :   U N L O C K;
  USE                         :   U S E;
  UTC_TIMESTAMP               :   U T C Underscore T I M E S T A M P;
  UUID                        :   U U I D;
  VARIABLES                   :   V A R I A B L E S;
  WARNINGS                    :   W A R N I N G S;
  WEEK                        :   W E E K;
  WITH                        :   W I T H;
  VARIANCE                    :   V A R I A N C E;
  XA                          :   X A;
  YEAR_MONTH	                :   Y E A R Underscore M O N T H;
  ZEROFILL                    :   Z E R O F I L L;
    
// stuff specific to dve
  ADAPTIVE                    :   A D A P T I V E;
  BROADCAST                   :   B R O A D C A S T;
  CACHE                       :   C A C H E;
  CONTAINER                   :   C O N T A I N E R;
  CONTAINER_TENANT            :   C O N T A I N E R Underscore T E N A N T;
  CONTAINER_TENANTS           :   C O N T A I N E R Underscore T E N A N T S;
  CONTAINERS                  :   C O N T A I N E R S;
  DATABASE                    :   D A T A B A S E;
  DISCRIMINATE                :   D I S C R I M I N A T E;
  DISTRIBUTE                  :   D I S T R I B U T E;
  DVE                         :   D V E;
  EMULATE                     :   E M U L A T E;
  ENABLED                     :   E N A B L E D;
  GENERATION                  :   G E N E R A T I O N;
  GENERATIONS                 :   G E N E R A T I O N S;
  GROUPS                      :   G R O U P S;
  INSTANCES                   :   I N S T A N C E S;
  JDBC                        :   J D B C;
  LOGGING                     :   L O G G I N G;
  MASTER                      :   M A S T E R;
  MASTERMASTER                :   M A S T E R M A S T E R;
  MODE                        :   M O D E;
  MULTITENANT                 :   M U L T I T E N A N T;
  OFF                         :   O F F;
  OPTIONAL                    :   O P T I O N A L;
  PERSISTENT                  :   P E R S I S T E N T;
  PLAN                        :   P L A N;
  PLANS                       :   P L A N S;
  POLICIES                    :   P O L I C I E S;
  POLICY                      :   P O L I C Y;
  POOL                        :   P O O L;
  PROVIDER                    :   P R O V I D E R;
  PROVIDERS                   :   P R O V I D E R S;
  RANDOM                      :   R A N D O M;
  RANGES                      :   R A N G E S;
  RAW                         :   R A W;
  REBALANCE                   :   R E B A L A N C E;
  RELOAD                      :   R E L O A D;
  RELAXED                     :   R E L A X E D;
  REQUIRED                    :   R E Q U I R E D;
  RESUME                      :   R E S U M E;
  SERVER                      :   S E R V E R;
  SERVERS                     :   S E R V E R S;
  SERVICE                     :   S E R V I C E;
  SERVICES                    :   S E R V I C E S;
  SINGLE                      :   S I N G L E;
  SITE                        :   S I T E;
  SITES                       :   S I T E S;
  STANDARD                    :   S T A N D A R D;
  STATISTICS                  :   S T A T I S T I C S;
  STOP                        :   S T O P;
  STORAGE                     :   S T O R A G E;
  STRICT                      :   S T R I C T;
  SUSPEND                     :   S U S P E N D;
  TEMPLATE                    :   T E M P L A T E;
  TEMPLATES                   :   T E M P L A T E S;
  TEMP                        :   T E M P;
  TENANT                      :   T E N A N T;
  TENANTS                     :   T E N A N T S;
  XML                         :   X M L;
  VARIABLE                    :   V A R I A B L E;

//  Punctuation and Arithmetic/Logical Operators

Not_Equals_Operator     
	:	 '<>' | '!=';
Greater_Or_Equals_Operator  
	:	 '>=';
Less_Or_Equals_Operator 
	:	 '<=';
Concatenation_Operator  
	:	 '||';
Right_Arrow             
	:	 '->';
Double_Colon            
	:	 '::';
Double_Period           
	:	 '..';
Back_Quote              
	:	 '`';
Tilde                   
	:	 '~';
Exclamation             
	:	 '!';
AT_Sign                 
	:	 '@';
Percent                 
	:	 '\%';
Circumflex              
	:	 '^';
Ampersand               
	:	 '&';
Double_Ampersand
  :  '&&';
Asterisk                
	:	 '*';
Left_Paren              
	:	 '(';
Right_Paren             
	:	 ')';
Plus_Sign               
	:	 '+';
Minus_Sign              
	:	 '-';
fragment
Hyphen              
	:	 '-';
Equals_Operator         
	:	 '=';
Left_Brace              
	:	 '{';
Right_Brace             
	:	 '}';
Left_Bracket            
	:	 '[';
Left_Bracket_Trigraph   
	:	 '??(';
Right_Bracket           
	:	 ']';
Right_Bracket_Trigraph  
	:	 '??)';
Vertical_Bar            
	:	 '|';
Colon                   
	:	 ':';
Semicolon               
	:	 ';';
Double_Quote            
	:	 '"';
Quote                   
	:	 '\'';
Less_Than_Operator      
	:	 '<';
Greater_Than_Operator   
	:	 '>';
Left_Shift_Operator
    :  '<<';
Right_Shift_Operator
    : '>>';
Comma                   
	:	 ',';
Period                  
	:	 '.';
Question_Mark           
	:	 '?';
Slash                   
	:	 '/';

fragment
Underscore : '_';
fragment
Hash_Sign : '#';
fragment
Dollar_Sign : '$';
fragment
Embedded_Space : ' ' | '\t';

// Unicode Character Ranges
//fragment
//Valid_Quoted_Identifier_ASCII_Character_Range :   
//    '\u0001' .. '\u0059'
//    // excluding backquote (`)
//    | '\u0061' .. '\u007F';
    
fragment
Valid_Identifier_Extended_Character_Range :   
    '\u0080' .. '\uFFFF';
    
fragment
Unicode_Character_Without_Single_Quotes :   
    '\u0000' .. '\u0026'
    | '\u0028' .. '\u009f'
    // escaped single quote (\')
    | '\u005c' '\u0027'
    // double escape (\\)
    | '\u005c' '\u005c'
    | '\u00A0' .. '\uFFFF'
    ;

fragment
Unicode_Character_Without_Double_Quotes :   
    '\u0000' .. '\u0021'
    | '\u0023' .. '\u009f'
    // escaped double quote (\")
    | '\u005c' '\u0022' 
    // double escape (\\)
    | '\u005c' '\u005c'
    | '\u00A0' .. '\uFFFF'
    ;

fragment
Control_Characters : '\u0001' .. '\u0009' | '\u000B' | '\u000C' | '\u000E' .. '\u001F';

fragment
Extended_Control_Characters : '\u0080' .. '\u009F';

fragment
HexPair : Hexit Hexit;

fragment
HexQuad : Hexit Hexit Hexit Hexit;

fragment
HexValue :
  ( '0'
      ( ( 'x' | 'X' ) 
        HexPair ( HexPair ( HexQuad (HexQuad HexQuad)? )? )? 
      | OctalDigit ( OctalDigit )* 
      | {true}? 
        ( '0' )*
      ))
  ;

fragment
Unsigned_Integer :
  HexValue
	|	( '1'..'9' ) ( Digit )* 
	;

fragment
Signed_Integer :
	( Plus_Sign | Minus_Sign ) ( Digit )+ 			
	;

fragment
Exponential_indicator:
  'f' | 'F' | 'd' | 'D' | 'e' | 'E'
  ;

Number :
  // if stuff after any leading sign but before any exponentiation has a decimal point, it is a floating pt value
  // so this includes n., n.n, .n, but not .
  ((ps=Plus_Sign | ms=Minus_Sign)?
   ((Digit+ (fp=Period Digit*)?) | (sp=Period Digit+)) 
   (e=Exponential_indicator (Plus_Sign | Minus_Sign)? Digit+)?
  {
    boolean isfp = !($fp == null && $sp == null);
    boolean signed = !($ps == null && $ms == null);
    _type = (isfp ? 
                  (signed ? Signed_Float : Unsigned_Float) : 
                          ($e == null ? (signed ? Signed_Integer : Unsigned_Integer) : 
                                      (signed ? Signed_Float : Unsigned_Float)));
  })
  | ( '0' 
      (('x' | 'X') Hexit+ { _type = Unsigned_Integer; }))   
  ;

fragment
Character_Set_Name  : ( ( ( Regular_Identifier )  Period )? 
                          ( Regular_Identifier )  Period )? 
                            Regular_Identifier ;

fragment
Character_Set	:	Underscore  Character_Set_Name;

fragment
Character_String_Literal :
  Character_Set? Unhinted_Character_String_Literal
  ;

fragment
Unhinted_Character_String_Literal :
		Quote (( Unicode_Character_Without_Single_Quotes  )* Quote ( Quote ( Unicode_Character_Without_Single_Quotes  )* Quote )*)
		| Double_Quote (( Unicode_Character_Without_Double_Quotes  )* Double_Quote ( Double_Quote ( Unicode_Character_Without_Double_Quotes  )* Double_Quote )*)
		;

fragment
National_Character_String_Literal :
    'N' Unhinted_Character_String_Literal
		;

fragment
Unicode_Character_String_Literal  :
		'U' Ampersand Quote ( Unicode_Character_Without_Single_Quotes  )* Quote ( Quote ( Unicode_Character_Without_Single_Quotes  )* Quote )*
		;

fragment
Single_Or_Double_Quote :  (Quote | Double_Quote);
		
fragment
Single_Or_Double_Or_Back_Quote :  (Single_Or_Double_Quote | Back_Quote);

fragment
Bit : ('0' | '1');

fragment
Bit_String_Literal  :
		B Quote Bit+ Quote ( Quote Bit+ Quote )*
		;

fragment
Hex_String_Literal :
  X Quote ( Hexit  Hexit  )* Quote ( Quote ( Hexit  Hexit  )* Quote )*
  ;

String_Literal	:
	(	Character_Set
		(
			Unicode_Character_String_Literal
				{
					_type  =  Unicode_Character_String_Literal;
				}
		|	Unhinted_Character_String_Literal
				{
					_type  =  Character_String_Literal;
				}
		)
	|	Bit_String_Literal
			{
				_type  =  Bit_String_Literal;
			}	
	|	Hex_String_Literal
			{
				_type  =  Hex_String_Literal;
			}	
	|	National_Character_String_Literal
			{
				_type  =  National_Character_String_Literal;
			}
	|	Unicode_Character_String_Literal
			{
				_type  =  Unicode_Character_String_Literal;
			}
	|	Unhinted_Character_String_Literal
			{
				_type  =  Character_String_Literal;
			}
	);

Regular_Identifier  :  MySQL_Identifier;

fragment
MySQL_Identifier  :  Back_Quote MySQL_Quoted_Identifier_body Back_Quote | MySQL_Identifier_body;
fragment
MySQL_Identifier_body  :  MySQL_Identifier_Start ( MySQL_Identifier_Part )*;
fragment
MySQL_Quoted_Identifier_body  :  Embedded_Space* MySQL_Quoted_Identifier_Start ( Embedded_Space | MySQL_Quoted_Identifier_Part )*;
fragment
MySQL_Quoted_Identifier_Start  :  MySQL_Identifier_Start | Valid_Identifier_Extended_Character_Range;  
fragment
MySQL_Identifier_Start  :  Latin_Letter | Digit | Underscore;
fragment
MySQL_Quoted_Identifier_Part  :  MySQL_Identifier_Part | Hyphen | Valid_Identifier_Extended_Character_Range;
fragment
MySQL_Identifier_Part  :  Latin_Letter | Digit | Underscore | Dollar_Sign;

// This compiles without errors and passes most of the tests,
// but fails on 'InsertIntoSelect.testPE1330', 'RandomCustomerTest.testPE842'
// and tests from 'com.tesora.dve.tools.aitemplatebuilder.BugTest'.
// Not sure why it has to explicitly name 'Underscore',
// but otherwise it does not parse names with '_'.
// Note: Unquote 'Valid_Quoted_Identifier_ASCII_Character_Range' fragment.
//fragment
//MySQL_Quoted_Identifier_body  :  MySQL_Quoted_Identifier_Start ( MySQL_Quoted_Identifier_Part )*;
//fragment
//MySQL_Quoted_Identifier_Start  :  Valid_Quoted_Identifier_ASCII_Character_Range | Valid_Identifier_Extended_Character_Range | Underscore;  
//fragment
//MySQL_Quoted_Identifier_Part  :  Valid_Quoted_Identifier_ASCII_Character_Range | Hyphen | Valid_Identifier_Extended_Character_Range | Underscore;


// W h i t e s p a c e   T o k e n s

// ignore all control characters and non-Ascii characters that are not enclosed in quotes

Space    :  (' ' | '\u000D' | '\u000A' | Carriage_Return_Literal )
{
	skip();
};

fragment
Carriage_Return_Literal : '\\' 'n';


White_Space  :	( Control_Characters  | Extended_Control_Characters | Carriage_Return_Literal )+ 
{
	skip();
};

Back_Quoted_String_Literal:
  Back_Quote (MySQL_Identifier_Part | Period | Percent)+ Back_Quote
  ;

BAD : . { error(UNKNOWN_CHARACTER, $text); skip(); } ;
