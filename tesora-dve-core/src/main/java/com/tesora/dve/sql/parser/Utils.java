package com.tesora.dve.sql.parser;

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


import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.antlr.runtime.CommonToken;
import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.BaseTreeAdaptor;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.TreeAdaptor;
import org.antlr.runtime.tree.TreeParser;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.sql.ParserException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.DelegatingLiteralExpression;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.util.Pair;

public class Utils {
	
	private List<Pair<Pass,String>> messages;
	private ParserOptions options;
	
	public Utils(ParserOptions opts) {
		reset();
		this.options = opts;
		if (this.options == null)
			this.options = ParserOptions.NONE;
	}
	
	public void reset() {
		messages = new ArrayList<Pair<Pass,String>>();
	}
	
    public void error(Pass component, String description, String text) {
    	throw new ParserException(component, "error: " + description + " on '" + text + "'");
    }
    
    public void collectError(Pass component, String message) {
    	if (options.isFailEarly())
    		throw new ParserException(component, message);
    	else
    		messages.add(new Pair<Pass, String>(component,message));
    }
    
    public List<Pair<Pass,String>> getErrors() {
    	return messages;
    }
        
    public ParserException buildError() {
    	if (messages.isEmpty())
    		return null;
    	StringWriter sw = new StringWriter();
    	PrintWriter pw = new PrintWriter(sw);
    	pw.println("Parsing FAILED:");
    	Pass firstSeenPass = null;
    	for(Pair<Pass,String> s : messages) {
    		pw.println("Error from " + s.getFirst().toString() + ": " + s.getSecond());
    		if (firstSeenPass == null)
    			firstSeenPass = s.getFirst();
    	}
    	pw.close();
    	return new ParserException(firstSeenPass,sw.toString());
    }
    
    public String displayTree(String context, Object o) {
    	if (context != null)
    		return (context + ": '" + ((CommonTree)o).toStringTree() + "'");
    	else
    		return ((CommonTree)o).toStringTree();
    }

    public ParserOptions getOptions() {
    	return options;
    }

    public Object buildKeywordEscapeIdentifier(TreeAdaptor adaptor, Object tree) {
    	if (tree instanceof CommonToken) {
    		Token tok = (Token) tree;
        	return adaptor.create(TokenTypes.Regular_Identifier, tok, tok.getText());    		
    	} else {
    		CommonTree ct = (CommonTree)tree;
    		return adaptor.create(TokenTypes.Regular_Identifier, ct.getToken(), ct.getText());
    	}
    }
    
    public Object buildKeywordEscapeIdentifier(Parser parser, TreeAdaptor adaptor, Object tree) {
    	CommonTree ct = (CommonTree)tree;
    	String txt = ct.getText();
    	if (txt == null) {
    		CommonToken leftToken = (CommonToken) parser.getTokenStream().get(ct.getTokenStartIndex());
    		CommonToken rightToken = (CommonToken) parser.getTokenStream().get(ct.getTokenStopIndex());
    		txt = leftToken.getInputStream().substring(leftToken.getStartIndex(), rightToken.getStopIndex());
    	}

    	return adaptor.create(TokenTypes.Regular_Identifier, ct.getToken(), /*ct.getText()*/txt);
    }
    
    public Object buildIdentifier(Parser parser, TreeAdaptor adaptor, String text) {
    	return adaptor.create(TokenTypes.Regular_Identifier, text);
    }
    
    public Object buildJDBCUrl(TreeAdaptor adaptor, String dbType, String hostname, String port, String dbname) {
    	StringBuilder buf = new StringBuilder();
    	buf.append("'");
    	buf.append("jdbc:");
    	if (dbType == null) 
    		throw new ParserException(Pass.FIRST, "Malformed jdbc url: missing dbtype");
    	buf.append(dbType).append("://");
    	if (hostname == null)
    		throw new ParserException(Pass.FIRST, "Malformed jdbc url: missing host");
    	buf.append(hostname);
    	if (port != null) 
    		buf.append(":").append(port);
    	if (dbname != null)
    		buf.append("/").append(dbname);
    	buf.append("'");
    	return adaptor.create(TokenTypes.Character_String_Literal, buf.toString());
    }
    
    public String massageHostSpec(String in) {
    	if (in.length() > 0 && in.charAt(0) == '`' && in.charAt(in.length() - 1) == '`')
    		return "'" + in.substring(1,in.length() - 1) + "'";
    	return in;
    }
    
    public String extractLine(Parser parser, RecognitionException e) {
    	return extractLine(parser.getTokenStream(), e);
    }

    public String extractLine(TreeParser parser, RecognitionException e) {
    	return extractLine(parser.getTreeNodeStream().getTokenStream(), e);
    }
    
    private String extractLine(TokenStream input, RecognitionException e) {
    	int line = e.line;
    	// select tokens on the same line
    	boolean done =false;
    	int position = -1;
    	StringBuilder buf = new StringBuilder();
    	while(!done) {
    		position++;
    		try {
    			Token tok = input.get(position);
    			if (tok.getLine() < line)
    				continue;
    			if (tok.getLine() == line) {
    				buf.append(tok.getText());
    				buf.append(" ");
    			} else if (tok.getLine() > line)
    				done = true;
    		} catch (NoSuchElementException nsee) {
    			done = true;
    		}
    	}    	
    	return buf.toString();	
    }
    
    public String formatErrorMessage(@SuppressWarnings("rawtypes") List stack, String msg, String line) {
        // return "from line '" + onLine + "'" + lf + msg + lf +stackMsg;
    	StringBuffer stackBuf = new StringBuffer();
    	for(ListIterator<?> iter = stack.listIterator(stack.size() - 1); iter.hasPrevious();) {
    		stackBuf.append(iter.previous()).append(PEConstants.LINE_SEPARATOR);
    	}
    	return "from line '" + line + "'" + PEConstants.LINE_SEPARATOR + msg + PEConstants.LINE_SEPARATOR + stackBuf;
    }

    private static int getOperatorPrecedence(int tok) {
    	Integer out = precedenceMap.get(tok);
    	if (out == null)
    		throw new ParserException(Pass.FIRST,"Unknown operator type: " + tok);
    	return out.intValue();
    }
    
    // i.e., find the operator with least precedence
	private static int findPivot(List<Token> operators, int startIndex, int stopIndex) {
    	int pivot = startIndex;
    	Token ct = operators.get(pivot);
    	int pivotRank = getOperatorPrecedence( ct.getType() );
    	for(int i = startIndex + 1; i <= stopIndex; i++) {
    		int type = operators.get(i).getType();
    		int current = getOperatorPrecedence(type);
    		if (current >= pivotRank) {
    			pivot = i;
    			pivotRank = current;
    		}
    	}
    	return pivot;
    }
    
    // sam harwell's trick for binary operators
	private static ExpressionNode createPrecedenceTree(TranslatorUtils me, TreeAdaptor adaptor, List<ExpressionNode> expressions, List<Token> operators, int startIndex, int stopIndex) {
    	if (stopIndex == startIndex) {
    		ExpressionNode out = expressions.get(startIndex);
    		if (out instanceof ExpressionNodeList)
    			throw new IllegalStateException("unhandled fake expression");
    		return out;
    	}
    	int pivot = findPivot(operators, startIndex, stopIndex - 1);
    	
    	Token opToken = operators.get(pivot);
    	
    	FunctionName fn = new FunctionName(opToken.getText(),opToken.getType(),true);

    	if (specialOperators.contains(opToken.getType())) {
    		if (pivot+1 == stopIndex) {
    			// unpack the rhs
    			ExpressionNode en = expressions.get(stopIndex);
    			ArrayList<ExpressionNode> allParams = new ArrayList<ExpressionNode>();
    			allParams.add(createPrecedenceTree(me,adaptor,expressions,operators,startIndex,pivot));
    			if (en instanceof ExpressionNodeList) {
    				ExpressionNodeList enl = (ExpressionNodeList) en;
    				allParams.addAll(enl.args);
    			} else {
    				allParams.add(en);
    			}
    			return me.buildFunctionCall(fn, allParams, null, null);
    		}
    	}

    	return me.buildFunctionCall(fn, Arrays.asList(new ExpressionNode[] {
    			createPrecedenceTree(me,adaptor,expressions,operators,startIndex,pivot),
    			createPrecedenceTree(me,adaptor,expressions,operators,pivot+1,stopIndex) }), null, null);
    }

    
    public Object makeTinyTree(TreeAdaptor adaptor, Token ct) {
    	return adaptor.create(ct);
    }
    
	public ExpressionNode createPrecedenceTree(TranslatorUtils me, TreeAdaptor adaptor, List<ExpressionNode> expressions, List<Token> operators) {
    	return createPrecedenceTree(me, adaptor, expressions, operators, 0, expressions.size() - 1);
    }    
    
    
    private static final Map<Integer,Integer> precedenceMap = buildPrecedenceMap();
    
    private static Map<Integer, Integer> buildPrecedenceMap() {
    	HashMap<Integer, Integer> out = new HashMap<Integer,Integer>();
    	int[][] levels = new int[][] {
    			new int[] { TokenTypes.Asterisk, TokenTypes.Slash, TokenTypes.Percent },
    			new int[] { TokenTypes.Minus_Sign, TokenTypes.Plus_Sign },
    			new int[] { TokenTypes.Ampersand },
    			new int[] { TokenTypes.Vertical_Bar },
    			new int[] { TokenTypes.Equals_Operator, TokenTypes.Not_Equals_Operator, TokenTypes.Less_Than_Operator, 
    					    TokenTypes.Greater_Than_Operator, TokenTypes.Less_Or_Equals_Operator, 
    					    TokenTypes.Greater_Or_Equals_Operator, TokenTypes.IN, TokenTypes.LIKE, TokenTypes.IS,
    					    TokenTypes.BETWEEN,
    					    TokenTypes.NOTIS, TokenTypes.NOTLIKE, TokenTypes.NOTIN, TokenTypes.NOTBETWEEN },
    			new int[] { TokenTypes.NOT },
    			new int[] { TokenTypes.AND, TokenTypes.Double_Ampersand },
				new int[] { TokenTypes.XOR },
    			new int[] { TokenTypes.OR, TokenTypes.Concatenation_Operator },
    			new int[] { TokenTypes.REGEXP, TokenTypes.RLIKE }
    	};
    	for(int i = 0; i < levels.length; i++) {
    		int prec = i + 1;
    		int[] same = levels[i];
    		for(int o = 0; o < same.length; o++)
    			out.put(same[o],prec);
    	}
    	return out;
    }
    
    private static final Set<Integer> specialOperators = new HashSet<Integer>(Arrays.asList(new Integer[] {
    		TokenTypes.IS, TokenTypes.BETWEEN, TokenTypes.LIKE, TokenTypes.IN,
    		TokenTypes.NOTIS, TokenTypes.NOTBETWEEN, TokenTypes.NOTLIKE, TokenTypes.NOTIN
    }));
    
    public void collectSpecialNottable(TreeAdaptor adaptor, Object mainOp, List<ExpressionNode> params, 
    		Object notOp, List<ExpressionNode> exprs, List<Token> opNodes) {
    	BaseTreeAdaptor bta = (BaseTreeAdaptor) adaptor;
    	CommonTree opNode = (CommonTree) mainOp;
    	CommonTree notNode = (CommonTree) notOp;
    	CommonToken opToken = null;
    	if (notNode != null) {
    		int newType = findNottableConversion(opNode);
    		opToken = (CommonToken) bta.createToken(newType, opNode.getText());
    	} else {
    		opToken = (CommonToken) opNode.getToken();
    	}
    	opNodes.add(opToken);
    	exprs.add(new ExpressionNodeList(params)); 
    }

	public ExpressionNode buildPredicate(TreeAdaptor ta, ExpressionNode lhs,
			Token not, 
			Token in, List<ExpressionNode> inParams,
			Token between, ExpressionNode blhs, ExpressionNode brhs,
			Token like, List<ExpressionNode> likeParams,
			Token re, Token rl, ExpressionNode reParams,
			Object relOp, ExpressionNode relOpParam) {
		if (in == null && between == null && like == null && re == null && rl == null && relOp == null)
			return lhs;
		BaseTreeAdaptor bta = (BaseTreeAdaptor) ta;
		List<ExpressionNode> params = new ArrayList<ExpressionNode>();
		params.add(lhs);
		Token main = null;
		if (in != null) {
			main = in;
			params.addAll(inParams);
		} else if (between != null) {
			params.add(blhs);
			params.add(brhs);
			main = between;
		} else if (like != null) {
			params.addAll(likeParams);
			main = like;
		} else if (re != null) {
			params.add(reParams);
			main = re;
		} else if (rl != null) {
			params.add(reParams);
			main = rl;
		} else if (relOp != null) {
			if (relOp instanceof CommonTree) {
				main = ((CommonTree)relOp).getToken();
			} else if (relOp instanceof Token) {
				main = (Token) relOp;
			}
			params.add(relOpParam);
		}
		if (not != null) {
			int newType = findNottableConversion(main);
			main = bta.createToken(newType, main.getText());
		}
		FunctionName fn = new FunctionName(main.getText(),main.getType(),true);
		FunctionCall fc = new FunctionCall(fn,params);
		return fc;
	}
	
	public ExpressionNode buildBooleanExpr(TreeAdaptor ta, Token ln, ExpressionNode predicate, Token is, Token not, List<ExpressionNode> is_parameters) {
		ExpressionNode out = predicate;
		if (is != null) {
			BaseTreeAdaptor bta = (BaseTreeAdaptor) ta;
			List<ExpressionNode> params = new ArrayList<ExpressionNode>();
			params.add(predicate);
			Token ftok = is;
			if (not != null) {
				int newType = findNottableConversion(ftok);
				ftok = bta.createToken(newType,ftok.getText());
			}
			params.addAll(is_parameters);
			out = new FunctionCall(new FunctionName(ftok.getText(), ftok.getType(), true),params);
		}
		if (ln != null) {
			out = new FunctionCall(new FunctionName(ln.getText(), ln.getType(), true), out);
		}
		return out;
	}

    
    public static int findNottableConversion(Object in) {
    	int orig = -1;
    	if (in instanceof Token) {
    		orig = ((Token)in).getType();
    	} else if (in instanceof CommonTree) {
    		orig = ((CommonTree)in).getType();
    	} else {
    		throw new ParserException(Pass.FIRST, "Unknown node implementation: " + in.getClass().getSimpleName());
    	}
    	switch (orig) {
    	case TokenTypes.BETWEEN:
    		return TokenTypes.NOTBETWEEN;
    	case TokenTypes.IN:
    		return TokenTypes.NOTIN;
    	case TokenTypes.IS:
    		return TokenTypes.NOTIS;
    	case TokenTypes.LIKE:
    		return TokenTypes.NOTLIKE;
    	default:
    		throw new ParserException(Pass.FIRST, "Unknown special not case: " + in);
    	}
    }
    
    public FunctionName buildNottableFunction(TreeAdaptor adaptor, Token in) {
    	BaseTreeAdaptor bta = (BaseTreeAdaptor) adaptor;
    	int nt = findNottableConversion(in);
    	Token newTok = bta.createToken(nt,in.getText());
    	TranslatorUtils me = (TranslatorUtils) this;
    	return me.buildFunctionName(newTok, false);
    }
    
    
    public void unsupported(String description) {
    	throw new SchemaException(Pass.FIRST, "Unsupported: " + description);
    }
    
    public void updateSourcePosition(Object obj, Token lhs, Token rhs) {
    	if (!(obj instanceof ExpressionNode)) return;
    	ExpressionNode en = (ExpressionNode) obj;
    	int type = 0;
    	if (en.getSourceLocation() != null)
    		type = en.getSourceLocation().getType();
    	TranslatorUtils me = (TranslatorUtils) this;
    	String origStmt = me.getInputSQL();
    	// doesn't matter what kind of sloc we have, we're switching to computed now
    	int l = lhs.getCharPositionInLine();
    	char first = origStmt.charAt(l);
    	if (first == ',') l++;
    	String text = null;
    	if (rhs.getLine() != lhs.getLine())
    		text = origStmt.substring(l).trim();
    	else
    		text = origStmt.substring(l, rhs.getCharPositionInLine()).trim();
    	en.setSourceLocation(new ComputedSourceLocation(l,lhs.getLine(),text,type));
    }
        
    private static class ExpressionNodeList extends ExpressionNode {

    	List<ExpressionNode> args;
    	
    	protected ExpressionNodeList(List<ExpressionNode> params) {
    		super((SourceLocation)null);
    		args = params;
    	}
    	
		@Override
		protected LanguageNode copySelf(CopyContext cc) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		protected boolean schemaSelfEqual(LanguageNode other) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		protected int selfHashCode() {
			// TODO Auto-generated method stub
			return 0;
		}

    }
    
    public PrecedenceCollector buildPrecedenceCollector(TreeAdaptor ta) {
    	return new PrecedenceCollector(this,ta);
    }
    
    public static class PrecedenceCollector {
    	
        List<ExpressionNode> expressions = new ArrayList<ExpressionNode>();
        List<Token> operators = new ArrayList<Token>();
        TranslatorUtils utils;
        TreeAdaptor adaptor;
        
        public PrecedenceCollector(Utils tu, TreeAdaptor adaptor) {
        	utils = (TranslatorUtils) tu;
        	this.adaptor = adaptor;
        	
        }
        
        public void addExpr(ExpressionNode e) {
        	expressions.add(e);
        }
    	
        public void addOp(Object o) {
        	if (o instanceof CommonTree) {
        		CommonTree ct = (CommonTree) o;
        		operators.add(ct.getToken());
        	} else if (o instanceof Token) {
        		operators.add((Token)o);
        	}
        }
        
        public ExpressionNode build() {
        	return createPrecedenceTree(utils, adaptor, expressions, operators, 0, expressions.size() - 1);
        }
        
    	public void collectSignedNumericLiteral(Object intree) {
        	BaseTreeAdaptor bta = (BaseTreeAdaptor) adaptor;
        	LiteralExpression litex = (LiteralExpression) intree;
        	int tokType = litex.getSourceLocation().getType();
        	String text = litex.getSourceLocation().getText();
        	char leading = text.charAt(0);
        	int binTokType = -1;
        	if (leading == '+') {
        		binTokType = TokenTypes.Plus_Sign;
        	} else if (leading == '-') {
        		binTokType = TokenTypes.Minus_Sign;
        	} else {
        		throw new ParserException(Pass.FIRST, "Unexpected unary signed numeric kind: '" + leading + "'");
        	}
        	operators.add(bta.createToken(binTokType, text.substring(0, 1)));
        	// we need to convert the token type of the rhs
        	int newTokType = -1;
        	switch(tokType) {
        	case TokenTypes.Signed_Float:
        		newTokType = TokenTypes.Unsigned_Float;
        		break;
        	case TokenTypes.Signed_Integer:
        		newTokType = TokenTypes.Unsigned_Integer;
        		break;
        	case TokenTypes.Signed_Large_Integer:
        		newTokType = TokenTypes.Unsigned_Large_Integer;
        		break;
        	default:
        		throw new ParserException(Pass.FIRST, "Cannot convert signed numeric type " + tokType + " to unsigned numeric type");
        	}
        	Token ntok = bta.createToken(newTokType, text.substring(1));
        	ntok.setLine(litex.getSourceLocation().getLineNumber());
        	ntok.setCharPositionInLine(litex.getSourceLocation().getPositionInLine());

        	ExpressionNode newlit = null;
        	
        	if (litex instanceof DelegatingLiteralExpression) {
        		DelegatingLiteralExpression odle = (DelegatingLiteralExpression) litex;
                DelegatingLiteralExpression dle =
            			utils.replaceLiteral(odle, newTokType, SourceLocation.make(ntok), Singletons.require(HostService.class).getDBNative().getValueConverter().convertLiteral(ntok.getText(), newTokType));
            	newlit = dle;
        	} else {
        		newlit = utils.buildLiteral(ntok);
        	}
        	
        	expressions.add(newlit);
        }

    }
}

    
