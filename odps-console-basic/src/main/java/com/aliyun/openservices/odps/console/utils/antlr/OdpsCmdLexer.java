// Generated from OdpsCmd.g4 by ANTLR 4.3
package com.aliyun.openservices.odps.console.utils.antlr;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class OdpsCmdLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__6=1, T__5=2, T__4=3, T__3=4, T__2=5, T__1=6, T__0=7, COMMENTA=8, COMMENTB=9, 
		STRING=10, COMMONCHAR=11;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] tokenNames = {
		"'\\u0000'", "'\\u0001'", "'\\u0002'", "'\\u0003'", "'\\u0004'", "'\\u0005'", 
		"'\\u0006'", "'\\u0007'", "'\b'", "'\t'", "'\n'", "'\\u000B'"
	};
	public static final String[] ruleNames = {
		"T__6", "T__5", "T__4", "T__3", "T__2", "T__1", "T__0", "COMMENTA", "COMMENTB", 
		"STRING", "COMMONCHAR"
	};


	public OdpsCmdLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "OdpsCmd.g4"; }

	@Override
	public String[] getTokenNames() { return tokenNames; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	@Override
	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 7: return COMMENTA_sempred((RuleContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean COMMENTA_sempred(RuleContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0: return getCharPositionInLine() == 0;
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\rX\b\1\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3"+
		"\t\3\t\7\t*\n\t\f\t\16\t-\13\t\3\t\3\t\7\t\61\n\t\f\t\16\t\64\13\t\3\n"+
		"\3\n\3\n\3\n\7\n:\n\n\f\n\16\n=\13\n\3\13\3\13\3\13\3\13\7\13C\n\13\f"+
		"\13\16\13F\13\13\3\13\3\13\3\13\3\13\3\13\7\13M\n\13\f\13\16\13P\13\13"+
		"\3\13\6\13S\n\13\r\13\16\13T\3\f\3\f\2\2\r\3\3\5\4\7\5\t\6\13\7\r\b\17"+
		"\t\21\n\23\13\25\f\27\r\3\2\7\4\2\13\13\"\"\4\2\f\f\17\17\4\2))^^\4\2"+
		"$$^^\b\2\13\f\16\17\"\"$$)+==`\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t"+
		"\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2"+
		"\2\2\25\3\2\2\2\2\27\3\2\2\2\3\31\3\2\2\2\5\33\3\2\2\2\7\35\3\2\2\2\t"+
		"\37\3\2\2\2\13!\3\2\2\2\r#\3\2\2\2\17%\3\2\2\2\21\'\3\2\2\2\23\65\3\2"+
		"\2\2\25R\3\2\2\2\27V\3\2\2\2\31\32\7\"\2\2\32\4\3\2\2\2\33\34\7\f\2\2"+
		"\34\6\3\2\2\2\35\36\7\13\2\2\36\b\3\2\2\2\37 \7\17\2\2 \n\3\2\2\2!\"\7"+
		"*\2\2\"\f\3\2\2\2#$\7+\2\2$\16\3\2\2\2%&\7=\2\2&\20\3\2\2\2\'+\6\t\2\2"+
		"(*\t\2\2\2)(\3\2\2\2*-\3\2\2\2+)\3\2\2\2+,\3\2\2\2,.\3\2\2\2-+\3\2\2\2"+
		".\62\7%\2\2/\61\n\3\2\2\60/\3\2\2\2\61\64\3\2\2\2\62\60\3\2\2\2\62\63"+
		"\3\2\2\2\63\22\3\2\2\2\64\62\3\2\2\2\65\66\7/\2\2\66\67\7/\2\2\67;\3\2"+
		"\2\28:\n\3\2\298\3\2\2\2:=\3\2\2\2;9\3\2\2\2;<\3\2\2\2<\24\3\2\2\2=;\3"+
		"\2\2\2>D\7)\2\2?C\n\4\2\2@A\7^\2\2AC\13\2\2\2B?\3\2\2\2B@\3\2\2\2CF\3"+
		"\2\2\2DB\3\2\2\2DE\3\2\2\2EG\3\2\2\2FD\3\2\2\2GS\7)\2\2HN\7$\2\2IM\n\5"+
		"\2\2JK\7^\2\2KM\13\2\2\2LI\3\2\2\2LJ\3\2\2\2MP\3\2\2\2NL\3\2\2\2NO\3\2"+
		"\2\2OQ\3\2\2\2PN\3\2\2\2QS\7$\2\2R>\3\2\2\2RH\3\2\2\2ST\3\2\2\2TR\3\2"+
		"\2\2TU\3\2\2\2U\26\3\2\2\2VW\n\6\2\2W\30\3\2\2\2\f\2+\62;BDLNRT\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}