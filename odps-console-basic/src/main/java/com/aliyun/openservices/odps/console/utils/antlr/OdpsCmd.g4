grammar OdpsCmd;

// This .g MUST cover case that cmd don't have a ';' at end
// Just adding a ';' at end CANNOT assure the cmd have a ';' when the last line is comment
cmds: cmd (semicolon cmd)* semicolon?;

// AntlrObject will use this rule to get each meaningful part of cmd
// only first 4 items will be reserved in AntlrObject
cmd: (id | string | leftparen | rightparen | comment | newline | space)*;

comment: COMMENTA | COMMENTB;

// # style comment can have 0/some space ahead in line
COMMENTA: {getCharPositionInLine() == 0}? [ \t]* '#' ~[\r\n]*;
// -- style comment can start at any position, to end of line
COMMENTB: '--' ~[\r\n]*;

string: STRING; // STRING must be a token. otherwise COMMENTB will eat the last ' in '--comment'
STRING: ( '\'' ( ~('\''|'\\') | ('\\' .) )* '\'' | '\"' ( ~('\"'|'\\') | ('\\' .) )* '\"' )+;

// COMMONCHAR can have '-' in it, but when the cmd have '--', COMMENTB will win the match as it's longer
COMMONCHAR: ~[ \t\n\r\f\"\'();];

// id should a rule. if it is ID(a token), 'a--b' will split to 'a-' '-b' by lexer
id: COMMONCHAR+;

semicolon: ';';
leftparen:  '(';
rightparen: ')';
newline: ('\r'?'\n' | '\r');
space: (' ' | '\t');

