@skip whitespace {
    commands ::= scan | filter | aggregate;
    scan ::= 'SCAN' '(' words ')';
    filter ::= 'FILTER' '(' commands ',' number pred number ')';
    aggregate ::= 'AGGREGATE' '(' commands ',' number ',' aggregator ')';
}

words ::= ([A-Z] | [a-z] | [0-9] | [_.-])+;
number ::= [0-9]+;
pred ::= '=' | '>' | '<' | '<=' | '>=' | '!=' ;
aggregator ::= 'MIN' | 'MAX' | 'SUM' | 'AVG' | 'COUNT';
whitespace ::= [ \t\r\n]+;
