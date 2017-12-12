@skip whitespace {
    commands ::= scan | filter | aggregate | join | hash_join;
    scan ::= 'SCAN' '(' words ')';
    filter ::= 'FILTER' '(' commands ',' number pred number ')';
    aggregate ::= 'AGGREGATE' '(' commands ',' number ',' aggregator ')';
    join ::= 'JOIN' '(' commands ',' commands ',' number pred number ')';
    hash_join ::= 'HASH_JOIN' '(' commands ',' commands ',' number pred number ')';
}

words ::= ([A-Z] | [a-z] | [0-9] | [_.-])+;
number ::= [0-9]+;
pred ::= '=' | '>' | '<' | '<=' | '>=' | '!=' ;
aggregator ::= 'MIN' | 'MAX' | 'SUM' | 'AVG' | 'COUNT';
whitespace ::= [ \t\r\n]+;
