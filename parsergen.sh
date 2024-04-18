#! /bin/bash
rm -rf src/main/java/org/csfundamental/database/cli/parser
jjtree RookieParser.jjt
javacc src/main/java/org/csfundamental/database/cli/parser/RookieParser.jj
rm -f src/main/java/org/csfundamental/database/cli/parser/RookieParser.jj
