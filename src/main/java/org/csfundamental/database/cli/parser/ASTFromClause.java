/* Generated By:JJTree: Do not edit this line. ASTFromClause.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package org.csfundamental.database.cli.parser;

public
class ASTFromClause extends SimpleNode {
  public ASTFromClause(int id) {
    super(id);
  }

  public ASTFromClause(RookieParser p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {

    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=c645f719f401158c9dc674ecb9339029 (do not edit this line) */
