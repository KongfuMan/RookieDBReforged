/* Generated By:JJTree: Do not edit this line. ASTNotExpression.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package org.csfundamental.database.cli.parser;

public
class ASTNotExpression extends SimpleNode {
  public ASTNotExpression(int id) {
    super(id);
  }

  public ASTNotExpression(RookieParser p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {

    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=47185d4c6ec6f0e70cea08bb2c3f3ade (do not edit this line) */
