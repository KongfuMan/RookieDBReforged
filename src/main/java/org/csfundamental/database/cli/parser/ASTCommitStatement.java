/* Generated By:JJTree: Do not edit this line. ASTCommitStatement.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package org.csfundamental.database.cli.parser;

public
class ASTCommitStatement extends SimpleNode {
  public ASTCommitStatement(int id) {
    super(id);
  }

  public ASTCommitStatement(RookieParser p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {

    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=10a7359942c5f75cf66523440becba28 (do not edit this line) */