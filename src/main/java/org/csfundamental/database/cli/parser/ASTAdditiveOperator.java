/* Generated By:JJTree: Do not edit this line. ASTAdditiveOperator.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package org.csfundamental.database.cli.parser;

public
class ASTAdditiveOperator extends SimpleNode {
  public ASTAdditiveOperator(int id) {
    super(id);
  }

  public ASTAdditiveOperator(RookieParser p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {

    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=c0575e76306a51dc69faa3c857d33f36 (do not edit this line) */
