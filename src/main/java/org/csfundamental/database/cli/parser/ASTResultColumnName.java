/* Generated By:JJTree: Do not edit this line. ASTResultColumnName.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package org.csfundamental.database.cli.parser;

public
class ASTResultColumnName extends SimpleNode {
  public ASTResultColumnName(int id) {
    super(id);
  }

  public ASTResultColumnName(RookieParser p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {

    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=8139faf18e8522b9247df1b0d9a95617 (do not edit this line) */