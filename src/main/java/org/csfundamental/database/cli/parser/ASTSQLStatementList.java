/* Generated By:JJTree: Do not edit this line. ASTSQLStatementList.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package org.csfundamental.database.cli.parser;

public
class ASTSQLStatementList extends SimpleNode {
  public ASTSQLStatementList(int id) {
    super(id);
  }

  public ASTSQLStatementList(RookieParser p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public void jjtAccept(RookieParserVisitor visitor, Object data) {

    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=06ea15cf3d56162c525663fb2dcc51a7 (do not edit this line) */
