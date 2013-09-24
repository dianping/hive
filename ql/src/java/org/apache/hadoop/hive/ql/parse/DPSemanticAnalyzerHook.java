package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.shims.ShimLoader;

public class DPSemanticAnalyzerHook extends AbstractSemanticAnalyzerHook {
  private final SessionState ss = SessionState.get();
  private Hive hive = null;
  private String hql = "";
  private String onOrWhereHql = "";
  private String currentDatabase = "default";
  private String tableAlias = "";
  private String tableName = "";
  private String tableDatabase = "";
  private Boolean needCheckPartition = false;
  private static List<String> noNeedCheckKeywords = new ArrayList<String>();

  static {
    // noNeedCheckKeywords.add("insert");
    // noNeedCheckKeywords.add("create");
  }

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
      throws SemanticException {
    try {
      LogHelper console = SessionState.getConsole();
      hql = ss.getCmd().toLowerCase();
      String username = ShimLoader.getHadoopShims().getUserName(context.getConf());

      for (String word : noNeedCheckKeywords) {
        if (hql.contains(word)) {
          return ast;
        }
      }

      if (hql.contains("on")) {
        onOrWhereHql = hql.substring(hql.indexOf("on"));
      } else if (hql.contains(" where ")) {
        onOrWhereHql = hql.substring(hql.indexOf("where"));
      }

      if (ast.getToken().getType() == HiveParser.TOK_QUERY) {
        try {
          hive = context.getHive();
          currentDatabase = hive.getCurrentDatabase();
        } catch (HiveException e) {
          throw new SemanticException(e);
        }

        extractFromClause((ASTNode) ast.getChild(0));

        if (needCheckPartition && !StringUtils.isBlank(tableName)) {
          String dbname = StringUtils.isEmpty(tableDatabase) ? currentDatabase : tableDatabase;
          String tbname = tableName;
          String[] parts = tableName.split(".");
          if (parts.length == 2) {
            dbname = parts[0];
            tbname = parts[1];
          }
          Table t = hive.getTable(dbname, tbname);
          if (t.isPartitioned()) {
            if (StringUtils.isBlank(onOrWhereHql)) {
              console.printError("Not Specify where or on clause in HQL:" + hql + " username:"
                  + username);
            } else {
              List<FieldSchema> partitionKeys = t.getPartitionKeys();
              List<String> partitionNames = new ArrayList<String>();
              for (int i = 0; i < partitionKeys.size(); i++) {
                partitionNames.add(partitionKeys.get(i).getName().toLowerCase());
              }

              if (!containsPartCond(partitionNames, onOrWhereHql, tableAlias)) {
                console
                    .printError("Hql is not efficient, Please specify partition condition! HQL:"
                        + hql + " username:" + username);
              }
            }
          }
        }

      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return ast;
  }

  private boolean containsPartCond(List<String> partitionKeys, String sql, String alias) {
    for (String pk : partitionKeys) {
      if (sql.contains(pk)) {
        return true;
      }
      if (!StringUtils.isEmpty(alias) && sql.contains(alias + "." + pk)) {
        return true;
      }
    }
    return false;
  }

  private void extractFromClause(ASTNode ast) {
    if (HiveParser.TOK_FROM == ast.getToken().getType()) {
      ASTNode refNode = (ASTNode) ast.getChild(0);
      if (refNode.getToken().getType() == HiveParser.TOK_TABREF && ast.getChildCount() == 1) {
        ASTNode tabNameNode = (ASTNode) (refNode.getChild(0));
        int refNodeChildCount = refNode.getChildCount();
        if (tabNameNode.getToken().getType() == HiveParser.TOK_TABNAME) {
          if (tabNameNode.getChildCount() == 2) {
            tableDatabase = tabNameNode.getChild(0).getText().toLowerCase();
            tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabNameNode.getChild(1))
                .toLowerCase();
          } else if (tabNameNode.getChildCount() == 1) {
            tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabNameNode.getChild(0))
                .toLowerCase();
          } else {
            return;
          }

          if (refNodeChildCount == 2) {
            tableAlias = BaseSemanticAnalyzer.unescapeIdentifier(refNode.getChild(1).getText())
                .toLowerCase();
          }
          needCheckPartition = true;
        }
      }
    }
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
      List<Task<? extends Serializable>> rootTasks) throws SemanticException {
    // LogHelper console = SessionState.getConsole();
    // Set<ReadEntity> readEntitys = context.getInputs();
    // console.printInfo("Total Read Entity Size:" + readEntitys.size());
    // for (ReadEntity readEntity : readEntitys) {
    // Partition p = readEntity.getPartition();
    // Table t = readEntity.getTable();
    // }
  }
}
