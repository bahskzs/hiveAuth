package com.yqy.hive.utils;

import org.apache.hadoop.hive.ql.lib.*;
import org.apache.hadoop.hive.ql.parse.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * @author bahsk
 * @createTime 2021-01-31 1:10
 * @description 参考文档：https://blog.csdn.net/u011250186/article/details/106897705/
 * https://blog.csdn.net/u011250186/article/details/107047617?ops_request_misc=%25257B%252522request%25255Fid%252522%25253A%252522161208203216780265460769%252522%25252C%252522scm%252522%25253A%25252220140713.130102334.pc%25255Fblog.%252522%25257D&request_id=161208203216780265460769&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~blog~first_rank_v2~rank_v29-20-107047617.pc_v2_rank_blog_default&utm_term=hive
 */
public class LineageUtils implements NodeProcessor {

    private static final Logger LOG = LoggerFactory.getLogger("com.yqy.hive.LineageUtils");

    // 存放输入表
    TreeSet<String> inputTableList = new TreeSet<String>();

    // 存放目标表
    TreeSet<String> outputTableList = new TreeSet<String>();

    TreeSet<String> withTableList = new TreeSet<String>();

    public TreeSet getInputTableList() {
        return inputTableList;
    }

    public TreeSet getOutputTableList() {
        return outputTableList;
    }

    public TreeSet getWithTableList() {
        return withTableList;
    }

    @Override
    public Object process(Node node, Stack<Node> stack, NodeProcessorCtx nodeProcessorCtx, Object... objects) throws SemanticException {
        ASTNode pt = (ASTNode) node;
        switch (pt.getToken().getType()) {
            //create语句
            case HiveParser.TOK_CREATETABLE: {
                String createName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0));
                outputTableList.add(createName);
                break;
            }

            //insert语句
            case HiveParser.TOK_TAB: {
                String insertName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0));
                outputTableList.add(insertName);
                break;
            }

            //from语句
            case HiveParser.TOK_TABREF: {
                ASTNode tabTree = (ASTNode) pt.getChild(0);
                String fromName = (tabTree.getChildCount() == 1) ? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "." + tabTree.getChild(1);
                inputTableList.add(fromName);
                break;
            }

            // with.....语句
            case HiveParser.TOK_CTE: {
                for (int i = 0; i < pt.getChildCount(); i++) {
                    ASTNode temp = (ASTNode) pt.getChild(i);
                    String cteName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) temp.getChild(1));
                    withTableList.add(cteName);
                }
                break;
            }
        }
        return null;
    }

    public void getLineageInfo(String query) throws ParseException, SemanticException {

        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(query);

        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }
        inputTableList.clear();
        outputTableList.clear();
        withTableList.clear();

        Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();

        Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);
        GraphWalker ogw = new DefaultGraphWalker(disp);

        ArrayList topNodes = new ArrayList();
        topNodes.add(tree);
        ogw.startWalking(topNodes, null);
    }

    public static void main(String[] args) throws IOException, ParseException, SemanticException {
        LineageUtils lineageUtils = new LineageUtils();
        String query = args[0];
        lineageUtils.getLineageInfo(query);
        LOG.error("Input tables = " + lineageUtils.getInputTableList());
        LOG.error("Output tables = " + lineageUtils.getOutputTableList());
        LOG.error("with tables = " + lineageUtils.getWithTableList());
    }

}
