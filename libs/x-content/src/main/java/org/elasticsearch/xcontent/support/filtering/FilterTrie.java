/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *//*


package org.elasticsearch.xcontent.support.filtering;

import java.util.HashMap;
import java.util.Map;

*/
/**
 * @author weizijun.wzj
 * @date 2021/10/25
 *//*

public class FilterTrie {
    private FilterNode root;

    public static class WildcardNode extends FilterNode {
        private Map<String, FilterTrie> childMap;

        public WildcardNode() {
            super('*');
            childMap = new HashMap<>();
        }

        public void addChild(String suffix, FilterTrie child) {
            childMap.put(suffix, child);
        }

        public Map<String, FilterTrie> getChildMap() {
            return childMap;
        }
    }

    public static class FilterNode {
        private Character content; // the character in the node
        private boolean isEnd; // whether the end of the words
        private Map<Character, FilterNode> childMap; // the child list
        private FilterTrie childFilterTrie;

        public FilterNode(Character c) {
            childMap = new HashMap<>();
            isEnd = false;
            content = c;
        }

        public FilterNode subNode(char c) {
            if (childMap != null) {
                return childMap.get(c);
            }
            return null;
        }

        public void addSubNode(Character c, FilterNode child) {
            childMap.put(c, child);
        }

        public boolean isEnd() {
            return isEnd;
        }

        public void setEnd(boolean end) {
            this.isEnd = end;
        }

        public Map<Character, FilterNode> subNodes() {
            return childMap;
        }

        public FilterTrie getChildFilterTrie() {
            return childFilterTrie;
        }

        public void setChildFilterTrie(FilterTrie childFilterTrie) {
            this.childFilterTrie = childFilterTrie;
        }
    }

    public void insert(String value, FilterTrie filterTrie) {
        FilterNode current = root;
        int i = 0;
        for (; i < value.length(); ++i) {
            FilterNode child = current.subNode(value.charAt(i));
            if (child == null) {
                current.setEnd(false);
                break;
            } else if (child instanceof WildcardNode) {
                ((WildcardNode) child).addChild(value.substring(i), filterTrie);
                break;
            } else {
                current = child;
            }
        }

        for (; i < value.length(); ++i) {
            if (value.charAt(i) == '*') {
                WildcardNode child = new WildcardNode();
                current.addSubNode('*', child);
                child.addChild(value.substring(i), filterTrie);
                break;
            } else {
                FilterNode child = new FilterNode(value.charAt(i));
                current.addSubNode(value.charAt(i), child);
                current = child;
            }
        }

        current.setChildFilterTrie(filterTrie);
        if (current.subNodes() == null || current.subNodes().isEmpty()) {
            current.setEnd(true);
        }
    }

    // public List<FilterTrie> search(String index){
    // FilterNode current = root;
    // int pos = 0;
    //
    // // 从根节点开始遍历字符
    // return searchSubNode(current, index, pos);
    // }

    // private List<FilterTrie> searchSubNode(FilterNode current, String index, int pos) {
    // if (current.isEnd()) {
    // // 已经遍历到模板的末尾
    // if (index.length() == pos) {
    // if (current.getChildFilterTrie() != null) {
    // return Collections.singletonList(current.getChildFilterTrie());
    // } else {
    // return Collections.emptyList();
    // }
    // }
    // }
    //
    // // 判断是否有带*号的查询，有的话，顺序过滤多个*号，到下一个字符
    // boolean wildcard = false;
    // while (pos < index.length() && index.charAt(pos) == '*') {
    // pos ++;
    // wildcard = true;
    // break;
    // }
    //
    // if (wildcard) {
    // // 前缀有带*号的case，则遍历剩下全部节点，找到符合的模板
    // return scanNodes(current, index);
    // }
    //
    // // 前缀没有带*号，则一直往下遍历
    // if (pos < index.length()) {
    // current = current.subNode(index.charAt(pos));
    // if (current == null) {
    // // 没找到对应的模板
    // return null;
    // } else {
    // return searchSubNode(current, index, pos + 1);
    // }
    // } else if (current.getIndexTemplate() != null) {
    // // index已经遍历完，且当前节点包含索引模板
    // boolean result = checkIndexMatchTemplate(index, current.getIndexTemplate());
    // if (result) {
    // return current.getIndexTemplate();
    // } else {
    // return null;
    // }
    // } else {
    // // index已经遍历完，但没命中到查询模板
    // return null;
    // }
    // }
    //
    // private List<FilterTrie> scanNodes(FilterNode current, String index) {
    // List<FilterTrie> filterTries = new ArrayList<>();
    //
    // for (FilterNode child : current.subNodes().values()) {
    // List<FilterTrie> childFilterTries = scanNodes(child, index);
    // }
    //
    // return indexTemplate;
    // }

}
*/
